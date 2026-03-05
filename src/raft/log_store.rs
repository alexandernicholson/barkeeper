use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::messages::LogEntry;
use super::state::PersistentState;

/// In-memory index entry for a WAL record.
struct WalEntry {
    offset: u64,
    len: u32,
    index: u64,
    term: u64,
}

struct WalInner {
    file: File,
    entries: Vec<WalEntry>,
    write_offset: u64,
    first_index: u64,
}

/// Durable storage for Raft log entries and hard state.
///
/// Uses an append-only WAL file (`raft.wal`) for log entries and a separate
/// metadata file (`raft.meta`) for hard state. An in-memory index maps log
/// indices to file offsets for O(1) lookups.
///
/// WAL frame format: `[u32 LE payload_len][bincode-serialized LogEntry]`
#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<WalInner>>,
    meta_path: PathBuf,
}

impl LogStore {
    /// Open or create a LogStore in the given directory.
    /// Creates `raft.wal` and `raft.meta` files inside `dir`.
    /// On open, scans the WAL to rebuild the in-memory index.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, io::Error> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let wal_path = dir.join("raft.wal");
        let meta_path = dir.join("raft.meta");

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&wal_path)?;

        // Scan WAL to rebuild in-memory index
        let file_len = file.metadata()?.len();
        let mut entries = Vec::new();
        let mut offset = 0u64;

        file.seek(SeekFrom::Start(0))?;
        while offset + 4 <= file_len {
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(len_buf);

            if offset + 4 + payload_len as u64 > file_len {
                // Partial frame at end — truncate
                file.set_len(offset)?;
                break;
            }

            let mut payload = vec![0u8; payload_len as usize];
            if file.read_exact(&mut payload).is_err() {
                file.set_len(offset)?;
                break;
            }

            match bincode::deserialize::<LogEntry>(&payload) {
                Ok(entry) => {
                    entries.push(WalEntry {
                        offset,
                        len: payload_len,
                        index: entry.index,
                        term: entry.term,
                    });
                    offset += 4 + payload_len as u64;
                }
                Err(_) => {
                    file.set_len(offset)?;
                    break;
                }
            }
        }

        let first_index = entries.first().map(|e| e.index).unwrap_or(0);

        Ok(LogStore {
            inner: Arc::new(Mutex::new(WalInner {
                file,
                entries,
                write_offset: offset,
                first_index,
            })),
            meta_path,
        })
    }

    /// Append entries to the log.
    pub fn append(&self, entries: &[LogEntry]) -> Result<(), io::Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        Self::write_entries(&mut inner, entries)?;
        inner.file.sync_data()?;
        Ok(())
    }

    /// Flush log entries and optionally hard state.
    pub fn flush(
        &self,
        entries: &[LogEntry],
        hard_state: Option<&PersistentState>,
    ) -> Result<(), io::Error> {
        if entries.is_empty() && hard_state.is_none() {
            return Ok(());
        }
        if let Some(state) = hard_state {
            self.write_meta(state)?;
        }
        if !entries.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            Self::write_entries(&mut inner, entries)?;
            inner.file.sync_data()?;
        }
        Ok(())
    }

    /// Get a single log entry by index.
    pub fn get(&self, index: u64) -> Result<Option<LogEntry>, io::Error> {
        let mut inner = self.inner.lock().unwrap();
        let (offset, len) = match Self::find_entry(&inner, index) {
            Some(e) => (e.offset, e.len),
            None => return Ok(None),
        };
        Self::read_payload(&mut inner.file, offset, len).map(Some)
    }

    /// Get entries in range [start, end] inclusive.
    pub fn get_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>, io::Error> {
        let mut inner = self.inner.lock().unwrap();
        let offsets: Vec<(u64, u32)> = inner
            .entries
            .iter()
            .filter(|e| e.index >= start && e.index <= end)
            .map(|e| (e.offset, e.len))
            .collect();

        let mut result = Vec::with_capacity(offsets.len());
        for (offset, len) in offsets {
            result.push(Self::read_payload(&mut inner.file, offset, len)?);
        }
        Ok(result)
    }

    /// Truncate all entries after the given index.
    pub fn truncate_after(&self, after_index: u64) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().unwrap();
        Self::truncate_inner(&mut inner, after_index)
    }

    /// Get the last log index (0 if empty).
    pub fn last_index(&self) -> Result<u64, io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.last().map(|e| e.index).unwrap_or(0))
    }

    /// Get the term of the last log entry (0 if empty).
    pub fn last_term(&self) -> Result<u64, io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.last().map(|e| e.term).unwrap_or(0))
    }

    /// Get the term for a specific log index.
    pub fn term_at(&self, index: u64) -> Result<Option<u64>, io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok(Self::find_entry(&inner, index).map(|e| e.term))
    }

    /// Save persistent Raft state (current_term, voted_for).
    pub fn save_hard_state(&self, state: &PersistentState) -> Result<(), io::Error> {
        self.write_meta(state)
    }

    /// Load persistent Raft state.
    pub fn load_hard_state(&self) -> Result<Option<PersistentState>, io::Error> {
        match fs::read(&self.meta_path) {
            Ok(bytes) => {
                let state = bincode::deserialize(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get total number of log entries.
    pub fn len(&self) -> Result<u64, io::Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.len() as u64)
    }

    pub fn is_empty(&self) -> Result<bool, io::Error> {
        Ok(self.len()? == 0)
    }

    // --- Private helpers ---

    fn write_entries(inner: &mut WalInner, entries: &[LogEntry]) -> io::Result<()> {
        if let Some(first) = entries.first() {
            if !inner.entries.is_empty() {
                let last_idx = inner.entries.last().unwrap().index;
                if first.index <= last_idx {
                    let truncate_to = if first.index == 0 { 0 } else { first.index - 1 };
                    Self::truncate_inner(inner, truncate_to)?;
                }
            }
            if inner.entries.is_empty() {
                inner.first_index = first.index;
            }
        }

        inner.file.seek(SeekFrom::Start(inner.write_offset))?;
        for entry in entries {
            let payload = bincode::serialize(entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let len = payload.len() as u32;
            inner.file.write_all(&len.to_le_bytes())?;
            inner.file.write_all(&payload)?;

            inner.entries.push(WalEntry {
                offset: inner.write_offset,
                len,
                index: entry.index,
                term: entry.term,
            });
            inner.write_offset += 4 + len as u64;
        }
        Ok(())
    }

    fn truncate_inner(inner: &mut WalInner, after_index: u64) -> io::Result<()> {
        if inner.entries.is_empty() {
            return Ok(());
        }

        if after_index == 0 || after_index < inner.first_index {
            inner.file.set_len(0)?;
            inner.entries.clear();
            inner.write_offset = 0;
            inner.first_index = 0;
            return Ok(());
        }

        let first_idx = inner.first_index;
        let keep_count = (after_index - first_idx + 1) as usize;
        if keep_count >= inner.entries.len() {
            return Ok(());
        }

        let new_file_len = inner.entries[keep_count].offset;
        inner.file.set_len(new_file_len)?;
        inner.entries.truncate(keep_count);
        inner.write_offset = new_file_len;
        Ok(())
    }

    fn find_entry(inner: &WalInner, index: u64) -> Option<&WalEntry> {
        if inner.entries.is_empty() || index < inner.first_index {
            return None;
        }
        let pos = (index - inner.first_index) as usize;
        inner.entries.get(pos).filter(|e| e.index == index)
    }

    fn read_payload(file: &mut File, offset: u64, len: u32) -> io::Result<LogEntry> {
        file.seek(SeekFrom::Start(offset + 4))?;
        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf)?;
        bincode::deserialize(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn write_meta(&self, state: &PersistentState) -> io::Result<()> {
        let bytes = bincode::serialize(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tmp_path = self.meta_path.with_extension("tmp");
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(&bytes)?;
        tmp.sync_data()?;
        fs::rename(&tmp_path, &self.meta_path)?;
        Ok(())
    }
}
