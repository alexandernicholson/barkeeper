//! TUI application state and transition functions.
//!
//! All state is plain data. All transitions are pure functions.
//! No I/O, no rendering, no async.

use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Dashboard,
    Keys,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub key: Vec<u8>,
    pub event_type: EventType,
    pub value_size: usize,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub name: String,
    pub id: u64,
    pub is_leader: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub leader_id: u64,
    pub raft_term: u64,
    pub member_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogKind {
    Put,
    DeleteConfirm,
    Search,
}

/// Actions the main loop should perform after an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingAction {
    /// Put key=dialog_key, value=dialog_value
    Put { key: String, value: String },
    /// Delete the given key
    Delete { key: String },
    /// Refresh all data
    Refresh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogField {
    Key,
    Value,
}

const MAX_WATCH_EVENTS: usize = 1000;

pub struct App {
    // Tab state
    active_tab: Tab,

    // Keys tab state
    prefix: String,
    keys: Vec<Vec<u8>>,
    selected_index: usize,
    search: Option<String>,

    // Key detail (value of selected key)
    selected_value: Option<Vec<u8>>,
    selected_key_meta: Option<KeyMeta>,

    // Dashboard state
    members: Vec<MemberInfo>,
    cluster_status: Option<ClusterStatus>,
    watch_events: Vec<WatchEvent>,

    // Dialog state
    dialog: Option<DialogKind>,
    dialog_key: String,
    dialog_value: String,
    dialog_field: DialogField,
    search_input: String,

    // Pending action for the main loop to execute
    pub pending_action: Option<PendingAction>,

    // Connection
    pub endpoint: String,
    pub watch_prefix: String,

    // Quit signal
    pub should_quit: bool,
}

#[derive(Debug, Clone)]
pub struct KeyMeta {
    pub revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

impl App {
    pub fn new() -> Self {
        Self {
            active_tab: Tab::Dashboard,
            prefix: "/".to_string(),
            keys: Vec::new(),
            selected_index: 0,
            search: None,
            selected_value: None,
            selected_key_meta: None,
            members: Vec::new(),
            cluster_status: None,
            watch_events: Vec::new(),
            dialog: None,
            dialog_key: String::new(),
            dialog_value: String::new(),
            dialog_field: DialogField::Key,
            search_input: String::new(),
            pending_action: None,
            endpoint: String::new(),
            watch_prefix: "/".to_string(),
            should_quit: false,
        }
    }

    // --- Tab ---

    pub fn active_tab(&self) -> Tab {
        self.active_tab
    }

    pub fn toggle_tab(&mut self) {
        self.active_tab = match self.active_tab {
            Tab::Dashboard => Tab::Keys,
            Tab::Keys => Tab::Dashboard,
        };
    }

    // --- Prefix navigation ---

    pub fn current_prefix(&self) -> &str {
        &self.prefix
    }

    pub fn set_keys(&mut self, keys: Vec<Vec<u8>>) {
        self.keys = keys;
        self.selected_index = 0;
    }

    /// Get visible sub-prefixes under the current prefix.
    pub fn visible_prefixes(&self) -> Vec<String> {
        let prefix = self.prefix.as_bytes();
        let mut prefixes = BTreeSet::new();
        for key in &self.keys {
            if key.starts_with(prefix) {
                let suffix = &key[prefix.len()..];
                if let Some(slash_pos) = suffix.iter().position(|&b| b == b'/') {
                    let sub = &suffix[..=slash_pos];
                    if let Ok(s) = std::str::from_utf8(sub) {
                        prefixes.insert(s.to_string());
                    }
                }
            }
        }
        prefixes.into_iter().collect()
    }

    /// Get visible leaf keys (no further slash) under the current prefix.
    pub fn visible_keys(&self) -> Vec<String> {
        let prefix = self.prefix.as_bytes();
        let mut result = Vec::new();
        for key in &self.keys {
            if key.starts_with(prefix) {
                let suffix = &key[prefix.len()..];
                if !suffix.contains(&b'/') && !suffix.is_empty() {
                    if let Ok(s) = std::str::from_utf8(suffix) {
                        result.push(s.to_string());
                    }
                }
            }
        }
        result
    }

    /// Total visible items (prefixes + keys) in the current view.
    pub fn visible_count(&self) -> usize {
        self.visible_prefixes().len() + self.visible_keys().len()
    }

    pub fn descend(&mut self, sub_prefix: &str) {
        self.prefix.push_str(sub_prefix);
        self.selected_index = 0;
    }

    pub fn ascend(&mut self) {
        if self.prefix == "/" {
            return;
        }
        // Remove trailing slash, then find the previous slash.
        let trimmed = &self.prefix[..self.prefix.len() - 1];
        if let Some(pos) = trimmed.rfind('/') {
            self.prefix = trimmed[..=pos].to_string();
        } else {
            self.prefix = "/".to_string();
        }
        self.selected_index = 0;
    }

    // --- Selection ---

    pub fn selected_index(&self) -> usize {
        self.selected_index
    }

    pub fn move_down(&mut self) {
        let max = self.visible_count().saturating_sub(1);
        if self.selected_index < max {
            self.selected_index += 1;
        }
    }

    pub fn move_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    pub fn selected_value(&self) -> Option<&[u8]> {
        self.selected_value.as_deref()
    }

    pub fn set_selected_value(&mut self, value: Option<Vec<u8>>) {
        self.selected_value = value;
    }

    pub fn selected_key_meta(&self) -> Option<&KeyMeta> {
        self.selected_key_meta.as_ref()
    }

    pub fn set_selected_key_meta(&mut self, meta: Option<KeyMeta>) {
        self.selected_key_meta = meta;
    }

    /// Get the full key path of the currently selected item, if it's a leaf key.
    pub fn selected_full_key(&self) -> Option<String> {
        let prefixes = self.visible_prefixes();
        let keys = self.visible_keys();
        if self.selected_index < prefixes.len() {
            None // selected a prefix, not a leaf
        } else {
            let key_idx = self.selected_index - prefixes.len();
            keys.get(key_idx).map(|k| format!("{}{}", self.prefix, k))
        }
    }

    // --- Watch events ---

    pub fn watch_events(&self) -> &[WatchEvent] {
        &self.watch_events
    }

    pub fn push_watch_event(&mut self, event: WatchEvent) {
        self.watch_events.push(event);
        if self.watch_events.len() > MAX_WATCH_EVENTS {
            let drain = self.watch_events.len() - MAX_WATCH_EVENTS;
            self.watch_events.drain(..drain);
        }
    }

    // --- Members ---

    pub fn members(&self) -> &[MemberInfo] {
        &self.members
    }

    pub fn set_members(&mut self, members: Vec<(String, u64, bool)>) {
        self.members = members
            .into_iter()
            .map(|(name, id, is_leader)| MemberInfo {
                name,
                id,
                is_leader,
            })
            .collect();
    }

    pub fn cluster_status(&self) -> Option<&ClusterStatus> {
        self.cluster_status.as_ref()
    }

    pub fn set_cluster_status(&mut self, status: ClusterStatus) {
        self.cluster_status = Some(status);
    }

    // --- Search ---

    pub fn set_search(&mut self, query: &str) {
        self.search = Some(query.to_string());
    }

    pub fn clear_search(&mut self) {
        self.search = None;
    }

    pub fn search_query(&self) -> Option<&str> {
        self.search.as_deref()
    }

    /// Get keys filtered by current search query (or all if no search).
    pub fn filtered_keys(&self) -> Vec<Vec<u8>> {
        match &self.search {
            None => self.keys.clone(),
            Some(query) => {
                let q = query.as_bytes();
                self.keys
                    .iter()
                    .filter(|k| k.windows(q.len()).any(|w| w == q))
                    .cloned()
                    .collect()
            }
        }
    }

    // --- Dialogs ---

    pub fn is_dialog_open(&self) -> bool {
        self.dialog.is_some()
    }

    pub fn is_delete_confirm_open(&self) -> bool {
        self.dialog == Some(DialogKind::DeleteConfirm)
    }

    pub fn open_put_dialog(&mut self) {
        self.dialog = Some(DialogKind::Put);
        self.dialog_key.clear();
        self.dialog_value.clear();
        self.dialog_field = DialogField::Key;
    }

    pub fn open_delete_confirm(&mut self) {
        self.dialog = Some(DialogKind::DeleteConfirm);
    }

    pub fn open_search_dialog(&mut self) {
        self.dialog = Some(DialogKind::Search);
        self.search_input.clear();
    }

    pub fn close_dialog(&mut self) {
        self.dialog = None;
    }

    pub fn dialog_kind(&self) -> Option<DialogKind> {
        self.dialog
    }

    pub fn dialog_key(&self) -> &str {
        &self.dialog_key
    }

    pub fn dialog_value(&self) -> &str {
        &self.dialog_value
    }

    pub fn set_dialog_key(&mut self, key: &str) {
        self.dialog_key = key.to_string();
    }

    pub fn set_dialog_value(&mut self, value: &str) {
        self.dialog_value = value.to_string();
    }

    pub fn dialog_field(&self) -> DialogField {
        self.dialog_field
    }

    pub fn toggle_dialog_field(&mut self) {
        self.dialog_field = match self.dialog_field {
            DialogField::Key => DialogField::Value,
            DialogField::Value => DialogField::Key,
        };
    }

    pub fn search_input(&self) -> &str {
        &self.search_input
    }

    /// Append a char to the active dialog field.
    pub fn dialog_type_char(&mut self, c: char) {
        match self.dialog {
            Some(DialogKind::Put) => match self.dialog_field {
                DialogField::Key => self.dialog_key.push(c),
                DialogField::Value => self.dialog_value.push(c),
            },
            Some(DialogKind::Search) => self.search_input.push(c),
            _ => {}
        }
    }

    /// Backspace in the active dialog field.
    pub fn dialog_backspace(&mut self) {
        match self.dialog {
            Some(DialogKind::Put) => match self.dialog_field {
                DialogField::Key => { self.dialog_key.pop(); }
                DialogField::Value => { self.dialog_value.pop(); }
            },
            Some(DialogKind::Search) => { self.search_input.pop(); }
            _ => {}
        }
    }

    /// Confirm the put dialog. Returns a PendingAction and closes the dialog.
    pub fn confirm_put(&mut self) -> Option<PendingAction> {
        if self.dialog_key.is_empty() {
            return None;
        }
        let action = PendingAction::Put {
            key: self.dialog_key.clone(),
            value: self.dialog_value.clone(),
        };
        self.dialog = None;
        self.pending_action = Some(action.clone());
        Some(action)
    }

    /// Confirm the delete dialog. Returns a PendingAction and closes the dialog.
    pub fn confirm_delete(&mut self) -> Option<PendingAction> {
        if let Some(key) = self.selected_full_key() {
            let action = PendingAction::Delete { key };
            self.dialog = None;
            self.pending_action = Some(action.clone());
            Some(action)
        } else {
            self.dialog = None;
            None
        }
    }

    /// Confirm the search dialog. Applies the filter and closes the dialog.
    pub fn confirm_search(&mut self) {
        if self.search_input.is_empty() {
            self.clear_search();
        } else {
            self.set_search(&self.search_input.clone());
        }
        self.dialog = None;
    }

    /// Take the pending action (returns it and clears it).
    pub fn take_pending_action(&mut self) -> Option<PendingAction> {
        self.pending_action.take()
    }
}
