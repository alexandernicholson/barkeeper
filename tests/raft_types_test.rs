use barkeeper::raft::messages::*;
use barkeeper::raft::state::*;

#[test]
fn test_raft_state_new() {
    let state = RaftState::new(1);
    assert_eq!(state.node_id, 1);
    assert_eq!(state.role, RaftRole::Follower);
    assert_eq!(state.persistent.current_term, 0);
    assert_eq!(state.persistent.voted_for, None);
    assert!(state.voters.contains(&1));
    assert_eq!(state.quorum_size(), 1);
}

#[test]
fn test_quorum_size() {
    let mut state = RaftState::new(1);
    state.voters.insert(2);
    state.voters.insert(3);
    assert_eq!(state.quorum_size(), 2); // majority of 3
    state.voters.insert(4);
    state.voters.insert(5);
    assert_eq!(state.quorum_size(), 3); // majority of 5
}

#[test]
fn test_leader_state_initialization() {
    let peers = vec![2, 3, 4];
    let leader_state = LeaderState::new(&peers, 10);
    assert_eq!(leader_state.next_index[&2], 11);
    assert_eq!(leader_state.next_index[&3], 11);
    assert_eq!(leader_state.match_index[&2], 0);
}

#[test]
fn test_raft_message_encode_decode() {
    let msg = RaftMessage::RequestVoteReq(RequestVoteRequest {
        term: 5,
        candidate_id: 2,
        last_log_index: 10,
        last_log_term: 4,
    });
    let encoded = raft_message_to_value(&msg);
    let decoded = raft_message_from_value(&encoded).unwrap();
    match decoded {
        RaftMessage::RequestVoteReq(req) => {
            assert_eq!(req.term, 5);
            assert_eq!(req.candidate_id, 2);
        }
        _ => panic!("wrong message type"),
    }
}

#[test]
fn test_log_entry_variants() {
    let cmd = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command { data: b"put key value".to_vec(), revision: 0 },
    };
    assert_eq!(cmd.term, 1);

    let noop = LogEntry {
        term: 2,
        index: 2,
        data: LogEntryData::Noop,
    };
    assert_eq!(noop.index, 2);
}

#[test]
fn test_log_entry_command_with_revision() {
    let entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command {
            data: vec![1, 2, 3],
            revision: 42,
        },
    };
    match &entry.data {
        LogEntryData::Command { data, revision } => {
            assert_eq!(data, &vec![1, 2, 3]);
            assert_eq!(*revision, 42);
        }
        _ => panic!("expected Command"),
    }
}

#[test]
fn test_peers_excludes_self() {
    let mut state = RaftState::new(1);
    state.voters.insert(2);
    state.voters.insert(3);
    state.learners.insert(4);
    let peers = state.peers();
    assert_eq!(peers.len(), 3);
    assert!(!peers.contains(&1));
}
