use barkeeper::raft::messages::*;

#[test]
fn test_raft_message_roundtrip_append_entries_req() {
    let msg = RaftMessage::AppendEntriesReq(AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 10,
        prev_log_term: 2,
        entries: vec![LogEntry {
            term: 3,
            index: 11,
            data: LogEntryData::Command(b"hello".to_vec()),
        }],
        leader_commit: 10,
    });

    let value = raft_message_to_value(&msg);
    let decoded = raft_message_from_value(&value).unwrap();

    match decoded {
        RaftMessage::AppendEntriesReq(req) => {
            assert_eq!(req.term, 3);
            assert_eq!(req.leader_id, 1);
            assert_eq!(req.entries.len(), 1);
        }
        _ => panic!("expected AppendEntriesReq"),
    }
}

#[test]
fn test_raft_message_roundtrip_request_vote() {
    let msg = RaftMessage::RequestVoteReq(RequestVoteRequest {
        term: 5,
        candidate_id: 2,
        last_log_index: 20,
        last_log_term: 4,
    });

    let value = raft_message_to_value(&msg);
    let decoded = raft_message_from_value(&value).unwrap();

    match decoded {
        RaftMessage::RequestVoteReq(req) => {
            assert_eq!(req.term, 5);
            assert_eq!(req.candidate_id, 2);
        }
        _ => panic!("expected RequestVoteReq"),
    }
}

#[test]
fn test_raft_message_roundtrip_all_variants() {
    let messages = vec![
        RaftMessage::AppendEntriesReq(AppendEntriesRequest {
            term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
            entries: vec![], leader_commit: 0,
        }),
        RaftMessage::AppendEntriesResp(AppendEntriesResponse {
            term: 1, success: true, last_log_index: 5,
        }),
        RaftMessage::RequestVoteReq(RequestVoteRequest {
            term: 2, candidate_id: 1, last_log_index: 5, last_log_term: 1,
        }),
        RaftMessage::RequestVoteResp(RequestVoteResponse {
            term: 2, vote_granted: true,
        }),
    ];

    for msg in messages {
        let value = raft_message_to_value(&msg);
        let decoded = raft_message_from_value(&value).unwrap();
        // Verify roundtrip by re-serializing
        let value2 = raft_message_to_value(&decoded);
        assert_eq!(
            format!("{:?}", value),
            format!("{:?}", value2),
            "roundtrip failed for message"
        );
    }
}
