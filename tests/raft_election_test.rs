use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use barkeeper::raft::core::*;
use barkeeper::raft::messages::*;
use barkeeper::raft::state::*;

fn init_core(node_id: u64, peers: Vec<u64>) -> RaftCore {
    let revision = Arc::new(AtomicI64::new(0));
    let mut core = RaftCore::new(node_id, revision);
    core.step(Event::Initialize {
        peers,
        hard_state: None,
        last_log_index: 0,
        last_log_term: 0,
    });
    core
}

#[test]
fn test_single_node_election() {
    let mut core = init_core(1, vec![]);
    assert_eq!(core.state.role, RaftRole::Follower);

    let actions = core.step(Event::ElectionTimeout);
    assert_eq!(core.state.role, RaftRole::Leader);
    assert_eq!(core.state.persistent.current_term, 1);
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::StartHeartbeatTimer)));
}

#[test]
fn test_three_node_election_starts() {
    let mut core = init_core(1, vec![2, 3]);
    let actions = core.step(Event::ElectionTimeout);
    assert_eq!(core.state.role, RaftRole::Candidate);
    assert_eq!(core.state.persistent.current_term, 1);
    assert_eq!(core.state.persistent.voted_for, Some(1));

    // Should send RequestVote to peers 2 and 3
    let vote_requests: Vec<_> = actions
        .iter()
        .filter(|a| {
            matches!(
                a,
                Action::SendMessage {
                    message: RaftMessage::RequestVoteReq(_),
                    ..
                }
            )
        })
        .collect();
    assert_eq!(vote_requests.len(), 2);
}

#[test]
fn test_wins_election_with_majority() {
    let mut core = init_core(1, vec![2, 3]);
    core.step(Event::ElectionTimeout);

    // Receive vote from node 2 (now have 2/3 = majority)
    let actions = core.step(Event::Message {
        from: 2,
        message: RaftMessage::RequestVoteResp(RequestVoteResponse {
            term: 1,
            vote_granted: true,
        }),
    });

    assert_eq!(core.state.role, RaftRole::Leader);
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::StartHeartbeatTimer)));
}

#[test]
fn test_vote_rejected_higher_term() {
    let mut core = init_core(1, vec![2, 3]);
    core.step(Event::ElectionTimeout);

    let _actions = core.step(Event::Message {
        from: 2,
        message: RaftMessage::RequestVoteResp(RequestVoteResponse {
            term: 5, // higher term
            vote_granted: false,
        }),
    });

    // Should step down to follower at term 5
    assert_eq!(core.state.role, RaftRole::Follower);
    assert_eq!(core.state.persistent.current_term, 5);
}

#[test]
fn test_grant_vote_if_not_voted() {
    let mut core = init_core(1, vec![2, 3]);

    let actions = core.step(Event::Message {
        from: 2,
        message: RaftMessage::RequestVoteReq(RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    // Should grant vote
    let resp = actions.iter().find_map(|a| match a {
        Action::SendMessage {
            to: 2,
            message: RaftMessage::RequestVoteResp(resp),
        } => Some(resp),
        _ => None,
    });
    assert!(resp.unwrap().vote_granted);
    assert_eq!(core.state.persistent.voted_for, Some(2));
}

#[test]
fn test_deny_vote_if_already_voted() {
    let mut core = init_core(1, vec![2, 3]);

    // Vote for node 2
    core.step(Event::Message {
        from: 2,
        message: RaftMessage::RequestVoteReq(RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    // Node 3 also requests vote
    let actions = core.step(Event::Message {
        from: 3,
        message: RaftMessage::RequestVoteReq(RequestVoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        }),
    });

    let resp = actions.iter().find_map(|a| match a {
        Action::SendMessage {
            to: 3,
            message: RaftMessage::RequestVoteResp(resp),
        } => Some(resp),
        _ => None,
    });
    assert!(!resp.unwrap().vote_granted);
}

#[test]
fn test_leader_steps_down_on_higher_term() {
    let mut core = init_core(1, vec![]);
    core.step(Event::ElectionTimeout); // become leader
    assert!(core.state.is_leader());

    // Receive AppendEntries from a node with higher term
    core.step(Event::Message {
        from: 2,
        message: RaftMessage::AppendEntriesReq(AppendEntriesRequest {
            term: 5,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    });

    assert_eq!(core.state.role, RaftRole::Follower);
    assert_eq!(core.state.persistent.current_term, 5);
    assert_eq!(core.state.leader_id, Some(2));
}

#[test]
fn test_proposal_not_leader() {
    let mut core = init_core(1, vec![2, 3]);

    let actions = core.step(Event::Proposal {
        id: 1,
        data: b"test".to_vec(),
    });

    assert!(actions.iter().any(|a| matches!(
        a,
        Action::RespondToProposal {
            result: ClientProposalResult::NotLeader { .. },
            ..
        }
    )));
}

#[test]
fn test_proposal_as_leader_single_node() {
    let mut core = init_core(1, vec![]);
    core.step(Event::ElectionTimeout);

    let actions = core.step(Event::Proposal {
        id: 42,
        data: b"put x 1".to_vec(),
    });

    // Should append to log and commit immediately (single-node)
    assert!(actions.iter().any(|a| matches!(a, Action::AppendToLog(_))));
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::ApplyEntries { .. })));
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::RespondToProposal {
            id: 42,
            result: ClientProposalResult::Success { .. }
        }
    )));
}
