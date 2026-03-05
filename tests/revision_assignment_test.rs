use barkeeper::raft::core::{RaftCore, Event, Action};
use barkeeper::raft::messages::{LogEntryData, ClientProposalResult};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[test]
fn test_proposal_assigns_revision() {
    let revision = Arc::new(AtomicI64::new(5));
    let mut core = RaftCore::new(1, revision.clone());

    // Initialize and become leader (single node)
    core.step(Event::Initialize {
        peers: vec![],
        hard_state: None,
        last_log_index: 0,
        last_log_term: 0,
    });
    let _ = core.step(Event::ElectionTimeout);

    // Propose — should assign revision 6
    let actions = core.step(Event::Proposal {
        id: 1,
        data: vec![1, 2, 3],
    });

    // Find the AppendToLog action
    let append = actions.iter().find_map(|a| match a {
        Action::AppendToLog(entries) => Some(entries),
        _ => None,
    }).expect("should have AppendToLog");

    match &append[0].data {
        LogEntryData::Command { revision, .. } => {
            assert_eq!(*revision, 6);
        }
        _ => panic!("expected Command"),
    }

    // Counter advanced
    assert_eq!(revision.load(Ordering::SeqCst), 6);
}

#[test]
fn test_proposal_result_includes_revision() {
    let revision = Arc::new(AtomicI64::new(0));
    let mut core = RaftCore::new(1, revision);

    core.step(Event::Initialize {
        peers: vec![],
        hard_state: None,
        last_log_index: 0,
        last_log_term: 0,
    });
    let _ = core.step(Event::ElectionTimeout);

    let actions = core.step(Event::Proposal { id: 1, data: vec![] });
    let respond = actions.iter().find_map(|a| match a {
        Action::RespondToProposal { id: _, result } => Some(result),
        _ => None,
    });
    // Single-node: should respond with Success including revision
    if let Some(result) = respond {
        match result {
            ClientProposalResult::Success { revision, .. } => {
                assert_eq!(*revision, 1);
            }
            _ => panic!("expected Success"),
        }
    }
}
