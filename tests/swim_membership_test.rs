use rebar_cluster::swim::*;
use std::net::SocketAddr;

#[test]
fn test_swim_alive_adds_to_membership() {
    let mut members = MembershipList::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    members.add(Member::new(2, addr));

    assert_eq!(members.alive_count(), 1);
    assert!(members.get(2).is_some());
    assert_eq!(members.get(2).unwrap().state, NodeState::Alive);
}

#[test]
fn test_swim_dead_marks_member() {
    let mut members = MembershipList::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    members.add(Member::new(2, addr));
    members.mark_dead(2);

    assert_eq!(members.get(2).unwrap().state, NodeState::Dead);
    assert_eq!(members.alive_count(), 0);
}

#[test]
fn test_gossip_queue_propagates_alive() {
    let mut queue = GossipQueue::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    queue.add(GossipUpdate::Alive {
        node_id: 2,
        addr,
        incarnation: 1,
        cert_hash: None,
    });

    let updates = queue.drain(10);
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        GossipUpdate::Alive { node_id, .. } => assert_eq!(*node_id, 2),
        _ => panic!("expected Alive"),
    }
}

#[test]
fn test_membership_sync_alive_event_adds_voter() {
    use barkeeper::cluster::membership_sync::MembershipSync;

    let mut sync = MembershipSync::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();

    sync.on_alive(2, addr);
    let voters = sync.current_voters();
    assert!(voters.contains(&2));
}

#[test]
fn test_membership_sync_dead_event_removes_peer() {
    use barkeeper::cluster::membership_sync::MembershipSync;

    let mut sync = MembershipSync::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();

    sync.on_alive(2, addr);
    assert!(sync.current_voters().contains(&2));

    sync.on_dead(2);
    assert!(!sync.has_peer(2));
}
