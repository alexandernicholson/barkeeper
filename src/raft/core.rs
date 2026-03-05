use super::messages::*;
use super::state::*;

/// Actions the RaftCore wants the actor to perform.
#[derive(Debug)]
pub enum Action {
    /// Send a Raft message to a peer node.
    SendMessage { to: u64, message: RaftMessage },
    /// Persist hard state to durable storage.
    PersistHardState(PersistentState),
    /// Append entries to log store.
    AppendToLog(Vec<LogEntry>),
    /// Truncate log after given index.
    TruncateLogAfter(u64),
    /// Apply committed entries [last_applied+1..=commit_index] to state machine.
    ApplyEntries { from: u64, to: u64 },
    /// Reset election timer (randomized).
    ResetElectionTimer,
    /// Start sending heartbeats (became leader).
    StartHeartbeatTimer,
    /// Stop heartbeat timer (stepped down).
    StopHeartbeatTimer,
    /// Respond to a client proposal.
    RespondToProposal { id: u64, result: ClientProposalResult },
}

/// Events that drive the Raft state machine.
#[derive(Debug)]
pub enum Event {
    /// Raft message received from a peer.
    Message { from: u64, message: RaftMessage },
    /// Election timeout fired.
    ElectionTimeout,
    /// Heartbeat timer fired (leader only).
    HeartbeatTimeout,
    /// Client submitted a proposal.
    Proposal { id: u64, data: Vec<u8> },
    /// Node is starting up with restored state.
    Initialize {
        peers: Vec<u64>,
        hard_state: Option<PersistentState>,
        last_log_index: u64,
        last_log_term: u64,
    },
}

pub struct RaftCore {
    pub state: RaftState,
    last_log_index: u64,
    last_log_term: u64,
    /// Pending client proposals awaiting commit (leader only).
    pending_proposals: Vec<(u64, u64)>, // (proposal_id, log_index)
}

impl RaftCore {
    pub fn new(node_id: u64) -> Self {
        RaftCore {
            state: RaftState::new(node_id),
            last_log_index: 0,
            last_log_term: 0,
            pending_proposals: Vec::new(),
        }
    }

    /// Process an event and return actions to perform.
    pub fn step(&mut self, event: Event) -> Vec<Action> {
        match event {
            Event::Initialize {
                peers,
                hard_state,
                last_log_index,
                last_log_term,
            } => self.handle_initialize(peers, hard_state, last_log_index, last_log_term),
            Event::ElectionTimeout => self.handle_election_timeout(),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout(),
            Event::Proposal { id, data } => self.handle_proposal(id, data),
            Event::Message { from, message } => self.handle_message(from, message),
        }
    }

    fn handle_initialize(
        &mut self,
        peers: Vec<u64>,
        hard_state: Option<PersistentState>,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Vec<Action> {
        if let Some(hs) = hard_state {
            self.state.persistent = hs;
        }
        for peer in peers {
            self.state.voters.insert(peer);
        }
        self.last_log_index = last_log_index;
        self.last_log_term = last_log_term;
        vec![Action::ResetElectionTimer]
    }

    fn handle_election_timeout(&mut self) -> Vec<Action> {
        if self.state.is_leader() {
            return vec![];
        }
        self.start_election()
    }

    fn start_election(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();

        self.state.persistent.current_term += 1;
        self.state.role = RaftRole::Candidate;
        self.state.persistent.voted_for = Some(self.state.node_id);
        self.state.votes_received.clear();
        self.state.votes_received.insert(self.state.node_id);
        self.state.leader_id = None;

        actions.push(Action::PersistHardState(self.state.persistent.clone()));
        actions.push(Action::ResetElectionTimer);

        // Single-node cluster: immediately become leader
        if self.state.quorum_size() <= 1 {
            return self.become_leader(actions);
        }

        let req = RequestVoteRequest {
            term: self.state.persistent.current_term,
            candidate_id: self.state.node_id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        };

        let peers: Vec<u64> = self
            .state
            .voters
            .iter()
            .copied()
            .filter(|&id| id != self.state.node_id)
            .collect();
        for peer in peers {
            actions.push(Action::SendMessage {
                to: peer,
                message: RaftMessage::RequestVoteReq(req.clone()),
            });
        }

        actions
    }

    fn become_leader(&mut self, mut actions: Vec<Action>) -> Vec<Action> {
        self.state.role = RaftRole::Leader;
        self.state.leader_id = Some(self.state.node_id);

        let peers: Vec<u64> = self.state.peers();
        self.state.leader_state = Some(LeaderState::new(&peers, self.last_log_index));

        actions.push(Action::StartHeartbeatTimer);

        // Append no-op entry to commit entries from previous terms
        let noop = LogEntry {
            term: self.state.persistent.current_term,
            index: self.last_log_index + 1,
            data: LogEntryData::Noop,
        };
        self.last_log_index = noop.index;
        self.last_log_term = noop.term;
        actions.push(Action::AppendToLog(vec![noop]));

        // Update own match_index for the no-op
        if let Some(ref mut ls) = self.state.leader_state {
            ls.match_index
                .insert(self.state.node_id, self.last_log_index);
            ls.next_index
                .insert(self.state.node_id, self.last_log_index + 1);
        }

        // Send initial empty AppendEntries (heartbeat) to all peers
        actions.extend(self.send_append_entries_to_all());

        actions
    }

    fn handle_heartbeat_timeout(&mut self) -> Vec<Action> {
        if !self.state.is_leader() {
            return vec![];
        }
        self.send_append_entries_to_all()
    }

    fn handle_proposal(&mut self, id: u64, data: Vec<u8>) -> Vec<Action> {
        if !self.state.is_leader() {
            return vec![Action::RespondToProposal {
                id,
                result: ClientProposalResult::NotLeader {
                    leader_id: self.state.leader_id,
                },
            }];
        }

        let entry = LogEntry {
            term: self.state.persistent.current_term,
            index: self.last_log_index + 1,
            data: LogEntryData::Command(data),
        };
        self.last_log_index = entry.index;
        self.last_log_term = entry.term;
        self.pending_proposals.push((id, entry.index));

        let mut actions = vec![Action::AppendToLog(vec![entry])];

        // Update own match_index
        if let Some(ref mut ls) = self.state.leader_state {
            ls.match_index
                .insert(self.state.node_id, self.last_log_index);
            ls.next_index
                .insert(self.state.node_id, self.last_log_index + 1);
        }

        // Replicate to followers
        actions.extend(self.send_append_entries_to_all());

        // Check if we can commit (single-node cluster)
        actions.extend(self.advance_commit_index());

        actions
    }

    fn handle_message(&mut self, from: u64, message: RaftMessage) -> Vec<Action> {
        match message {
            RaftMessage::RequestVoteReq(req) => self.handle_request_vote(from, req),
            RaftMessage::RequestVoteResp(resp) => self.handle_request_vote_response(from, resp),
            RaftMessage::AppendEntriesReq(req) => self.handle_append_entries(from, req),
            RaftMessage::AppendEntriesResp(resp) => {
                self.handle_append_entries_response(from, resp)
            }
            RaftMessage::InstallSnapshotReq(_req) => vec![], // placeholder for Task 15
            RaftMessage::InstallSnapshotResp(_resp) => vec![], // placeholder for Task 15
        }
    }

    fn handle_request_vote(&mut self, from: u64, req: RequestVoteRequest) -> Vec<Action> {
        let mut actions = Vec::new();

        // If request term > our term, step down
        if req.term > self.state.persistent.current_term {
            self.step_down(req.term, &mut actions);
        }

        let vote_granted = req.term == self.state.persistent.current_term
            && (self.state.persistent.voted_for.is_none()
                || self.state.persistent.voted_for == Some(req.candidate_id))
            && self.is_log_up_to_date(req.last_log_index, req.last_log_term);

        if vote_granted {
            self.state.persistent.voted_for = Some(req.candidate_id);
            actions.push(Action::PersistHardState(self.state.persistent.clone()));
            actions.push(Action::ResetElectionTimer);
        }

        actions.push(Action::SendMessage {
            to: from,
            message: RaftMessage::RequestVoteResp(RequestVoteResponse {
                term: self.state.persistent.current_term,
                vote_granted,
            }),
        });

        actions
    }

    fn handle_request_vote_response(
        &mut self,
        from: u64,
        resp: RequestVoteResponse,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        if resp.term > self.state.persistent.current_term {
            self.step_down(resp.term, &mut actions);
            return actions;
        }

        if self.state.role != RaftRole::Candidate
            || resp.term != self.state.persistent.current_term
        {
            return actions;
        }

        if resp.vote_granted {
            self.state.votes_received.insert(from);
            if self.state.votes_received.len() >= self.state.quorum_size() {
                return self.become_leader(actions);
            }
        }

        actions
    }

    fn handle_append_entries(&mut self, from: u64, req: AppendEntriesRequest) -> Vec<Action> {
        let mut actions = Vec::new();

        if req.term < self.state.persistent.current_term {
            actions.push(Action::SendMessage {
                to: from,
                message: RaftMessage::AppendEntriesResp(AppendEntriesResponse {
                    term: self.state.persistent.current_term,
                    success: false,
                    last_log_index: self.last_log_index,
                }),
            });
            return actions;
        }

        if req.term > self.state.persistent.current_term {
            self.step_down(req.term, &mut actions);
        } else if self.state.role == RaftRole::Candidate {
            self.state.role = RaftRole::Follower;
            actions.push(Action::StopHeartbeatTimer);
        }

        self.state.leader_id = Some(from);
        actions.push(Action::ResetElectionTimer);

        if !req.entries.is_empty() {
            let first_new_index = req.entries[0].index;
            actions.push(Action::TruncateLogAfter(first_new_index - 1));
            self.last_log_index = req.entries.last().unwrap().index;
            self.last_log_term = req.entries.last().unwrap().term;
            actions.push(Action::AppendToLog(req.entries));
        }

        // Update commit index
        if req.leader_commit > self.state.volatile.commit_index {
            let new_commit = std::cmp::min(req.leader_commit, self.last_log_index);
            if new_commit > self.state.volatile.commit_index {
                let old_commit = self.state.volatile.commit_index;
                self.state.volatile.commit_index = new_commit;
                actions.push(Action::ApplyEntries {
                    from: old_commit + 1,
                    to: new_commit,
                });
            }
        }

        actions.push(Action::SendMessage {
            to: from,
            message: RaftMessage::AppendEntriesResp(AppendEntriesResponse {
                term: self.state.persistent.current_term,
                success: true,
                last_log_index: self.last_log_index,
            }),
        });

        actions
    }

    fn handle_append_entries_response(
        &mut self,
        from: u64,
        resp: AppendEntriesResponse,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        if resp.term > self.state.persistent.current_term {
            self.step_down(resp.term, &mut actions);
            return actions;
        }

        if !self.state.is_leader() || resp.term != self.state.persistent.current_term {
            return actions;
        }

        let leader_state = self.state.leader_state.as_mut().unwrap();

        if resp.success {
            leader_state.match_index.insert(from, resp.last_log_index);
            leader_state
                .next_index
                .insert(from, resp.last_log_index + 1);
            actions.extend(self.advance_commit_index());
        } else {
            // Decrement next_index and retry (log inconsistency)
            let next = leader_state.next_index.get(&from).copied().unwrap_or(1);
            let new_next = std::cmp::min(next.saturating_sub(1).max(1), resp.last_log_index + 1);
            leader_state.next_index.insert(from, new_next);
        }

        actions
    }

    // --- Helper methods ---

    fn step_down(&mut self, new_term: u64, actions: &mut Vec<Action>) {
        self.state.persistent.current_term = new_term;
        self.state.persistent.voted_for = None;
        self.state.role = RaftRole::Follower;
        self.state.leader_state = None;
        self.state.votes_received.clear();
        actions.push(Action::PersistHardState(self.state.persistent.clone()));
        actions.push(Action::StopHeartbeatTimer);
    }

    fn is_log_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        if last_log_term != self.last_log_term {
            last_log_term > self.last_log_term
        } else {
            last_log_index >= self.last_log_index
        }
    }

    fn send_append_entries_to_all(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        let leader_state = match &self.state.leader_state {
            Some(ls) => ls,
            None => return actions,
        };

        let peers: Vec<u64> = self
            .state
            .voters
            .iter()
            .copied()
            .filter(|&id| id != self.state.node_id)
            .collect();
        for peer in peers {
            let next = leader_state.next_index.get(&peer).copied().unwrap_or(1);
            let prev_index = next - 1;

            let req = AppendEntriesRequest {
                term: self.state.persistent.current_term,
                leader_id: self.state.node_id,
                prev_log_index: prev_index,
                prev_log_term: 0, // Actor fills this from LogStore
                entries: vec![],  // Actor fills this from LogStore
                leader_commit: self.state.volatile.commit_index,
            };
            actions.push(Action::SendMessage {
                to: peer,
                message: RaftMessage::AppendEntriesReq(req),
            });
        }

        actions
    }

    fn advance_commit_index(&mut self) -> Vec<Action> {
        if !self.state.is_leader() {
            return vec![];
        }

        let leader_state = self.state.leader_state.as_ref().unwrap();

        // Find the highest index replicated to a majority
        let mut match_indices: Vec<u64> = leader_state.match_index.values().copied().collect();
        match_indices.sort_unstable();
        let quorum_idx = match_indices.len() - self.state.quorum_size();
        let new_commit = match_indices[quorum_idx];

        if new_commit > self.state.volatile.commit_index {
            let old_commit = self.state.volatile.commit_index;
            self.state.volatile.commit_index = new_commit;

            let mut actions = vec![Action::ApplyEntries {
                from: old_commit + 1,
                to: new_commit,
            }];

            // Respond to any pending proposals that are now committed
            let committed: Vec<(u64, u64)> = self
                .pending_proposals
                .iter()
                .filter(|(_, idx)| *idx <= new_commit)
                .cloned()
                .collect();
            self.pending_proposals.retain(|(_, idx)| *idx > new_commit);
            for (id, entry_index) in committed {
                actions.push(Action::RespondToProposal {
                    id,
                    result: ClientProposalResult::Success {
                        index: entry_index,
                        revision: 0,
                    },
                });
            }

            actions
        } else {
            vec![]
        }
    }
}
