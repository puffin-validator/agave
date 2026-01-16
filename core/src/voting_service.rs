use {
    crate::{
        consensus::tower_storage::{SavedTowerVersions, TowerStorage},
        mock_alpenglow_consensus::MockAlpenglowConsensus,
    },
    bincode::serialize,
    crossbeam_channel::Receiver,
    solana_clock::Slot,
    solana_gossip::{cluster_info::ClusterInfo, epoch_specs::EpochSpecs},
    solana_measure::measure::Measure,
    solana_runtime::bank_forks::BankForks,
    solana_tpu_client_next::{Client, TransactionSender, leader_updater::LeaderUpdater},
    solana_transaction::Transaction,
    std::{
        net::UdpSocket,
        sync::{Arc, RwLock},
        thread::{self, Builder, JoinHandle},
    },
};

// Attempt to send our vote transaction to this amount of leaders.
pub(crate) const UPCOMING_LEADER_FANOUT: usize = 2;


pub enum VoteSender {
    UDP(UdpSocket, Box<dyn LeaderUpdater>),
    QUIC(TransactionSender, Client)
}

pub enum VoteOp {
    PushVote {
        tx: Transaction,
        tower_slots: Vec<Slot>,
        saved_tower: SavedTowerVersions,
    },
    RefreshVote {
        tx: Transaction,
        last_voted_slot: Slot,
    },
}

impl VoteOp {
    fn tx(&self) -> &Transaction {
        match self {
            VoteOp::PushVote { tx, .. } => tx,
            VoteOp::RefreshVote { tx, .. } => tx,
        }
    }
}

pub struct VotingService {
    thread_hdl: JoinHandle<()>,
}

impl VotingService {
    pub fn new(
        vote_receiver: Receiver<VoteOp>,
        cluster_info: Arc<ClusterInfo>,
        tower_storage: Arc<dyn TowerStorage>,
        mut vote_sender: VoteSender,
        alpenglow_socket: Option<UdpSocket>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solVoteService".to_string())
            .spawn({
                let mut mock_alpenglow = alpenglow_socket.map(|s| {
                    MockAlpenglowConsensus::new(
                        s,
                        cluster_info.clone(),
                        EpochSpecs::from(bank_forks.clone()),
                    )
                });
                move || {
                    for vote_op in vote_receiver.iter() {
                        // Figure out if we are casting a vote for a new slot, and what slot it is for
                        let vote_slot = match vote_op {
                            VoteOp::PushVote {
                                tx: _,
                                ref tower_slots,
                                ..
                            } => tower_slots.iter().copied().last(),
                            _ => None,
                        };
                        // perform all the normal vote handling routines
                        Self::handle_vote(
                            &cluster_info,
                            tower_storage.as_ref(),
                            vote_op,
                            &mut vote_sender,
                        );
                        // trigger mock alpenglow vote if we have just cast an actual vote
                        if let Some(slot) = vote_slot {
                            if let Some(ag) = mock_alpenglow.as_mut() {
                                let root_bank = { bank_forks.read().unwrap().root_bank() };
                                ag.signal_new_slot(slot, &root_bank);
                            }
                        }
                    }
                    if let Some(ag) = mock_alpenglow {
                        let _ = ag.join();
                    }
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn handle_vote(
        cluster_info: &ClusterInfo,
        tower_storage: &dyn TowerStorage,
        vote_op: VoteOp,
        vote_sender: &mut VoteSender,
    ) {
        if let VoteOp::PushVote { saved_tower, .. } = &vote_op {
            let mut measure = Measure::start("tower storage save");
            if let Err(err) = tower_storage.store(saved_tower) {
                error!("Unable to save tower to storage: {err:?}");
                std::process::exit(1);
            }
            measure.stop();
            trace!("{measure}");
        }

        if let Ok(serialized) = serialize(vote_op.tx()) {
            match vote_sender {
                VoteSender::UDP(socket, leader_updater) => {
                    let upcoming_leader_sockets = leader_updater.next_leaders(UPCOMING_LEADER_FANOUT);
                    for dst in upcoming_leader_sockets {
                        if socket.send_to(serialized.as_slice(), dst).is_err() {
                            warn!("Failed to send vote to {:?}", dst);
                        }
                    }
                    leader_updater.update();
                }
                VoteSender::QUIC(sender, _) => {
                    sender.try_send_transactions_in_batch(vec![serialized]).unwrap();
                }
            }
        } else {
            warn!("Failed to serialize vote");
        }

        match vote_op {
            VoteOp::PushVote {
                tx, tower_slots, ..
            } => {
                cluster_info.push_vote(&tower_slots, tx);
            }
            VoteOp::RefreshVote {
                tx,
                last_voted_slot,
            } => {
                cluster_info.refresh_vote(tx, last_voted_slot);
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
