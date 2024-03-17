use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::{OmniPaxosDurability, TxLogEntry};
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::messages::*;
use omnipaxos::util::NodeId;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::mpsc,
    time,
};

const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
const TICK_PERIOD: Duration = Duration::from_millis(10);
const TXN_PERIOD: Duration = Duration::from_millis(5);
const LEADER_UPDATE_PERIOD: Duration = Duration::from_millis(10);

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    pub sender_channels: HashMap<NodeId, mpsc::Sender<Message<TxLogEntry>>>,
    pub receiver_channel: mpsc::Receiver<Message<TxLogEntry>>,
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self.node.lock().unwrap().omni_durability.omni_paxos.outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            if self.node.lock().unwrap().connection.contains(&receiver) {
                if let Some(sender) = self.sender_channels.get_mut(&receiver) {
                    let _ = sender.send(msg).await;
                }
            }
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        let mut txn_interval = time::interval(TXN_PERIOD);
        let mut leader_update_interval = time::interval(LEADER_UPDATE_PERIOD);
        loop {
            tokio::select! {
                biased;
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                },
                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().omni_durability.omni_paxos.tick();
                },
                _ = txn_interval.tick() => {
                    self.node.lock().unwrap().apply_replicated_txns();
                },
                _ = leader_update_interval.tick() => {
                    self.node.lock().unwrap().update_leader();
                },
                Some(in_msg) = self.receiver_channel.recv() => {
                    if self.node.lock().unwrap().connection.contains(&in_msg.get_sender()) {
                        self.node.lock().unwrap().omni_durability.omni_paxos.handle_incoming(in_msg);
                    }
                },
                else => {}
            }
        }
    }
}

pub struct Node {
    node_id: NodeId, // Unique identifier for the node
    leader: Option<NodeId>,
    datastore: ExampleDatastore,
    omni_durability: OmniPaxosDurability,
    connection: HashSet<NodeId>, // Set of nodes that this node can communicate with
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability, connection: HashSet<NodeId>) -> Self {
        Node {
            node_id,
            leader: None,
            datastore: ExampleDatastore::new(),
            omni_durability,
            connection,
        }
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        let current_leader = self.omni_durability.omni_paxos.get_current_leader();
        if self.leader != current_leader {
            if let Some(leader) = self.leader {
                if leader == self.node_id {
                    self.datastore
                    .rollback_to_replicated_durability_offset()
                    .expect("failed to rollback");
                }
            }
            if current_leader == Some(self.node_id) {
                self.apply_replicated_txns();
            }
            self.leader = current_leader;
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let durable_tx_offset = self.omni_durability.get_durable_tx_offset();
        let replicated_offset = self.datastore.get_replicated_offset();
        if let Some(durable_offset) = durable_tx_offset {
            match replicated_offset {
                Some(offset) => {
                    if durable_offset.0 > offset.0 {
                        if self.leader == Some(self.node_id) {
                            self.advance_replicated_durability_offset().expect("Failed to advance leader's offset");
                        } else {
                            let txns = self.omni_durability.iter_starting_from_offset(offset);
                            for (tx_offset, tx_data) in txns {
                                self.datastore
                                .replay_transaction(&tx_data)
                                .expect("Failed to replay transaction: Case Some()");
                                self.datastore
                                .advance_replicated_durability_offset(tx_offset)
                                .expect("Invalid replay offset");
                            }
                        }
                    }
                }
                None => {
                    if self.leader == Some(self.node_id) {
                        self.advance_replicated_durability_offset().expect("Failed to advance leader's offset");
                    } else {
                        let txns = self.omni_durability.iter();
                        for (tx_offset, tx_data) in txns {
                            self.datastore
                            .replay_transaction(&tx_data)
                            .expect("Failed to replay transaction: Case None");
                            self.datastore
                            .advance_replicated_durability_offset(tx_offset)
                            .expect("Invalid replay offset");
                        }
                    }
                }
            }
        }
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.datastore.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.datastore.release_tx(tx);
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        if self.leader == Some(self.node_id) {
            Ok(self.datastore.begin_mut_tx())
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        if self.leader == Some(self.node_id) {
            let result = self.datastore.commit_mut_tx(tx);
            match &result {
                Ok(ref tx_result) => {
                    self.omni_durability.append_tx(tx_result.tx_offset, tx_result.tx_data.clone());
                }
                Err(_) => {}
            }
            result
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        if let Some(durable_tx_offset) = self.omni_durability.get_durable_tx_offset() {
            self.datastore.advance_replicated_durability_offset(durable_tx_offset)
        } else {
            Err(DatastoreError::DurabilityOffsetError)
        }
    }

    fn update_connection(&mut self, connection: HashSet<NodeId>) {
        self.connection = connection;
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    // use omnipaxos::util::FlexibleQuorum;
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    // const SERVERS: [NodeId; 3] = [1, 2, 3];
    // const NUM_THREADS: usize = 4;
    const SERVERS: [NodeId; 5] = [1, 2, 3, 4, 5];
    const NUM_THREADS: usize = 6;

    const BUFFER_SIZE: usize = 10000;
    const ELECTION_TICK_TIMEOUT: u64 = 5;

    const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(1000);
    const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<TxLogEntry>>>,
        HashMap<NodeId, mpsc::Receiver<Message<TxLogEntry>>>,
    ) {
        let mut sender_channels = HashMap::new();
        let mut receiver_channels = HashMap::new();
        for pid in SERVERS {
            let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
            sender_channels.insert(pid, sender);
            receiver_channels.insert(pid, receiver);
        }
        (sender_channels, receiver_channels)
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(NUM_THREADS)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels();
        let configuration_id = 1;
        for pid in SERVERS {
            let cluster_config = ClusterConfig {
                configuration_id,
                nodes: SERVERS.into(),
                // flexible_quorum: Some(FlexibleQuorum{read_quorum_size: 2, write_quorum_size: 2})
                ..Default::default()
            };
            let server_config = ServerConfig {
                pid,
                election_tick_timeout: ELECTION_TICK_TIMEOUT,
                ..Default::default()
            };
            let op_config = OmniPaxosConfig {
                cluster_config,
                server_config,
            };
            let omni_paxos = op_config.build(MemoryStorage::default()).expect("failed to build OmniPaxos");
            let omni_durability = OmniPaxosDurability{omni_paxos};
            let connection = SERVERS.iter().filter(|&&x| x != pid).cloned().collect();
            let node = Arc::new(Mutex::new(Node::new(pid, omni_durability, connection)));
            let mut node_runner = NodeRunner {
                node: Arc::clone(&node),
                sender_channels: sender_channels.clone(),
                receiver_channel: receiver_channels.remove(&pid).unwrap(),
            };
            let handle = runtime.spawn(async move {
                node_runner.run().await;
            });
            nodes.insert(pid, (node, handle));
        }
        nodes
    }

    /// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
    #[test]
    fn test_commit_transaction() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, _) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find follower
        let follower = SERVERS.iter().find(|&&pid| pid != leader).unwrap();
        println!("Follower: {:?}", *follower);
        // check replicated offset of follower's datastore before transaction
        let (follower_server, _) = nodes.get(follower).unwrap();
        assert_eq!(follower_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            assert_eq!(follower_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // check whether the transactions are really chosen among the nodes
        let leader_tx = leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        let follower_tx = follower_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in test_examples.clone() {
            assert_eq!(leader_tx.get(&key), Some(val.clone()));
            assert_eq!(follower_tx.get(&key), Some(val.clone()));
            assert_eq!(leader_tx.get(&key), follower_tx.get(&key));
        }
        println!("Transactions are really chosen among the nodes");
        leader_server.lock().unwrap().release_tx(leader_tx);
        follower_server.lock().unwrap().release_tx(follower_tx);
    }

    /// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
    #[test]
    fn test_leader_failure() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, leader_handle) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find follower
        let follower = SERVERS.iter().find(|&&pid| pid != leader).unwrap();
        println!("Follower: {:?}", *follower);
        let (follower_server, _) = nodes.get(follower).unwrap();
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // kill leader
        println!("Killing leader: {}...", leader);
        leader_handle.abort();
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find new leader
        let new_leader = follower_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("New Leader: {}", new_leader);
        // check replicated offset of new leader's datastore
        let (new_leader_server, _) = nodes.get(&new_leader).unwrap();
        let new_leader_tx = new_leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in test_examples.clone() {
            assert_eq!(new_leader_tx.get(&key), Some(val.clone()));
        }
        println!("The replicated state in new leader server is still correct");
        new_leader_server.lock().unwrap().release_tx(new_leader_tx);
        // create new test examples
        let new_test_examples = vec![
            ("5".to_string(), "five".to_string()),
            ("6".to_string(), "six".to_string()),
            ("7".to_string(), "seven".to_string()),
            ("8".to_string(), "eight".to_string()),
            ("9".to_string(), "nine".to_string()),
        ];
        // commit new example transactions
        for (key,val) in new_test_examples.clone() {
            // commit transaction
            let mut tx = new_leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = new_leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(new_leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // check whether the new transactions are really chosen
        let new_leader_tx = new_leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in new_test_examples.clone() {
            assert_eq!(new_leader_tx.get(&key), Some(val.clone()));
        }
        println!("The new transactions are really chosen");
    }

    /// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
    /// Verify that the transaction was first committed in memory but later rolled back.
    #[test]
    fn test_leader_rollback() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, _) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find followers
        let followers: Vec<NodeId> = SERVERS.iter().filter(|&&pid| pid != leader).cloned().collect();
        println!("Followers: {:?}", followers);
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // disconnect leader from followers
        println!("Disconnecting leader: {} from followers...", leader);
        leader_server.lock().unwrap().update_connection(HashSet::new());
        for follower in followers.clone() {
            let connection = followers.clone().into_iter().filter(|&x| x != follower).collect();
            let (follower_server, _) = nodes.get(&follower).unwrap();
            follower_server.lock().unwrap().update_connection(connection);
        }
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find new leader
        let first_follower = followers.first().unwrap();
        let (first_follower_server, _) = nodes.get(first_follower).unwrap();
        let new_leader = first_follower_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("New Leader: {}", new_leader);
        // check replicated state of new leader's datastore
        let replicated_offset = leader_server.lock().unwrap().datastore.get_replicated_offset();
        let (new_leader_server, _) = nodes.get(&new_leader).unwrap();
        assert_eq!(new_leader_server.lock().unwrap().datastore.get_replicated_offset(), replicated_offset);
        let new_leader_tx = new_leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in test_examples.clone() {
            assert_eq!(new_leader_tx.get(&key), Some(val.clone()));
        }
        new_leader_server.lock().unwrap().release_tx(new_leader_tx);
        println!("The replicated state in new leader server is still correct");
        // create new test examples
        let new_test_examples = vec![
            ("5".to_string(), "five".to_string()),
            ("6".to_string(), "six".to_string()),
            ("7".to_string(), "seven".to_string()),
            ("8".to_string(), "eight".to_string()),
            ("9".to_string(), "nine".to_string()),
        ];
        // commit new example transactions in memory
        for (key,val) in new_test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check current offset and replicated offset after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_cur_offset(), Some(TxOffset(example_idx)));
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), replicated_offset);
            println!("Transaction {} is committed in memory", example_idx);
            example_idx += 1;
        }
        // reconnect leader to followers
        println!("Reconnecting leader: {} to followers...", leader);
        leader_server.lock().unwrap().update_connection(followers.clone().into_iter().collect());
        for follower in followers.clone() {
            let connection = SERVERS.into_iter().filter(|&x| x != follower).collect();
            let (follower_server, _) = nodes.get(&follower).unwrap();
            follower_server.lock().unwrap().update_connection(connection);
        }
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // check replicated state and memory state of old leader's datastore
        let leader_tx = leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in test_examples.clone() {
            assert_eq!(leader_tx.get(&key), Some(val.clone()));
        }
        leader_server.lock().unwrap().release_tx(leader_tx);
        println!("The replicated state in old leader server is still correct");
        let leader_tx = leader_server.lock().unwrap().begin_tx(DurabilityLevel::Memory);
        for (key,_) in new_test_examples.clone() {
            assert_eq!(leader_tx.get(&key), None);
        }
        leader_server.lock().unwrap().release_tx(leader_tx);
        println!("The transactions committed in memory are rolled back");
    }

    /// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover?
    /// Case 1: Chained scenario
    #[test]
    fn test_chained_scenario() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, _) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find followers
        let followers: Vec<NodeId> = SERVERS.iter().filter(|&&pid| pid != leader).cloned().collect();
        println!("Followers: {:?}", followers);
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // find first follower
        let first_follower = followers.first().unwrap();
        let (first_follower_server, _) = nodes.get(first_follower).unwrap();
        let second_follower = followers.last().unwrap();
        let (second_follower_server, _) = nodes.get(second_follower).unwrap();
        // disconnect leader from second follower
        println!("Disconnecting leader: {} from follower: {}...", leader, second_follower);
        leader_server.lock().unwrap().update_connection(followers.clone().into_iter().filter(|&x| x != *second_follower).collect());
        let second_follower_connection = second_follower_server.lock().unwrap().connection.clone();
        second_follower_server.lock().unwrap().update_connection(second_follower_connection.clone().into_iter().filter(|&x| x != leader).collect());
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find current leader server
        let current_leader = first_follower_server.lock().unwrap().leader.expect("Failed to get leader");
        let (current_leader_server, _) = nodes.get(&current_leader).unwrap();
        // create new test examples
        let new_test_examples = vec![
            ("5".to_string(), "five".to_string()),
            ("6".to_string(), "six".to_string()),
            ("7".to_string(), "seven".to_string()),
            ("8".to_string(), "eight".to_string()),
            ("9".to_string(), "nine".to_string()),
        ];
        // commit new example transactions
        for (key,val) in new_test_examples.clone() {
            // commit transaction
            let mut tx = current_leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = current_leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(current_leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // check whether the new transactions are really chosen in leader's datastore
        let leader_tx = current_leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in new_test_examples.clone() {
            assert_eq!(leader_tx.get(&key), Some(val.clone()));
        }
        current_leader_server.lock().unwrap().release_tx(leader_tx);
        // check whether the new transactions are really chosen in follower's datastore
        let follower_tx = first_follower_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in new_test_examples.clone() {
            assert_eq!(follower_tx.get(&key), Some(val.clone()));
        }
        first_follower_server.lock().unwrap().release_tx(follower_tx);
        println!("Stable progress is made in case of chained scenario");
    }

    /// Case 2: Quorum-loss scenario
    #[test]
    fn test_quorum_loss_scenario() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, _) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find followers
        let followers: Vec<NodeId> = SERVERS.iter().filter(|&&pid| pid != leader).cloned().collect();
        println!("Followers: {:?}", followers);
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // disconnect leader and followers[1..] from quorum
        let quorum_loss_connection: HashSet<NodeId> = followers.clone().into_iter().take(1).collect();
        println!("quorum_loss_connection: {:?}", quorum_loss_connection.clone());
        println!("Disconnecting leader: {} from quorum...", leader);
        leader_server.lock().unwrap().update_connection(quorum_loss_connection.clone());
        for follower in followers.clone().into_iter().skip(1) {
            println!("Disconnecting follower: {} from quorum...", follower);
            let (follower_server, _) = nodes.get(&follower).unwrap();
            follower_server.lock().unwrap().update_connection(quorum_loss_connection.clone());
        }
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find current leader server
        let current_leader = leader_server.lock().unwrap().leader.expect("Failed to get leader");
        assert_eq!(current_leader, followers[0]);
        println!("Server {} detects that leader is not QC and overtakes it.", current_leader);
        // check whether the old leader lost leadership
        let leadership_in_leader = leader_server.lock().unwrap().leader.unwrap();
        assert_ne!(leadership_in_leader, leader);
        println!("Old leader server {} lost leadership.", leader);
        let (current_leader_server, _) = nodes.get(&current_leader).unwrap();
        // create new test examples
        let new_test_examples = vec![
            ("5".to_string(), "five".to_string()),
            ("6".to_string(), "six".to_string()),
            ("7".to_string(), "seven".to_string()),
            ("8".to_string(), "eight".to_string()),
            ("9".to_string(), "nine".to_string()),
        ];
        // commit new example transactions
        for (key,val) in new_test_examples.clone() {
            // commit transaction
            let mut tx = current_leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = current_leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(current_leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // check whether the new transactions are really chosen in leader's datastore
        let leader_tx = current_leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in new_test_examples.clone() {
            assert_eq!(leader_tx.get(&key), Some(val.clone()));
        }
        current_leader_server.lock().unwrap().release_tx(leader_tx);
        // check whether the new transactions are really chosen in old leader's datastore
        let old_leader_tx = leader_server.lock().unwrap().begin_tx(DurabilityLevel::Replicated);
        for (key,val) in new_test_examples.clone() {
            assert_eq!(old_leader_tx.get(&key), Some(val.clone()));
        }
        leader_server.lock().unwrap().release_tx(old_leader_tx);
        println!("Stable progress is made in case of quorum-loss scenario");
    }

    /// Case 3: Constrained election scenario
    #[test]
    fn test_constrained_election_scenario() {
        // spawn nodes
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find leader
        let (first_server, _) = nodes.get(&1).unwrap();
        let leader = first_server.lock().unwrap().leader.expect("Failed to get leader");
        println!("Leader: {:?}", leader);
        // check replicated offset of leader's datastore before transaction
        let (leader_server, _) = nodes.get(&leader).unwrap();
        assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), None);
        // find followers
        let followers: Vec<NodeId> = SERVERS.iter().filter(|&&pid| pid != leader).cloned().collect();
        println!("Followers: {:?}", followers);
        // create test examples
        let test_examples = vec![
            ("0".to_string(), "zero".to_string()),
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string()),
            ("4".to_string(), "four".to_string()),
        ];
        let mut example_idx = 0;
        // commit example transactions
        for (key,val) in test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // isolate first follower
        let first_follower = followers.first().unwrap();
        let (first_follower_server, _) = nodes.get(first_follower).unwrap();
        println!("Isolating first follower: {} from quorum...", first_follower);
        first_follower_server.lock().unwrap().update_connection(HashSet::new());
        // create new test examples
        let new_test_examples = vec![
            ("5".to_string(), "five".to_string()),
            ("6".to_string(), "six".to_string()),
            ("7".to_string(), "seven".to_string()),
            ("8".to_string(), "eight".to_string()),
            ("9".to_string(), "nine".to_string()),
        ];
        // commit new transactions then reconnect first server, isolate leader and disconnect followers[1..] from quorum
        for (key, val) in new_test_examples.clone() {
            // commit transaction
            let mut tx = leader_server.lock().unwrap().begin_mut_tx().unwrap();
            tx.set(key.clone(), val.clone());
            let result = leader_server.lock().unwrap().commit_mut_tx(tx);
            assert!(result.is_ok());
            std::thread::sleep(WAIT_DECIDED_TIMEOUT);
            // check replicated offsets after transaction
            assert_eq!(leader_server.lock().unwrap().datastore.get_replicated_offset(), Some(TxOffset(example_idx)));
            println!("Transaction {} is chosen", example_idx);
            example_idx += 1;
        }
        // reconnect first follower
        println!("Reconnecting first follower: {} to quorum...", first_follower);
        first_follower_server.lock().unwrap().update_connection(SERVERS.into_iter().filter(|&x| x != *first_follower).collect());
        // isolate leader
        println!("Disconnecting leader: {} from quorum...", leader);
        leader_server.lock().unwrap().update_connection(HashSet::new());
        // disconnect followers[1..] from quorum
        let quorum_loss_connection: HashSet<NodeId> = followers.clone().into_iter().take(1).collect();
        println!("quorum_loss_connection: {:?}", quorum_loss_connection.clone());
        for follower in followers.clone().into_iter().skip(1) {
            let (follower_server, _) = nodes.get(&follower).unwrap();
            follower_server.lock().unwrap().update_connection(quorum_loss_connection.clone());
            println!("Disconnecting follower: {} from quorum...", follower);
        }
        // check whether the log length of first follower is less than the log length of other servers
        assert!(first_follower_server.lock().unwrap().omni_durability.omni_paxos.read_entries(..).unwrap().len() < leader_server.lock().unwrap().omni_durability.omni_paxos.read_entries(..).unwrap().len());
        println!("log length in server {} is less than log length in leader server {}", first_follower, leader);
        for follower in followers.clone().into_iter().skip(1) {
            let (follower_server, _) = nodes.get(&follower).unwrap();
            assert!(first_follower_server.lock().unwrap().omni_durability.omni_paxos.read_entries(..).unwrap().len() < follower_server.lock().unwrap().omni_durability.omni_paxos.read_entries(..).unwrap().len());
            println!("log length in server {} is less than log length in server {}", first_follower, follower)
        }
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        // find current leader server
        let current_leader = first_follower_server.lock().unwrap().leader.expect("Failed to get leader");
        assert_eq!(current_leader, followers[0]);
        println!("Server {} is a qualified leader candidate and become the new leader", current_leader);
    }
}
