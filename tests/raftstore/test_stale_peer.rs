// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! A module contains test cases of stale peers gc.

use std::time::Duration;
use std::thread;

use kvproto::eraftpb::MessageType;
use kvproto::raft_serverpb::{RegionLocalState, PeerState};
use tikv::raftstore::store::{keys, Peekable};

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::{Isolate, IsolateRegionStore};
use super::util::*;

/// A helper function for testing the behaviour of the gc of stale peer
/// which is out of region.
/// If a peer detects the leader is missing for a specified long time,
/// it should consider itself as a stale peer which is removed from the region.
/// This test case covers the following scenario:
/// At first, there are three peer A, B, C in the cluster, and A is leader.
/// Peer B gets down. And then A adds D, E, F int the cluster.
/// Peer D becomes leader of the new cluster, and then removes peer A, B, C.
/// After all these peer in and out, now the cluster has peer D, E, F.
/// If peer B goes up at this moment, it still thinks it is one of the cluster
/// and has peers A, C. However, it could not reach A, C since they are removed from
/// the cluster or probably destroyed.
/// Meantime, D, E, F would not reach B, Since it's not in the cluster anymore.
/// In this case, Peer B would notice that the leader is missing for a long time,
/// and it would check with pd to confirm whether it's still a member of the cluster.
/// If not, it should destroy itself as a stale peer which is removed out already.
fn test_stale_peer_out_of_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // Revise raft base tick interval small to make this test case run fast.
    cluster.cfg.raft_store.raft_base_tick_interval = 10;
    // Specify the heartbeat and election timeout ticks here
    // so that this test case works without depending on the default value of them
    cluster.cfg.raft_store.raft_heartbeat_ticks = 3;
    cluster.cfg.raft_store.raft_election_timeout_ticks = 15;
    // Use a value of 3 seconds as max time here just for test.
    // In production environment, the value of max_leader_missing_duration
    // should be configured far beyond the election timeout.
    let max_leader_missing_duration = Duration::from_secs(3);
    cluster.cfg.raft_store.max_leader_missing_duration = max_leader_missing_duration;
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, key, value);

    // isolate peer 2 from other part of the cluster
    cluster.add_send_filter(Isolate::new(2));

    // add peer [(4, 4), (5, 5), (6, 6)]
    pd_client.must_add_peer(r1, new_peer(4, 4));
    pd_client.must_add_peer(r1, new_peer(5, 5));
    pd_client.must_add_peer(r1, new_peer(6, 6));

    // remove peer [(1, 1), (2, 2), (3, 3)]
    pd_client.must_remove_peer(r1, new_peer(1, 1));
    pd_client.must_remove_peer(r1, new_peer(2, 2));
    pd_client.must_remove_peer(r1, new_peer(3, 3));

    // wait for max_leader_missing_duration to timeout
    thread::sleep(max_leader_missing_duration);
    // check whether this region is still functional properly
    let (key2, value2) = (b"k2", b"v2");
    cluster.must_put(key2, value2);
    assert_eq!(cluster.get(key2), Some(value2.to_vec()));

    // check whether peer(2, 2) and its data are destroyed
    must_get_none(&engine_2, key);
    must_get_none(&engine_2, key2);
}

#[test]
fn test_node_stale_peer_out_of_region() {
    let count = 6;
    let mut cluster = new_node_cluster(0, count);
    test_stale_peer_out_of_region(&mut cluster);
}

#[test]
fn test_server_stale_peer_out_of_region() {
    let count = 6;
    let mut cluster = new_server_cluster(0, count);
    test_stale_peer_out_of_region(&mut cluster);
}

/// A help function for testing the behaviour of the gc of stale peer
/// which is out or region.
/// If a peer detects the leader is missing for a specified long time,
/// it should consider itself as a stale peer which is removed from the region.
/// This test case covers the following scenario:
/// A peer, B is initialized as a replicated peer without data after
/// receiving a single raft AE message. But then it goes through some process like
/// the case of `test_stale_peer_out_of_region`, it's removed out of the region
/// and wouldn't be contacted anymore.
/// In both cases, peer B would notice that the leader is missing for a long time,
/// and it's an initialized peer without any data. It would destroy itself as
/// as stale peer directly.
fn test_stale_peer_without_data<T: Simulator>(cluster: &mut Cluster<T>) {
    // Revise raft base tick interval small to make this test case run fast.
    cluster.cfg.raft_store.raft_base_tick_interval = 10;
    // Specify the heartbeat and election timeout ticks here
    // so that this test case works without depending on the default value of them
    cluster.cfg.raft_store.raft_heartbeat_ticks = 3;
    cluster.cfg.raft_store.raft_election_timeout_ticks = 15;
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = 5;
    // Use a value of 3 seconds as max time here just for test.
    // In production environment, the value of max_leader_missing_duration
    // should be configured far beyond the election timeout.
    let max_leader_missing_duration = Duration::from_secs(3);
    cluster.cfg.raft_store.max_leader_missing_duration = max_leader_missing_duration;
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();
    // block peer (2, 2) at receiving snapshot, but not the heartbeat
    cluster.add_send_filter(IsolateRegionStore::new(1, 2).msg_type(MessageType::MsgSnapshot));

    pd_client.must_add_peer(r1, new_peer(2, 2));

    // wait for the heartbeat broadcasted from peer (1, 1) to peer (2, 2)
    thread::sleep(Duration::from_millis(60));

    // and then isolate peer (2, 2) from peer (1, 1)
    cluster.add_send_filter(Isolate::new(2));

    // wait for max_leader_missing_duration to timeout
    thread::sleep(max_leader_missing_duration);

    // check whether peer(2, 2) is destroyed
    // if it's destroyed, it will write tomstone into the engine.
    let engine = cluster.get_engine(2);
    let state_key = keys::region_state_key(1);
    let state: RegionLocalState = engine.get_msg(&state_key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);
}

#[test]
fn test_node_stale_peer_without_data() {
    let count = 2;
    let mut cluster = new_node_cluster(0, count);
    test_stale_peer_without_data(&mut cluster);
}

#[test]
fn test_server_stale_peer_without_data() {
    let count = 2;
    let mut cluster = new_server_cluster(0, count);
    test_stale_peer_without_data(&mut cluster);
}
