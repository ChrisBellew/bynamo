// use std::collections::{HashMap, HashSet};

// use crate::hash_ring::{HashRingNode, Partition, NUM_FOLLOWERS};

// use super::HashRing;

// /// Returns a new hash ring which is the result of transitioning
// /// from the previous hash ring to the next hash ring.
// ///
// /// At a point in time the cluster will be described by a single
// /// hash ring. Over time nodes will enter and exit the ring. This
// /// causes some nodes to enter or exit leadership or followership.
// /// Given that it takes time for the nodes to switch roles (due to
// /// catching up state of a partition) there will be a period of time
// /// where a node is in a transition state such as preleader or
// /// prefollower. This method assumes that the current hash ring is
// /// the previous state and the next hash ring is the new state. It
// /// will return a new hash ring which will contain nodes in a
// /// transition state if the nodes have switched roles.
// pub fn transition_hash_ring(previous: &HashRing, next: HashRing) -> HashRing {
//     let partitions = transition_partitions(
//         &previous.partitions,
//         next.partitions,
//         &next.nodes_ip_addresses,
//     );
//     HashRing {
//         partitions,
//         nodes_ip_addresses: next.nodes_ip_addresses,
//         cordoned_node_ip_addresses: next.cordoned_node_ip_addresses,
//     }
// }

// fn transition_partitions(
//     previous: &HashMap<u64, Partition>,
//     next: HashMap<u64, Partition>,
//     next_nodes_ip_addresses: &HashSet<String>,
// ) -> HashMap<u64, Partition> {
//     next.into_iter()
//         .map(|next_partition| match previous.get(&next_partition.0) {
//             Some(previous_partition) => (
//                 next_partition.0,
//                 transition_partition(
//                     previous_partition,
//                     next_partition.1,
//                     next_nodes_ip_addresses,
//                 ),
//             ),
//             None => next_partition,
//         })
//         .collect()
// }

// fn transition_partition(
//     previous: &Partition,
//     next: Partition,
//     previous_nodes_ip_addresses: &HashSet<String>,
//     next_nodes_ip_addresses: &HashSet<String>,
// ) -> Partition {
//   let next_node_ip_addresses = vec![next.leader].into_iter().chain(next.followers).chain(next.prefollowers).collect::<HashSet<_>>();
//   //let nodes_in_previous = previous_nodes_ip_addresses.contains(next.leader.ip_address)

//     let (leader, preleader) = if next.leader.ip_address == previous.leader.ip_address {
//         (next.leader, None)
//     } else {
//         (next.leader, None)
//     };

//     let mut followers = Vec::new();
//     let mut prefollowers = Vec::new();

//     for follower in next.followers.iter() {
//         let follower_in_previous = previous
//             .followers
//             .iter()
//             .any(|previous| previous.ip_address == follower.ip_address);
//         if follower_in_previous {
//             followers.push(follower.clone());
//         } else {
//             prefollowers.push(follower.clone());
//         }
//     }

//     for follower in previous.followers.iter() {
//         let follower_in_followers = followers
//             .iter()
//             .any(|next| next.ip_address == follower.ip_address);

//         if !follower_in_followers && followers.len() < NUM_FOLLOWERS {
//             followers.push(follower.clone());
//         }
//     }

//     Partition {
//         leader,
//         preleader,
//         followers,
//         prefollowers,
//     }
// }

// #[test]
// fn transition_empty_to_empty() {
//     let previous = HashRing::new();
//     let next = HashRing::new();
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 0);
// }

// #[test]
// fn transition_empty_to_one_partition_one_node() {
//     let previous = HashRing::new();
//     let next = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions[&0],
//         Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![],
//             prefollowers: vec![],
//         }
//     );
// }

// #[test]
// fn transition_empty_to_one_partition_two_nodes() {
//     let previous = HashRing::new();
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions[&0],
//         Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![HashRingNode::from_ip_address("2.2.2.2".to_string())],
//             prefollowers: vec![],
//         }
//     );
// }

// #[test]
// fn transition_empty_to_one_partition_three_nodes() {
//     let previous = HashRing::new();
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             HashRingNode::from_ip_address("3.3.3.3".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![
//                 HashRingNode::from_ip_address("2.2.2.2".to_string()),
//                 HashRingNode::from_ip_address("3.3.3.3".to_string())
//             ],
//             prefollowers: vec![],
//         }]
//     );
// }

// #[test]
// fn transition_empty_to_one_partition_four_nodes() {
//     let previous = HashRing::new();
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             HashRingNode::from_ip_address("3.3.3.3".to_string()),
//             HashRingNode::from_ip_address("4.4.4.4".to_string()),
//         ],
//         1,
//     );
//     println!("next: {:#?}", next);
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![
//                 HashRingNode::from_ip_address("2.2.2.2".to_string()),
//                 HashRingNode::from_ip_address("3.3.3.3".to_string())
//             ],
//             prefollowers: vec![],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_one_node_same() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![],
//             prefollowers: vec![],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_one_node_different() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("2.2.2.2".to_string())],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             preleader: None,
//             followers: vec![],
//             prefollowers: vec![],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_two_nodes_one_same() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![],
//             prefollowers: vec![HashRingNode::from_ip_address("2.2.2.2".to_string())],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_two_nodes_none_same() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             HashRingNode::from_ip_address("3.3.3.3".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             preleader: Some(HashRingNode::from_ip_address("2.2.2.2".to_string())),
//             followers: vec![],
//             prefollowers: vec![HashRingNode::from_ip_address("3.3.3.3".to_string())],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_three_nodes_one_same() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             HashRingNode::from_ip_address("3.3.3.3".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("1.1.1.1".to_string()),
//             preleader: None,
//             followers: vec![],
//             prefollowers: vec![
//                 HashRingNode::from_ip_address("2.2.2.2".to_string()),
//                 HashRingNode::from_ip_address("3.3.3.3".to_string())
//             ],
//         }]
//     );
// }

// #[test]
// fn transition_one_partition_one_node_to_one_partition_three_nodes_none_same() {
//     let previous = HashRing::from_nodes(
//         vec![HashRingNode::from_ip_address("1.1.1.1".to_string())],
//         1,
//     );
//     let next = HashRing::from_nodes(
//         vec![
//             HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             HashRingNode::from_ip_address("3.3.3.3".to_string()),
//             HashRingNode::from_ip_address("4.4.4.4".to_string()),
//         ],
//         1,
//     );
//     let result = transition_hash_ring(&previous, next);
//     assert_eq!(result.partitions.len(), 1);
//     assert_eq!(
//         result.partitions.into_values().collect::<Vec<_>>(),
//         vec![Partition {
//             leader: HashRingNode::from_ip_address("2.2.2.2".to_string()),
//             preleader: None,
//             followers: vec![
//                 HashRingNode::from_ip_address("3.3.3.3".to_string()),
//                 HashRingNode::from_ip_address("4.4.4.4".to_string())
//             ],
//             prefollowers: vec![],
//         }]
//     );
// }
