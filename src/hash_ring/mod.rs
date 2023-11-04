pub mod transition;

use crate::util::hash::compute_hash;
//use image::{Pixel, Rgba, RgbaImage};
use image::{ImageBuffer, Rgb, RgbImage};
use imageproc::{
    drawing::{
        draw_filled_circle_mut, draw_filled_rect_mut, draw_hollow_circle_mut,
        draw_hollow_ellipse_mut, draw_text_mut,
    },
    rect::Rect,
};
use rusttype::{Font, Scale};
use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet},
    hash::Hash,
};

#[derive(Clone, Debug, PartialEq)]
pub struct HashRingNode {
    position: u64,
    ip_address: String,
    cordoned: bool,
}

impl HashRingNode {
    pub fn from_ip_address(ip_address: String) -> HashRingNode {
        HashRingNode {
            position: compute_hash(&ip_address),
            ip_address,
            cordoned: false,
        }
    }
    fn from_ip_address_and_cordoned(ip_address: String, cordoned: bool) -> HashRingNode {
        HashRingNode {
            position: compute_hash(&ip_address),
            ip_address,
            cordoned,
        }
    }
}

#[derive(Clone, Debug)]
enum HashRingNodeRole {
    /// The node is a leader for the given partition. It will serve
    /// all writes and strongly consistent reads for the partition.
    Leader,

    /// The node has been elected leader for the given partition but
    /// it does not yet have the full history of the partition. It is
    /// catching up to the current leader. It will not serve writes or
    /// reads for the partition. Once it has caught up it will ascend to
    /// leadership of the partition.
    Preleader,

    /// The node has been elected as a follower for the given partition.
    /// It will serve eventually consistent reads for the partition,
    /// but no writes.
    Follower,

    /// The node has been elected as a follower for the given partition
    /// but it does not have the full history of the partition. It is
    /// catching up to the current leader. It will not serve writes or
    /// reads for the partition. Once it has caught up it will ascend to
    /// followership of the partition.
    Prefollower,
}

#[derive(Debug, Clone, PartialEq)]
struct Partition {
    leader: HashRingNode,
    preleader: Option<HashRingNode>,
    followers: Vec<HashRingNode>,
    prefollowers: Vec<HashRingNode>,
}

/// A consistent hash ring for mapping keys to nodes.
#[derive(Debug, Clone)]
pub struct HashRing {
    partitions: HashMap<u64, Partition>,
    nodes_ip_addresses: HashSet<String>,
    cordoned_node_ip_addresses: HashSet<String>,
}

const NUM_FOLLOWERS: usize = 2;

impl HashRing {
    pub fn new() -> HashRing {
        HashRing {
            partitions: HashMap::new(),
            nodes_ip_addresses: HashSet::new(),
            cordoned_node_ip_addresses: HashSet::new(),
        }
    }
    pub fn from_nodes(
        mut nodes: Vec<HashRingNode>,
        //ip_addresses: Vec<HashRingNode>,
        num_partitions: usize,
        //cordoned_node_ip_addresses: HashSet<String>,
    ) -> HashRing {
        // let mut nodes = ip_addresses
        //     .iter()
        //     .map(|ip_address| HashRingNode::from_ip_address(ip_address.clone()))
        //     .collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.position);
        //println!("nodes: {:?}", nodes);

        //let mut node_cycle = nodes.iter().cycle();

        let mut partitions = HashMap::new();
        let partition_size = (u64::MAX / num_partitions as u64)
            .try_into()
            .expect("Partition size overflows usize");
        for partition_position in (0u64..u64::MAX as u64).step_by(partition_size) {
            //println!("partition_position: {}", partition_position);
            let mut skipped = 0;

            let mut partition_nodes = nodes.iter().cycle().skip_while(|node| {
                if skipped >= nodes.len() {
                    return false;
                }
                skipped += 1;
                node.position < partition_position
            });

            let leader = partition_nodes
                .next()
                .expect("Unable to get leader")
                .clone();

            let followers = (0..min(NUM_FOLLOWERS, nodes.len() - 1))
                .map(|i| {
                    partition_nodes
                        .next()
                        .expect(&format!("Unable to get follower {}", i + 1))
                        .clone()
                })
                .collect();

            let partition = Partition {
                leader,
                preleader: None,
                followers,
                prefollowers: vec![],
            };
            //println!("partition: {:?}", partition);
            partitions.insert(partition_position, partition);
            //node_cycle = node_cycle;
        }

        let nodes_ip_addresses =
            HashSet::from_iter(nodes.iter().map(|node| node.ip_address.clone()));
        let cordoned_node_ip_addresses = HashSet::from_iter(
            nodes
                .iter()
                .filter(|node| node.cordoned)
                .map(|node| node.ip_address.clone()),
        );
        HashRing {
            partitions,
            nodes_ip_addresses,
            cordoned_node_ip_addresses,
        }
    }

    // pub fn lookup_partition(&self, partition_key: &str) -> &Partition {
    //     let partition_position = self.partition_position_from_key(partition_key);
    //     self.partitions
    //         .get(&partition_position)
    //         .expect("Missing partition")
    // }
    // fn partition_position_from_key(&self, partition_key: &str) -> u64 {
    //     let hash = compute_hash(partition_key);
    //     hash % self.num_partitions as u64
    // }
}

pub fn hashring_to_image(hash_ring: &HashRing) -> ImageBuffer<Rgb<u8>, Vec<u8>> {
    let mut image = RgbImage::new(600, 400);
    let black = Rgb([0u8, 0u8, 0u8]);
    let white = Rgb([255u8, 255u8, 255u8]);
    draw_filled_rect_mut(&mut image, Rect::at(0, 0).of_size(600, 400), white);
    draw_hollow_ellipse_mut(&mut image, (300, 200), 200, 150, black);

    let font = Vec::from(include_bytes!("../Gilroy-Regular.ttf") as &[u8]);
    let font = Font::try_from_vec(font).unwrap();

    let mut partition_index = 0;
    let mut partitions: Vec<_> = hash_ring.partitions.iter().collect();
    partitions.sort_by(|a, b| a.0.cmp(b.0));
    for partition in partitions.iter() {
        let fraction = *partition.0 as f64 / u64::MAX as f64;
        //println!("fraction: {}", fraction);

        let x = (300.0 + 200.0 * (fraction * 360f64).to_radians().cos()) as i32;
        let y = (200.0 + 150.0 * (fraction * 360f64).to_radians().sin()) as i32;
        draw_filled_circle_mut(&mut image, (x, y), 2, white);
        draw_hollow_circle_mut(&mut image, (x, y), 2, black);
        // let height = 20f32;
        // let scale = Scale {
        //     x: height,
        //     y: height,
        // };
        // draw_text_mut(
        //     &mut image,
        //     black,
        //     x as i32 - 4,
        //     y as i32 - 10,
        //     scale,
        //     &font,
        //     &partition_index.to_string(),
        // );
        // partition_index += 1;
    }
    for partition in partitions {
        let leader = &partition.1.leader;
        let fraction = leader.position as f64 / u64::MAX as f64;
        //println!("fraction: {}", fraction);

        let x = (300.0 + 200.0 * (fraction * 360f64).to_radians().cos()) as i32;
        let y = (200.0 + 150.0 * (fraction * 360f64).to_radians().sin()) as i32;
        draw_filled_circle_mut(&mut image, (x, y), 10, white);
        draw_hollow_circle_mut(&mut image, (x, y), 10, black);

        println!("partition.1.followers {}", partition.1.followers.len());
        for follower in &partition.1.followers {
            let fraction = follower.position as f64 / u64::MAX as f64;
            //println!("fraction: {}", fraction);

            let x = (300.0 + 200.0 * (fraction * 360f64).to_radians().cos()) as i32;
            let y = (200.0 + 150.0 * (fraction * 360f64).to_radians().sin()) as i32;
            draw_filled_circle_mut(&mut image, (x, y), 10, white);
            draw_hollow_circle_mut(&mut image, (x, y), 10, black);
        }
    }

    image
}
