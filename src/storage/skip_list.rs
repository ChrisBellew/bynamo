use super::key_value::KeyValue;
use rand::rngs::StdRng;
use rand::Rng;
use std::cmp::max;
use std::fmt::Display;
use std::fmt::{Formatter, Result};
use std::sync::{Arc, RwLock};
use stopwatch::Stopwatch;

#[derive(Clone)]
pub struct SkipList {
    pub size: usize,
    head: Link<SkipListNode>,
}

type Link<T> = Arc<RwLock<T>>;

impl SkipList {
    pub fn new() -> SkipList {
        SkipList {
            size: 0,
            head: Arc::new(RwLock::new(SkipListNode {
                level: 0,
                key: "".to_string(),
                value: "".to_string(),
                next: None,
                previous: None,
                below: None,
            })),
        }
    }
    pub fn insert(&mut self, key: String, value: String, highest_level: usize) {
        let mut head = self.head.clone();
        log::trace!("Inserting [{}:{}]", key, value);
        // Ensure we have n+1 start nodes
        let new_levels = max(0, highest_level as i32 - head.read().unwrap().level as i32);
        log::trace!("new_levels: {}", new_levels);
        for _ in 0..new_levels {
            let previous_head = head.clone();
            let previous_level = previous_head.read().unwrap().level;
            let new_head = SkipListNode {
                level: previous_level + 1,
                key: "".to_string(),
                value: "".to_string(),
                next: None,
                previous: None,
                below: Some(previous_head),
            };
            head = Arc::new(RwLock::new(new_head));
        }

        // Start at head, the sentinel node on the top level.
        // Navigate down the sentinel nodes until we find the
        // sentinel node of the highest level we are intending
        // to add the node to.
        let mut start = Some(Arc::clone(&head));
        while let Some(node) = start.clone() {
            let node = node.read().unwrap();
            log::trace!(
                "startlevel: {}, highest_level: {}",
                node.level,
                highest_level,
            );
            if node.level <= highest_level {
                break;
            }
            start = node.below.clone();
        }

        let mut added: Option<Arc<RwLock<SkipListNode>>> = None;
        while start.is_some() {
            let mut current = start;
            //log::trace!("start: {}", current.unwrap().read().unwrap().level);
            // Navigate along the nodes at the level until we find
            // a None or or a node with a key greater than the key
            // we are adding
            while let Some(node) = current.clone() {
                let node = node.read().unwrap();
                log::trace!("node: {}:{}", node.level, node.key);
                match &node.next {
                    None => break,
                    Some(next) => {
                        let next_key = &next.read().unwrap().key;
                        if *next_key == key {
                            next.write().unwrap().value = value;
                            return;
                        }
                        if *next_key > key {
                            break;
                        }
                    }
                }
                current = node.next.clone();
            }

            let current = current.expect("Missing node to insert after");

            log::trace!(
                "Adding before {} and after {}",
                current
                    .read()
                    .unwrap()
                    .next
                    .clone()
                    .map(|next| next.read().unwrap().key.clone())
                    .unwrap_or("None".to_string()),
                current.read().unwrap().key,
            );

            let new_node = SkipListNode {
                level: current.read().unwrap().level,
                key: key.clone(),
                value: value.clone(),
                next: current.read().unwrap().next.clone(),
                previous: Some(current.clone()),
                below: None,
            };
            let added_node = Some(Arc::new(RwLock::new(new_node)));
            if let Some(next) = current.read().unwrap().next.clone() {
                next.write().unwrap().previous = added_node.clone();
            }

            if let Some(node) = added {
                node.write().unwrap().below = added_node.clone();
            }

            added = added_node.clone();
            current.write().unwrap().next = added_node;

            // Navigate down from the current node
            let current = current.read().unwrap();
            start = current.below.clone();
        }
        self.size += key.len() + value.len();
        //self.assert_valid();
    }
    pub fn find(&self, key: &str) -> Option<String> {
        let head = self.head.write().unwrap();
        match self.find_node(key) {
            None => None,
            Some(node) => Some(node.read().unwrap().value.clone()),
        }
    }
    fn find_node(&self, key: &str) -> Option<Arc<RwLock<SkipListNode>>> {
        let mut current = Arc::clone(&self.head);
        loop {
            let next = current.read().unwrap().next.clone();
            if let Some(next) = next {
                let next_key = &next.read().unwrap().key;
                if next_key == key {
                    return Some(next.clone());
                }
                if next_key.as_str() < key {
                    //let asd = next.clone();
                    current = next.clone();
                    continue;
                }
            }
            let below = current.read().unwrap().below.clone();
            if let Some(below) = below {
                current = below.clone();
                log::trace!(
                    "Looking for {} on level {}",
                    key,
                    below.read().unwrap().level
                );
                continue;
            }
            break;
        }
        None
    }
    pub fn remove(&self, key: &str) {
        match self.find_node(key) {
            None => {}
            Some(node) => {
                log::trace!(
                    "Found {} for remove on level {}",
                    node.read().unwrap().key,
                    node.read().unwrap().level,
                );
                let mut node = Some(node.clone());
                while let Some(node_to_remove) = node {
                    log::trace!(
                        "Deleting node {} on level {}",
                        node_to_remove.read().unwrap().key,
                        node_to_remove.read().unwrap().level,
                    );
                    let previous = node_to_remove.read().unwrap().previous.clone();
                    let next = node_to_remove.read().unwrap().next.clone();
                    let below = node_to_remove.read().unwrap().below.clone();
                    if let Some(previous) = previous.clone() {
                        previous.write().unwrap().next = next.clone();
                    }
                    if let Some(next) = next {
                        next.write().unwrap().previous = previous;
                    }
                    node_to_remove.write().unwrap().below = None;
                    node = below;
                }
            }
        }
    }
    pub fn count_nodes(&self) -> usize {
        let head = self.head.read().unwrap();

        let mut count = 0;
        let mut start = Some(Arc::clone(&self.head));
        while let Some(node) = start.clone() {
            let node = node.read().unwrap();
            let mut current = node.next.clone();
            while let Some(node) = current.clone() {
                count += 1;
                current = node.read().unwrap().next.clone();
            }
            start = node.below.clone();
        }
        count
    }

    fn assert_valid(&self) {
        let head = self.head.read().unwrap();

        // Traverse all levels and nodes, making sure the keys
        // existing in the correct positions, and the previous
        // and next links are correct.
        let mut start = Arc::clone(&self.head);
        loop {
            let level = start.read().unwrap().level;
            let below = start.read().unwrap().below.clone();
            let mut current = start.clone();
            loop {
                assert_eq!(current.read().unwrap().level, level);
                let next = current.read().unwrap().next.clone();
                let previous = current.read().unwrap().previous.clone();
                if let Some(previous) = previous.clone() {
                    assert_eq!(
                        current.read().unwrap().key,
                        previous
                            .read()
                            .unwrap()
                            .next
                            .as_ref()
                            .unwrap()
                            .read()
                            .unwrap()
                            .key
                    );
                }
                if let Some(next) = next.clone() {
                    assert_eq!(
                        current.read().unwrap().key,
                        next.read()
                            .unwrap()
                            .previous
                            .as_ref()
                            .unwrap()
                            .read()
                            .unwrap()
                            .key
                    );
                    current = next;
                    continue;
                }
                break;
            }
            if let Some(below) = below {
                start = below;
                continue;
            }
            break;
        }
    }

    pub fn iter(&self) -> SkipListIterator {
        let mut current = self.head.clone();
        loop {
            let below = current.read().unwrap().below.clone();
            if let Some(below) = below {
                current = below;
                continue;
            }
            break;
        }

        SkipListIterator { current }
    }
}

pub struct SkipListIterator {
    current: Arc<RwLock<SkipListNode>>,
}

impl Iterator for SkipListIterator {
    type Item = KeyValue;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.current.read().unwrap().next.clone();
        match next {
            None => None,
            Some(next) => {
                self.current = next.clone();
                Some(KeyValue {
                    key: self.current.read().unwrap().key.clone(),
                    value: self.current.read().unwrap().value.clone(),
                })
            }
        }
    }
}

impl Display for SkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut start = Some(Arc::clone(&self.head));

        while let Some(node) = start.clone() {
            let node = node.read().unwrap();

            match node.next.clone() {
                None => write!(f, "{}>:", node.level)?,
                Some(next) => write!(f, "{}>{}:", node.level, next.read().unwrap().key)?,
            };
            let mut current = node.next.clone();
            while let Some(node) = current.clone() {
                let previous = match node.read().unwrap().previous.clone() {
                    None => "None".to_string(),
                    Some(previous) => previous.read().unwrap().key.clone(),
                };
                let next = match node.read().unwrap().next.clone() {
                    None => "None".to_string(),
                    Some(next) => next.read().unwrap().key.clone(),
                };
                write!(
                    f,
                    "{}<[{}:{}:{}]>{}  ",
                    previous,
                    node.read().unwrap().level,
                    node.read().unwrap().key,
                    node.read().unwrap().value,
                    next
                )?;
                current = node.read().unwrap().next.clone();
            }
            write!(f, "\n")?;
            start = node.below.clone();
        }

        Ok(())
    }
}

pub fn coin_flip() -> usize {
    let mut highest_level = 0;
    while rand::thread_rng().gen_bool(0.5) {
        highest_level += 1;
    }
    highest_level
}

pub fn coin_flip_with_rng(rng: &mut StdRng) -> usize {
    let mut highest_level = 0;
    while rng.gen_bool(0.5) {
        highest_level += 1;
    }
    highest_level
}

struct SkipListNode {
    key: String,
    value: String,
    level: usize,
    next: Option<Link<SkipListNode>>,
    previous: Option<Link<SkipListNode>>,
    below: Option<Link<SkipListNode>>,
}

mod tests {
    #[allow(unused_imports)]
    use super::super::skip_list::SkipList;
    #[allow(unused_imports)]
    use crate::storage::skip_list::coin_flip_with_rng;
    #[allow(unused_imports)]
    use rand::{Rng, SeedableRng};
    #[allow(unused_imports)]
    use std::{collections::HashSet, rc::Rc};

    #[test]
    fn add_single_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_structure(&skip_list, vec![vec!["key1".to_string()]]);
    }

    #[test]
    fn add_two_keys_in_order_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![vec!["key1".to_string(), "key2".to_string()]],
        );
    }

    #[test]
    fn add_two_keys_out_of_order_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key2".to_string(), "value2".to_string(), 0);
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![vec!["key1".to_string(), "key2".to_string()]],
        );
    }

    #[test]
    fn add_single_key_on_two_levels() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_structure(
            &skip_list,
            vec![vec!["key1".to_string()], vec!["key1".to_string()]],
        );
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_0_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![
                vec!["key1".to_string(), "key2".to_string()],
                vec!["key2".to_string()],
            ],
        );
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_1_0() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![
                vec!["key1".to_string(), "key2".to_string()],
                vec!["key2".to_string()],
            ],
        );
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_1_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![
                vec!["key1".to_string(), "key2".to_string()],
                vec!["key1".to_string(), "key2".to_string()],
            ],
        );
    }

    #[test]
    fn add_key_twice() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_structure(&skip_list, vec![vec!["key1".to_string()]]);
    }

    #[test]
    fn remove_single_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.remove("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_structure(&skip_list, vec![vec![]]);
    }

    #[test]
    fn remove_other_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.remove("key2");
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_structure(&skip_list, vec![vec!["key1".to_string()]]);
    }

    #[test]
    fn remove_single_key_on_two_levels() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.remove("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_structure(&skip_list, vec![vec![]]);
    }

    #[test]
    fn remove_first_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.remove("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![vec!["key2".to_string()], vec!["key2".to_string()]],
        );
    }

    #[test]
    fn remove_second_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.remove("key2");
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), None);
        assert_structure(
            &skip_list,
            vec![vec!["key1".to_string()], vec!["key1".to_string()]],
        );
    }

    /*
        Finding key21
    Deleting key15
    1:<[key53:53]>key61  key53<[key61:61]>None
    0:<[key14:14]>key15  key14<[key15:15]>key21  key15<[key21:21]>key5  key21<[key5:5]>key53  key5<[key53:53]>key61  key53<[key61:61]>key64  key61<[key64:64]>None

         */
    #[test]
    fn remove_scenario_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key53".to_string(), "value53".to_string(), 1);
        log::trace!("{}", skip_list);
        skip_list.insert("key15".to_string(), "value15".to_string(), 0);
        log::trace!("{}", skip_list);
        skip_list.remove("key15");
        log::trace!("{}", skip_list);
        assert_eq!(skip_list.find("key15"), None);
    }

    #[test]
    //#[ignore]
    fn fuzz_add_and_remove() {
        let iterations = 1000;
        let num_keys = 10;

        let mut rng = rand::rngs::StdRng::seed_from_u64(123);
        for _ in 0..iterations {
            log::trace!("===========================");
            let num = rng.gen_range(1..num_keys);
            log::trace!("Adding {} keys", num);
            let mut keys = HashSet::<String>::new();
            let mut skip_list = SkipList::new();
            for _ in 0..num {
                let (key, value) = loop {
                    let value = rng.gen_range(0..num_keys * 10).to_string();
                    let key = format!("key{}", value);
                    if !keys.contains(&key) {
                        keys.insert(key.clone());
                        break (key, value);
                    }
                };
                log::trace!("Inserting {}", key);

                let highest_level = coin_flip_with_rng(&mut rng);
                log::trace!("{}", highest_level);
                skip_list.insert(key.clone(), value.clone(), highest_level);

                log::trace!("Finding {}", key);
                assert_eq!(skip_list.find(&key), Some(value));
                log::trace!("{}", skip_list);
            }
            for key in keys {
                assert!(skip_list.find(&key).is_some());

                log::trace!("Deleting {}", key);
                skip_list.remove(&key);
                log::trace!("{}", skip_list);

                log::trace!("Ensuring removed {}", key);
                assert_eq!(skip_list.find(&key), None);
            }
        }
    }

    fn assert_structure(skip_list: &SkipList, keys: Vec<Vec<String>>) {
        let mut start = Some(skip_list.head.clone());
        while let Some(node) = start.clone() {
            let node = node.read().unwrap();
            let level = node.level;
            let mut current = node.next.clone();
            let mut index = 0;
            while let Some(node) = current.clone() {
                assert_eq!(
                    node.read().unwrap().key,
                    keys[node.read().unwrap().level][index]
                );
                assert_eq!(node.read().unwrap().level, level);
                if index > 0 {
                    if node
                        .read()
                        .unwrap()
                        .previous
                        .clone()
                        .unwrap()
                        .read()
                        .unwrap()
                        .key
                        != keys[node.read().unwrap().level][index - 1]
                    {
                        log::trace!(
                            "{} previous: {} != {}",
                            node.read().unwrap().key,
                            node.read()
                                .unwrap()
                                .previous
                                .clone()
                                .unwrap()
                                .read()
                                .unwrap()
                                .key,
                            keys[node.read().unwrap().level][index - 1],
                        );
                        panic!("{}", skip_list);
                    }
                }
                if index < keys[node.read().unwrap().level].len() - 1 {
                    if node
                        .read()
                        .unwrap()
                        .next
                        .clone()
                        .unwrap()
                        .read()
                        .unwrap()
                        .key
                        != keys[node.read().unwrap().level][index + 1]
                    {
                        log::trace!(
                            "{} after: {} != {}",
                            node.read().unwrap().key,
                            node.read()
                                .unwrap()
                                .next
                                .clone()
                                .unwrap()
                                .read()
                                .unwrap()
                                .key,
                            keys[node.read().unwrap().level][index + 1],
                        );
                        panic!("{}", skip_list);
                    }
                }
                current = node.read().unwrap().next.clone();
                index += 1;
            }
            start = node.below.clone();
        }
    }
}

pub fn bench_skip_list() -> Result {
    for i in (10_000..=20_000).step_by(10_000) {
        println!("Inserting {}", i);

        let stopwatch = Stopwatch::start_new();
        let mut skip_list = SkipList::new();
        for item in 0..i {
            skip_list.insert(
                format!("key{}", item),
                format!("value{}", item),
                coin_flip(),
            );
        }
        println!("Inserting {} took {}ms", i, stopwatch.elapsed_ms());

        // let stopwatch = Stopwatch::start_new();
        // let mut skip_map = SkipMap::new();
        // for item in 0..i {
        //     skip_map.insert(format!("key{}", item), format!("value{}", item));
        // }
        // println!("Inserting {} skip map took {}ms", i, stopwatch.elapsed_ms());
    }
    Ok(())
}
