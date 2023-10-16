use rand::rngs::StdRng;
use rand::Rng;
use std::cmp::max;
use std::fmt::Display;
use std::fmt::{Formatter, Result};
use std::{cell::RefCell, rc::Rc};

pub struct SkipList {
    head: Link<SkipListNode>,
}

type Link<T> = Rc<RefCell<T>>;

impl SkipList {
    pub fn new() -> SkipList {
        SkipList {
            head: Rc::new(RefCell::new(SkipListNode {
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
        println!("Inserting [{}:{}]", key, value);
        // Ensure we have n+1 start nodes
        let new_levels = max(0, highest_level as i32 - self.head.borrow().level as i32);
        println!("new_levels: {}", new_levels);
        for _ in 0..new_levels {
            let head = self.head.to_owned();
            let new_head = SkipListNode {
                level: self.head.borrow().level + 1,
                key: "".to_string(),
                value: "".to_string(),
                next: None,
                previous: None,
                below: Some(head),
            };
            self.head = Rc::new(RefCell::new(new_head));
        }

        // Start at head, the sentinel node on the top level.
        // Navigate down the sentinel nodes until we find the
        // sentinel node of the highest level we are intending
        // to add the node to.
        let mut start = Some(Rc::clone(&self.head));
        while let Some(node) = start.clone() {
            let node = node.borrow();
            println!(
                "startlevel: {}, highest_level: {}",
                node.level, highest_level
            );
            if node.level <= highest_level {
                break;
            }
            start = node.below.clone();
        }

        let mut added: Option<Rc<RefCell<SkipListNode>>> = None;
        while start.is_some() {
            let mut current = start;
            println!("start: {}", current.as_ref().unwrap().borrow().level);
            // Navigate along the nodes at the level until we find
            // a None or or a node with a key greater than the key
            // we are adding
            while let Some(node) = current.clone() {
                let node = node.borrow();
                println!("node: {}:{}", node.level, node.key);
                match &node.next {
                    None => break,
                    Some(next) => {
                        if next.borrow().key == key {
                            next.as_ref().borrow_mut().value = value;
                            return;
                        }
                        if next.borrow().key > key {
                            break;
                        }
                    }
                }
                current = node.next.clone();
            }

            let current = current.expect("Missing node to insert after");

            println!(
                "Adding before {} and after {}",
                current
                    .borrow()
                    .next
                    .clone()
                    .map(|next| next.borrow().key.clone())
                    .unwrap_or("None".to_string()),
                current.borrow().key
            );

            let new_node = SkipListNode {
                level: current.borrow().level,
                key: key.clone(),
                value: value.clone(),
                next: current.borrow().next.clone(),
                previous: Some(current.clone()),
                below: None,
            };
            let added_node = Some(Rc::new(RefCell::new(new_node)));
            if let Some(next) = current.borrow().next.clone() {
                next.as_ref().borrow_mut().previous = added_node.clone();
            }

            if let Some(node) = added {
                node.as_ref().borrow_mut().below = added_node.clone();
            }

            added = added_node.clone();
            current.as_ref().borrow_mut().next = added_node;

            // Navigate down from the current node
            let current = current.borrow();
            start = current.below.clone();
        }
        self.assert_valid();
    }
    pub fn find(&self, key: &str) -> Option<String> {
        match self.find_node(key) {
            None => None,
            Some(node) => Some(node.borrow().value.clone()),
        }
    }
    fn find_node(&self, key: &str) -> Option<Rc<RefCell<SkipListNode>>> {
        //let mut start = Rc::clone(&self.head);

        let mut current = Rc::clone(&self.head);
        loop {
            let next = current.borrow().next.clone();
            if let Some(next) = next {
                let next_key = &next.borrow().key;
                if next_key == key {
                    return Some(next.clone());
                }
                if next_key.as_str() < key {
                    //let asd = next.clone();
                    current = next.clone();
                    continue;
                }
            }
            let below = current.borrow().below.clone();
            if let Some(below) = below {
                current = below.clone();
                println!("Looking for {} on level {}", key, below.borrow().level);
                continue;
            }
            break;
        }
        None
    }
    pub fn delete(&self, key: &str) {
        match self.find_node(key) {
            None => {}
            Some(node) => {
                println!(
                    "Found {} for delete on level {}",
                    node.borrow().key,
                    node.borrow().level
                );
                let mut node = Some(node.clone());
                while let Some(node_to_delete) = node {
                    println!(
                        "Deleting node {} on level {}",
                        node_to_delete.borrow().key,
                        node_to_delete.borrow().level
                    );
                    let previous = node_to_delete.borrow().previous.clone();
                    let next = node_to_delete.borrow().next.clone();
                    let below = node_to_delete.borrow().below.clone();
                    if let Some(previous) = previous.clone() {
                        previous.as_ref().borrow_mut().next = next.clone();
                    }
                    if let Some(next) = next {
                        next.as_ref().borrow_mut().previous = previous;
                    }
                    node_to_delete.as_ref().borrow_mut().below = None;
                    node = below;
                }
            }
        }
    }
    pub fn count_nodes(&self) -> usize {
        let mut count = 0;
        let mut start = Some(Rc::clone(&self.head));
        while let Some(node) = start.clone() {
            let node = node.borrow();
            let mut current = node.next.clone();
            while let Some(node) = current.clone() {
                count += 1;
                current = node.borrow().next.clone();
            }
            start = node.below.clone();
        }
        count
    }

    fn assert_valid(&self) {
        // Traverse all levels and nodes, making sure the keys
        // existing in the correct positions, and the previous
        // and next links are correct.
        let mut start = Rc::clone(&self.head);
        loop {
            let level = start.borrow().level;
            let below = start.borrow().below.clone();
            let mut current = start.clone();
            loop {
                assert_eq!(current.borrow().level, level);
                let next = current.borrow().next.clone();
                let previous = current.borrow().previous.clone();
                if let Some(previous) = previous.clone() {
                    assert_eq!(
                        current.borrow().key,
                        previous.borrow().next.as_ref().unwrap().borrow().key
                    );
                }
                if let Some(next) = next.clone() {
                    assert_eq!(
                        current.borrow().key,
                        next.borrow().previous.as_ref().unwrap().borrow().key
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
}

impl Display for SkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut start = Some(Rc::clone(&self.head));

        while let Some(node) = start.clone() {
            let node = node.borrow();

            match node.next.clone() {
                None => write!(f, "{}>:", node.level)?,
                Some(next) => write!(f, "{}>{}:", node.level, next.borrow().key)?,
            };
            let mut current = node.next.clone();
            while let Some(node) = current.clone() {
                let previous = match node.borrow().previous.clone() {
                    None => "None".to_string(),
                    Some(previous) => previous.borrow().key.clone(),
                };
                let next = match node.borrow().next.clone() {
                    None => "None".to_string(),
                    Some(next) => next.borrow().key.clone(),
                };
                write!(
                    f,
                    "{}<[{}:{}:{}]>{}  ",
                    previous,
                    node.borrow().level,
                    node.borrow().key,
                    node.borrow().value,
                    next
                )?;
                current = node.borrow().next.clone();
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
    use std::{collections::HashSet, rc::Rc};

    use rand::{Rng, SeedableRng};

    use crate::skip_list::SkipList;

    use super::{coin_flip, coin_flip_with_rng};

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
    fn delete_single_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_structure(&skip_list, vec![vec![]]);
    }

    #[test]
    fn delete_other_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.delete("key2");
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_structure(&skip_list, vec![vec!["key1".to_string()]]);
    }

    #[test]
    fn delete_single_key_on_two_levels() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_structure(&skip_list, vec![vec![]]);
    }

    #[test]
    fn delete_first_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
        assert_structure(
            &skip_list,
            vec![vec!["key2".to_string()], vec!["key2".to_string()]],
        );
    }

    #[test]
    fn delete_second_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.delete("key2");
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
    fn delete_scenario_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key53".to_string(), "value53".to_string(), 1);
        println!("{}", skip_list);
        skip_list.insert("key15".to_string(), "value15".to_string(), 0);
        println!("{}", skip_list);
        skip_list.delete("key15");
        println!("{}", skip_list);
        assert_eq!(skip_list.find("key15"), None);
    }

    #[test]
    //#[ignore]
    fn fuzz_add_and_delete() {
        let iterations = 1000;
        let num_keys = 10;

        let mut rng = rand::rngs::StdRng::seed_from_u64(123);
        for _ in 0..iterations {
            println!("===========================");
            let num = rng.gen_range(1..num_keys);
            println!("Adding {} keys", num);
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
                println!("Inserting {}", key);

                let highest_level = coin_flip_with_rng(&mut rng);
                println!("{}", highest_level);
                skip_list.insert(key.clone(), value.clone(), highest_level);
                //assert_structure(&skip_list, vec![vec![key.clone()]]);

                println!("Finding {}", key);
                assert_eq!(skip_list.find(&key), Some(value));
                println!("{}", skip_list);
            }
            // for i in 0..num {
            //     println!("Finding {}", i);
            //     assert_eq!(
            //         skip_list.find(&format!("key{}", i)),
            //         Some(format!("value{}", i))
            //     );
            // }
            for key in keys {
                assert!(skip_list.find(&key).is_some());

                println!("Deleting {}", key);
                skip_list.delete(&key);
                println!("{}", skip_list);

                println!("Ensuring deleted {}", key);
                assert_eq!(skip_list.find(&key), None);
            }
            // for i in 0..num {
            //     assert_eq!(skip_list.find(&format!("key{}", i)), None);
            // }
        }
    }

    fn assert_structure(skip_list: &SkipList, keys: Vec<Vec<String>>) {
        let mut start = Some(Rc::clone(&skip_list.head));
        while let Some(node) = start.clone() {
            let node = node.borrow();
            let level = node.level;
            let mut current = node.next.clone();
            let mut index = 0;
            while let Some(node) = current.clone() {
                assert_eq!(node.borrow().key, keys[node.borrow().level][index]);
                assert_eq!(node.borrow().level, level);
                if index > 0 {
                    if node.borrow().previous.clone().unwrap().borrow().key
                        != keys[node.borrow().level][index - 1]
                    {
                        println!(
                            "{} previous: {} != {}",
                            node.borrow().key,
                            node.borrow().previous.clone().unwrap().borrow().key,
                            keys[node.borrow().level][index - 1]
                        );
                        panic!("{}", skip_list);
                    }
                }
                if index < keys[node.borrow().level].len() - 1 {
                    if node.borrow().next.clone().unwrap().borrow().key
                        != keys[node.borrow().level][index + 1]
                    {
                        println!(
                            "{} after: {} != {}",
                            node.borrow().key,
                            node.borrow().next.clone().unwrap().borrow().key,
                            keys[node.borrow().level][index + 1]
                        );
                        panic!("{}", skip_list);
                    }
                }
                current = node.borrow().next.clone();
                index += 1;
            }
            start = node.below.clone();
        }
    }
}
