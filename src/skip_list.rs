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
        // Ensure we have n+1 start nodes
        let new_levels = max(0, highest_level - self.head.borrow().level);
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
            println!("Inserting level {}", self.head.borrow().level);
        }

        // Start at head, the sentinel node on the top level.
        // Navigate down the sentinel nodes until we find the
        // sentinel node of the highest level we are intending
        // to add the node to.
        let mut start = Some(Rc::clone(&self.head));
        while let Some(node) = start.clone() {
            let node = node.borrow();
            if node.level <= highest_level {
                break;
            }
            start = node.next.clone();
        }
        println!(
            "Starting at level {}",
            start.as_ref().unwrap().borrow().level
        );

        let mut added: Option<Rc<RefCell<SkipListNode>>> = None;
        while start.is_some() {
            let mut current = start;
            // Navigate along the nodes at the level until we find
            // a None or or a node with a key greater than the key
            // we are adding
            while let Some(node) = current.clone() {
                let node = node.borrow();
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
            let new_node = SkipListNode {
                level: current.borrow().level,
                key: key.clone(),
                value: value.clone(),
                next: current.borrow().next.clone(),
                previous: Some(current.clone()),
                below: None,
            };
            println!("Added node {} at level {}", new_node.key, new_node.level);
            let added_node = Some(Rc::new(RefCell::new(new_node)));

            if let Some(node) = added {
                node.borrow_mut().below = added_node.clone();
            }

            added = added_node.clone();
            current.as_ref().borrow_mut().next = added_node;

            // Navigate down from the current node
            let current = current.borrow();
            start = current.below.clone();
        }
    }
    pub fn find(&self, key: &str) -> Option<String> {
        let mut start = Some(Rc::clone(&self.head));
        while start.is_some() {
            let mut current = start;
            while let Some(node) = current.clone() {
                let node = node.borrow();
                println!("Checking node {}", node.key);
                match &node.next {
                    None => break,
                    Some(next) => {
                        if next.borrow().key == *key {
                            return Some(next.borrow().value.clone());
                        }
                        if *next.borrow().key > *key {
                            break;
                        }
                    }
                }
                current = node.next.clone();
            }

            let current = current.expect("Missing node to search below");
            start = current.borrow().below.clone();
        }
        None
    }
    pub fn delete(&self, key: &str) {
        let mut start = Some(Rc::clone(&self.head));
        while start.is_some() {
            let mut current = start;
            while let Some(node) = current.clone() {
                let next = node.borrow().next.clone();
                match next {
                    None => break,
                    Some(next) => {
                        if next.borrow().key == *key {
                            println!("Found to delete");
                            let mut node = Some(next.clone());
                            while let Some(ref node_to_delete) = node {
                                println!("Deleting at level {}", node_to_delete.borrow().level);
                                node_to_delete.borrow().previous.clone().map(|previous| {
                                    previous.borrow_mut().next =
                                        node_to_delete.borrow().next.clone()
                                });

                                if let Some(after) = node_to_delete.borrow().next.clone() {
                                    after.borrow_mut().previous =
                                        node_to_delete.borrow().previous.clone();
                                }
                                let below = node_to_delete.borrow().below.clone();
                                node_to_delete.borrow_mut().below = None;
                                node = below;
                            }
                            return;
                        }
                    }
                }
                current = node.borrow().next.clone();
            }

            let current = current.expect("Missing node to search below");
            start = current.borrow().below.clone();
        }
    }
}

impl Display for SkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut start = Some(Rc::clone(&self.head));

        while let Some(node) = start.clone() {
            let node = node.borrow();
            write!(f, "{}:", node.level)?;
            let mut current = node.next.clone();
            while let Some(node) = current.clone() {
                write!(f, "[{}:{}]  ", node.borrow().key, node.borrow().value)?;
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

struct SkipListNode {
    key: String,
    value: String,
    level: usize,
    next: Option<Link<SkipListNode>>,
    previous: Option<Link<SkipListNode>>,
    below: Option<Link<SkipListNode>>,
}

mod tests {
    use crate::skip_list::SkipList;

    #[test]
    fn add_single_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
    }

    #[test]
    fn add_two_keys_in_order_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn add_two_keys_out_of_order_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key2".to_string(), "value2".to_string(), 0);
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn add_single_key_on_two_levels() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_0_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_1_0() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn add_two_keys_in_order_on_two_levels_1_1() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn add_key_twice() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
    }

    #[test]
    fn delete_single_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
    }

    #[test]
    fn delete_other_key_on_one_level() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 0);
        skip_list.delete("key2");
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
    }

    #[test]
    fn delete_single_key_on_two_levels() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
    }

    #[test]
    fn delete_first_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.delete("key1");
        assert_eq!(skip_list.find("key1"), None);
        assert_eq!(skip_list.find("key2"), Some("value2".to_string()));
    }

    #[test]
    fn delete_second_key() {
        let mut skip_list = SkipList::new();
        skip_list.insert("key1".to_string(), "value1".to_string(), 1);
        skip_list.insert("key2".to_string(), "value2".to_string(), 1);
        skip_list.delete("key2");
        assert_eq!(skip_list.find("key1"), Some("value1".to_string()));
        assert_eq!(skip_list.find("key2"), None);
    }
}
