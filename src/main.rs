mod skip_list;
use skip_list::{coin_flip, SkipList};

pub fn main() {
    let mut skip_list = SkipList::new();
    skip_list.insert("key1".to_string(), "value1".to_string(), coin_flip());
    println!("{:?}", skip_list.find("key1"));
}
