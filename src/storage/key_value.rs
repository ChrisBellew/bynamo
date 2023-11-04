use std::{
    fmt::{Display, Formatter},
    iter::from_fn,
};

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub fn merge_sorted_key_value_iters(
    iter_a: impl Iterator<Item = KeyValue>,
    iter_b: impl Iterator<Item = KeyValue>,
) -> impl Iterator<Item = KeyValue> {
    let mut iter_a = iter_a.peekable();
    let mut iter_b = iter_b.peekable();

    from_fn(move || {
        let next_a = iter_a.peek();
        let next_b = iter_b.peek();

        match (next_a, next_b) {
            (Some(a), Some(b)) => {
                if a.key < b.key {
                    iter_a.next()
                } else {
                    iter_b.next()
                }
            }
            (Some(_), None) => iter_a.next(),
            (None, Some(_)) => iter_b.next(),
            (None, None) => None,
        }
    })
}

impl Display for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}]", self.key, self.value)
    }
}
