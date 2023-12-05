use std::{fs::remove_file, path::Path};

pub fn delete_file_if_exists(path: &str) {
    if Path::exists(Path::new(path)) {
        remove_file(path).unwrap();
    }
}
