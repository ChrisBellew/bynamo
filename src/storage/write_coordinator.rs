// use crate::{skip_list::SkipList, ss_table::SSTable};

// struct WriteCoordinator {
//     skip_list: SkipList,
//     ss_table: SSTable,
// }

// impl WriteCoordinator {
//     pub fn new() -> Self {
//         Self {
//             skip_list: SkipList::new(),
//             ss_table: SSTable::new(),
//         }
//     }
//     pub fn write(&mut self, key: String, value: String) {
//         self.skip_list.insert(key, value);
//     }
//     pub fn flush(&mut self) {
//         self.ss_table.write_from_iter(self.skip_list.iter());
//         self.skip_list = SkipList::new();
//     }
// }
