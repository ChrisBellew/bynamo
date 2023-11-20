// #[macro_export]
// macro_rules! error_union {
//   ($enum_name:ident, [$($error_type:ty),+ $(,)?]) => {
//       // Define the enum with variants for each error type.
//       pub enum $enum_name {
//           $(
//               $error_type($error_type),
//           )+
//       }

//       $(
//           // Implement From trait for each error type.
//           impl From<$error_type> for $enum_name {
//               fn from(err: $error_type) -> Self {
//                   $enum_name::$error_type(err)
//               }
//           }
//       )+
//   };
// }
