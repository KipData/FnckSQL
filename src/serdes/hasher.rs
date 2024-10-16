use crate::implement_serialization_by_bincode;
use crate::optimizer::core::cm_sketch::FastHasher;

implement_serialization_by_bincode!(FastHasher);
