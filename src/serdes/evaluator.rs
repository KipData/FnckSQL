use crate::implement_serialization_by_bincode;
use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};

implement_serialization_by_bincode!(UnaryEvaluatorBox);
implement_serialization_by_bincode!(BinaryEvaluatorBox);
