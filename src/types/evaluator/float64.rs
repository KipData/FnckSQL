use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use crate::{numeric_binary_evaluator_definition, numeric_unary_evaluator_definition};
use paste::paste;
use serde::{Deserialize, Serialize};
use std::hint;

numeric_unary_evaluator_definition!(Float64, DataValue::Float64);
numeric_binary_evaluator_definition!(Float64, DataValue::Float64);
