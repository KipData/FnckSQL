use crate::errors::DatabaseError;
use crate::expression::function::table::{TableFunction, TableFunctionImpl};
use crate::expression::ScalarExpression;
use crate::serdes::{ReferenceSerialization, ReferenceTables, Serialization};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};
use std::sync::Arc;

impl ReferenceSerialization for TableFunction {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        (self.args.len() as u32).encode(writer)?;
        for arg in self.args.iter() {
            arg.encode(writer, is_direct, reference_tables)?
        }
        let bytes = bincode::serialize(&self.inner)?;
        (bytes.len() as u32).encode(writer)?;
        writer.write_all(&bytes)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let args_len = u32::decode(reader)? as usize;

        let mut args = Vec::with_capacity(args_len);
        for _ in 0..args_len {
            args.push(ScalarExpression::decode(reader, drive, reference_tables)?);
        }
        let mut buf = vec![0u8; u32::decode(reader)? as usize];
        reader.read_exact(&mut buf)?;
        let inner = bincode::deserialize::<Arc<dyn TableFunctionImpl>>(&buf)?;

        Ok(TableFunction { args, inner })
    }
}
