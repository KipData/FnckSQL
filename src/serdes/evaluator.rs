use crate::errors::DatabaseError;
use crate::serdes::Serialization;
use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
use std::io::{Read, Write};

impl Serialization for BinaryEvaluatorBox {
    type Error = DatabaseError;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(self)?;
        (bytes.len() as u32).encode(writer)?;
        writer.write_all(&bytes)?;

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = vec![0u8; u32::decode(reader)? as usize];
        reader.read_exact(&mut buf)?;

        Ok(bincode::deserialize::<Self>(&buf)?)
    }
}

impl Serialization for UnaryEvaluatorBox {
    type Error = DatabaseError;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(self)?;
        (bytes.len() as u32).encode(writer)?;
        writer.write_all(&bytes)?;

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = vec![0u8; u32::decode(reader)? as usize];
        reader.read_exact(&mut buf)?;

        Ok(bincode::deserialize::<Self>(&buf)?)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::Serialization;
    use crate::types::evaluator::boolean::{BooleanNotEqBinaryEvaluator, BooleanNotUnaryEvaluator};
    use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let binary_evaluator = BinaryEvaluatorBox(Arc::new(BooleanNotEqBinaryEvaluator));
        binary_evaluator.encode(&mut cursor)?;

        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(BinaryEvaluatorBox::decode(&mut cursor)?, binary_evaluator);
        cursor.seek(SeekFrom::Start(0))?;
        let unary_evaluator = UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator));
        unary_evaluator.encode(&mut cursor)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(UnaryEvaluatorBox::decode(&mut cursor)?, unary_evaluator);

        Ok(())
    }
}
