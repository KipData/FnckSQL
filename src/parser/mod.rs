mod rs_parser;
pub(crate) use self::rs_parser::*;
pub use sqlparser::ast::*;
pub use sqlparser::parser::ParserError;

pub trait SQLParser {
    fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError>;
}
