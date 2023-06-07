mod rs_parser;

use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

trait SQLParser {
    fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError>;
}
