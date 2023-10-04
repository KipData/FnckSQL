use sqlparser::parser::ParserError;
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

/// Parse a string to a collection of statements.
///
/// # Example
/// ```rust
/// use kip_sql::parser::parse_sql;
/// let sql = "SELECT a, b, 123, myfunc(b) \
///            FROM table_1 \
///            WHERE a > b AND b < 100 \
///            ORDER BY a DESC, b";
/// let ast = parse_sql(sql).unwrap();
/// println!("{:?}", ast);
/// ```
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}
