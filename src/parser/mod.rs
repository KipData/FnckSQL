use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

use anyhow::Result;

/// Parse a string to a collection of statements.
///
/// # Example
/// ```rust
/// use mineraldb::parser::parse;
/// let sql = "SELECT a, b, 123, myfunc(b) \
///            FROM table_1 \
///            WHERE a > b AND b < 100 \
///            ORDER BY a DESC, b";
/// let ast = parse(sql).unwrap();
/// println!("{:?}", ast);
/// ```
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements)
}
