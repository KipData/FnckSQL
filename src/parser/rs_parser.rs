use crate::parser::SQLParser;
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Tokenizer;

impl SQLParser for RSParser<'_> {
    fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        Parser::parse_sql(dialect, sql)
    }
}

/// SQL Parser based on [`sqlparser`]
///
pub(crate) struct RSParser<'a> {
    parser: Parser<'a>,
}

impl<'a> RSParser<'a> {
    /// Create a new parser for the specified tokens using the
    /// [`GenericDialect`].
    pub(crate) fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        RSParser::new_with_dialect(sql, dialect)
    }

    /// Create a new parser for the specified tokens with the
    /// specified dialect.
    pub(crate) fn new_with_dialect(
        sql: &str,
        dialect: &'a dyn Dialect,
    ) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(RSParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName};

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = RSParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected, "actual:\n{:#?}", statements[0]);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) {
        match RSParser::parse_sql(sql) {
            Ok(statements) => {
                panic!("Expected parse error for '{sql}', but was successful: {statements:?}");
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{expected_error}' not found in actual error '{error_message}'"
                );
            }
        }
    }

    #[test]
    fn test_create_ok() -> Result<(), ParserError> {
        let sql = "CREATE DATABASE db";

        let expected = Statement::CreateDatabase {
            db_name: ObjectName(vec![Ident::new("db")]),
            if_not_exists: false,
            location: None,
            managed_location: None,
        };

        expect_parse_ok(sql, expected)
    }

    #[test]
    fn test_describe_error() -> Result<(), ParserError> {
        let sql = "DESCRIBE ";
        let expected = "Expected identifier, found: EOF";
        expect_parse_error(sql, expected);
        Ok(())
    }

    #[test]
    fn test_parser_error_loc() {
        // test to assert the locations of the referenced token
        let sql = "SELECT this is a syntax error";
        let ast = RSParser::parse_sql(sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: a"
                    .to_string()
            ))
        );
    }

    #[test]
    fn test_nested_explain_error() {
        let sql = "EXPLAIN EXPLAIN SELECT 1";
        let ast = RSParser::parse_sql(sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Explain must be root of the plan".to_string()
            ))
        );
    }

    #[test]
    fn test_deque_insert_error() {
        let sql = "INSERT INTO database.Persons VALUES ()";
        let ast = RSParser::parse_sql(sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected an expression:, found: )".to_string()
            ))
        );
    }

    #[test]
    fn test_deque_select_ok() {
        let sql = "SELECT Persons.LastName, Persons.FirstName, Orders.OrderNo FROM Persons LEFT JOIN Orders ON Persons.Id_P=Orders.Id_P ORDER BY Persons.LastName\
        ; SELECT column_name(s) FROM table_name1 RIGHT JOIN table_name2 ON table_name1.column_name=table_name2.column_name";
        let ast = RSParser::parse_sql(sql);

        assert_eq!(ast.unwrap_or_default().len(), 2);
    }

    #[test]
    fn test_deque_select_error() {
        let sql = "SELECT Persons.LastName, Persons.FirstName, Orders.OrderNo FROM Persons LEFT JOIN Orders ON Persons.Id_P=Orders.Id_P ORDER BY Persons.LastName
        SELECT column_name(s) FROM table_name1 RIGHT JOIN table_name2 ON table_name1.column_name=table_name2.column_name;";
        let ast = RSParser::parse_sql(sql);

        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected end of statement, found: SELECT".to_string()
            ))
        );
    }
}
