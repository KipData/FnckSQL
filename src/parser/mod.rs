use std::collections::VecDeque;
use std::fmt;
use sqlparser::{
    ast::{
        ObjectName, Statement as SQLStatement,
    },
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithLocation, Tokenizer},
};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// Extension DDL for `DESCRIBE TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTableStmt {
    /// Table name
    pub table_name: ObjectName,
}


/// Tokens parsed by [`KParser`] are converted into these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// ANSI SQL AST node (from sqlparser-rs)
    Statement(Box<SQLStatement>),
    /// Extension: `DESCRIBE TABLE`
    DescribeTableStmt(DescribeTableStmt),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Statement(stmt) => write!(f, "{stmt}"),
            Statement::DescribeTableStmt(_) => write!(f, "DESCRIBE TABLE ..."),
        }
    }
}


/// SQL Parser based on [`sqlparser`]
///
pub struct KParser<'a> {
    parser: Parser<'a>,
}

impl<'a> KParser<'a> {
    /// Create a new parser for the specified tokens using the
    /// [`GenericDialect`].
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        KParser::new_with_dialect(sql, dialect)
    }

    /// Create a new parser for the specified tokens with the
    /// specified dialect.
    pub fn new_with_dialect(
        sql: &str,
        dialect: &'a dyn Dialect,
    ) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(KParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    pub fn parse_sql(sql: &str) -> Result<VecDeque<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        KParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL string and produce one or more [`Statement`]s with
    /// with the specified dialect.
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<Statement>, ParserError> {
        let mut parser = KParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(
        &self,
        expected: &str,
        found: TokenWithLocation,
    ) -> Result<T, ParserError> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::DESCRIBE => {
                        // move one token forward
                        self.parser.next_token();
                        // use custom parsing
                        self.parse_describe()
                    }
                    _ => {
                        // use the native parser
                        Ok(Statement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Statement(Box::from(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    /// Parse a SQL `DESCRIBE` statement
    pub fn parse_describe(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        Ok(Statement::DescribeTableStmt(DescribeTableStmt {
            table_name,
        }))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;
    use super::*;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = KParser::parse_sql(sql)?;
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
        match KParser::parse_sql(sql) {
            Ok(statements) => {
                panic!(
                    "Expected parse error for '{sql}', but was successful: {statements:?}"
                );
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
    fn test_parser_error_loc() {
        // test to assert the locations of the referenced token
        let sql = "SELECT this is a syntax error";
        let ast = KParser::parse_sql( sql);
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
        let ast = KParser::parse_sql( sql);
        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Explain must be root of the plan".to_string()
            ))
        );
    }

    #[test]
    fn test_describe_ok()-> Result<(), ParserError> {
        let sql = "DESCRIBE table";
        let expected=Statement::DescribeTableStmt(DescribeTableStmt {
            table_name: ObjectName(vec![Ident{ value: "table".to_string(), quote_style: None }]),
        });
        expect_parse_ok(sql, expected)?;

        Ok(())
    }

    #[test]
    fn test_describe_error()-> Result<(), ParserError> {
        let sql = "DESCRIBE ";
        let expected="Expected identifier, found: EOF";
        expect_parse_error(sql, expected);
        Ok(())
    }



    #[test]
    fn test_deque_insert_error() {
        let sql = "INSERT INTO database.Persons VALUES ()";
        let ast = KParser::parse_sql( sql );
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
        let ast = KParser::parse_sql( sql );

        assert_eq!(
            ast.unwrap_or_default().len(),
            2
        );
    }

    #[test]
    fn test_deque_select_error() {
        let sql = "SELECT Persons.LastName, Persons.FirstName, Orders.OrderNo FROM Persons LEFT JOIN Orders ON Persons.Id_P=Orders.Id_P ORDER BY Persons.LastName
        SELECT column_name(s) FROM table_name1 RIGHT JOIN table_name2 ON table_name1.column_name=table_name2.column_name;";
        let ast = KParser::parse_sql( sql );

        assert_eq!(
            ast,
            Err(ParserError::ParserError(
                "Expected end of statement, found: SELECT"
                    .to_string()
            ))
        );
    }
}