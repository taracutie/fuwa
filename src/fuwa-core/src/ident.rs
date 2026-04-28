use crate::{Error, Result};

/// Quote a PostgreSQL identifier with double quotes.
///
/// Embedded double quotes are escaped by doubling them.
pub fn quote_ident(identifier: &str) -> Result<String> {
    if identifier.is_empty() {
        return Err(Error::render("identifier cannot be empty"));
    }

    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::quote_ident;

    #[test]
    fn quotes_identifier() {
        assert_eq!(quote_ident("users").unwrap(), "\"users\"");
    }

    #[test]
    fn escapes_embedded_quotes() {
        assert_eq!(quote_ident("weird\"name").unwrap(), "\"weird\"\"name\"");
    }

    #[test]
    fn rejects_empty_identifier() {
        assert!(quote_ident("").is_err());
    }
}
