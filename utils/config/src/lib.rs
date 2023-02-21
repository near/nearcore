use std::io::Read;

use json_comments::StripComments;

// strip comments from a JSON string with comments.
// the comment formats that are supported: //, /* */ and #.
// json-comments-rs is used
// check out more details: https://github.com/tmccombs/json-comments-rs/blob/main/src/lib.rs
pub fn strip_comments_from_json_str(json_str: &String) -> std::io::Result<String> {
    let mut content_without_comments = String::new();
    StripComments::new(json_str.as_bytes()).read_to_string(&mut content_without_comments)?;
    Ok(content_without_comments)
}

// strip comments from a JSON input with comments.
pub fn strip_comments_from_json_reader(reader: impl Read) -> impl Read {
    StripComments::new(reader)
}

/// errors that arise when loading config files or config semantic checks
/// config files here include: genesis.json, config.json, node_key.json, validator_key.json
#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("config.json semantic issue: {error_message}")]
    ConfigSemanticsError { error_message: String },
    #[error("genesis.json semantic issue: {error_message}")]
    GenesisSemanticsError { error_message: String },
    #[error("config.json file issue: {error_message}")]
    ConfigFileError { error_message: String },
    #[error("genesis.json file issue: {error_message}")]
    GenesisFileError { error_message: String },
    #[error("node_key.json file issue: {error_message}")]
    NodeKeyFileError { error_message: String },
    #[error("validator_key.json file issue: {error_message}")]
    ValidatorKeyFileError { error_message: String },
    #[error("cross config files semantic issue: {error_message}")]
    CrossFileSematicError { error_message: String },
}

/// Used to collect errors on the go.
pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    pub fn new() -> Self {
        ValidationErrors(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push_errors(&mut self, error: ValidationError) {
        self.0.push(error)
    }

    pub fn push_config_semantics_error(&mut self, error_message: String) {
        self.0.push(ValidationError::ConfigSemanticsError { error_message: error_message })
    }

    pub fn push_config_file_error(&mut self, error_message: String) {
        self.0.push(ValidationError::ConfigFileError { error_message: error_message })
    }

    pub fn push_genesis_semantics_error(&mut self, error_message: String) {
        self.0.push(ValidationError::GenesisSemanticsError { error_message: error_message })
    }

    pub fn push_genesis_file_error(&mut self, error_message: String) {
        self.0.push(ValidationError::GenesisFileError { error_message: error_message })
    }

    pub fn push_node_key_file_error(&mut self, error_message: String) {
        self.0.push(ValidationError::NodeKeyFileError { error_message: error_message })
    }

    pub fn push_validator_key_file_error(&mut self, error_message: String) {
        self.0.push(ValidationError::ValidatorKeyFileError { error_message: error_message })
    }

    pub fn push_cross_file_semantics_error(&mut self, error_message: String) {
        self.0.push(ValidationError::CrossFileSematicError { error_message: error_message })
    }

    /// only to be used in panic_if_errors()
    fn generate_final_error_message(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            let mut final_error_message = String::new();
            for error in &self.0 {
                final_error_message += "\n";

                match error {
                    ValidationError::ConfigSemanticsError { error_message }
                    | ValidationError::GenesisSemanticsError { error_message } => {
                        // the final error_message is concatenation of GenesisSemanticsError or ConfigSemanticsError's ever seen
                        // not including the whole error.to_string() makes the final error message less confusing to read
                        final_error_message += error_message
                    }
                    _ => final_error_message += &error.to_string(),
                };
            }
            Some(final_error_message)
        }
    }

    /// concatenate all errors of a certain type in one error message
    /// to be used for error types that tend to appear in multiples, e.g. ConfigSemanticsError and GenesisSemanticsError
    pub fn generate_error_message_per_type(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            let mut final_error_message = String::new();
            for error in &self.0 {
                final_error_message += "\n";
                final_error_message += &error.to_string();
            }
            final_error_message += "\n";
            Some(final_error_message)
        }
    }

    /// only call this function when you want the program to return () or all errors so far
    /// should only be used when you finished inserting all errors
    pub fn return_ok_or_error(&self) -> anyhow::Result<()> {
        if self.0.is_empty() {
            tracing::info!(target: "config", "All validations have passed!");
            Ok(())
        } else {
            Err(anyhow::Error::msg(format!(
                "\nThe following config checks failed:{}\nPlease fix the config json files and validate again!",
                self.generate_final_error_message().unwrap()
            )))
        }
    }
}
