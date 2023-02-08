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

#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("config.json semantic check failed: {error_message}")]
    ConfigSemanticsError { error_message: String },
    #[error("genesis.json semantic check failed: {error_message}")]
    GenesisSemanticsError { error_message: String },
    #[error("config.json file issue: {error_message}")]
    ConfigFileError { error_message: String },
    #[error("genesis.json file issue: {error_message}")]
    GenesisFileError { error_message: String },
    #[error("node_key.json file issue: {error_message}")]
    NodeKeyFileError { error_message: String },
    #[error("validator_key.json file issue: {error_message}")]
    ValidatorKeyFileError { error_message: String },
}

pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    pub fn new() -> Self {
        ValidationErrors(Vec::new())
    }

    pub fn push_errors(&mut self, error: ValidationError) {
        self.0.push(error)
    }

    pub fn panic_if_errors(&self) {
        if self.0.is_empty() {
            println!("All validations have passed!")
        } else {
            let mut final_error_message = String::new();
            for error in &self.0 {
                final_error_message += "ERROR: ";
                final_error_message += &error.to_string();
                final_error_message += "\n";
            }
            panic!(
                "\nThe following config checks failed:\n{}\nPlease fix the config json files and validate again!",
                final_error_message
            );
        }
    }
}
