use std::io::Read;

use json_comments::StripComments;

pub fn strip_comments_from_str(input: &String) -> std::io::Result<String> {
    let mut content_without_comments = String::new();
    StripComments::new(input.as_bytes()).read_to_string(&mut content_without_comments)?;
    Ok(content_without_comments)
}