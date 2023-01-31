use std::io::Read;

use json_comments::StripComments;

pub fn strip_comments_from_json_str(json_str: &String) -> std::io::Result<String> {
    let mut content_without_comments = String::new();
    StripComments::new(json_str.as_bytes()).read_to_string(&mut content_without_comments)?;
    Ok(content_without_comments)
}

pub fn strip_comments_from_json_reader(reader: impl Read) -> impl Read {
    StripComments::new(reader)
}
