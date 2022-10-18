macro_rules! debug_page_string {
    ($html_file: literal, $handler: expr) => {
        $handler
            .read_html_file_override($html_file)
            .unwrap_or_else(|| include_str!(concat!("../res/", $html_file)).to_string())
    };
}
