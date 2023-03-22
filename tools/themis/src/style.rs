/// ANSI Color Codes
///
/// <https://en.wikipedia.org/wiki/ANSI_escape_code#8-bit>
#[allow(dead_code)]
#[derive(Eq, Copy, Debug, Clone, PartialEq)]
pub enum Color {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,
    Gray { shade: u8 },
    Color256(u8),
}

impl Color {
    pub fn as_ansi(self, foreground: bool) -> String {
        let color = match self {
            Color::Gray { shade } => return Color::Color256(0xE8 + shade).as_ansi(foreground),
            Color::Color256(n) => {
                return format!("\x1b[{};5;{}m", if foreground { 38 } else { 48 }, n)
            }
            Color::Black => 0,
            Color::Red => 1,
            Color::Green => 2,
            Color::Yellow => 3,
            Color::Blue => 4,
            Color::Magenta => 5,
            Color::Cyan => 6,
            Color::White => 7,
        };
        format!("\x1b[{}m", color + if foreground { 30 } else { 40 })
    }
}

pub fn reset() -> &'static str {
    "\x1b[0m"
}

pub fn bg(color: Color) -> String {
    color.as_ansi(false)
}

pub fn fg(color: Color) -> String {
    color.as_ansi(true)
}

pub fn bold() -> &'static str {
    "\x1b[1m"
}
