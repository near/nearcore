use std::fmt::Debug;

pub fn pretty_vec<T: Debug>(buf: &[T]) -> String {
    if buf.len() <= 5 {
        format!("{:#?}", buf)
    } else {
        format!(
            "({})[{:#?}, {:#?}, … {:#?}, {:#?}]",
            buf.len(),
            buf[0],
            buf[1],
            buf[buf.len() - 2],
            buf[buf.len() - 1]
        )
    }
}
