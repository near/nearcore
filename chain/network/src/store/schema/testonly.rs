use crate::store::schema::{Column, Error, Format};

impl super::Store {
    pub fn iter<C: Column>(
        &self,
    ) -> impl Iterator<Item = Result<(<C::Key as Format>::T, <C::Value as Format>::T), Error>> + '_
    {
        debug_assert!(!C::COL.is_rc());
        self.0
            .iter_raw_bytes(C::COL)
            .map(|item| item.and_then(|(k, v)| Ok((C::Key::decode(&k)?, C::Value::decode(&v)?))))
    }
}
