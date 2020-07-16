table! {
    access_keys (public_key, account_id, action, receipt_hash) {
        public_key -> Text,
        account_id -> Text,
        action -> Varchar,
        status -> Varchar,
        receipt_hash -> Text,
        block_height -> Numeric,
        permission -> Varchar,
    }
}
