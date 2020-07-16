CREATE TABLE access_keys (
    public_key text NOT NULL,
    account_id text NOT NULL,
    "action" varchar(6) NOT NULL,
    status varchar(7) NOT NULL,
    receipt_hash text NOT NULL,
    block_height numeric(48,0) NOT NULL,
    "permission" varchar(13) NOT NULL,
    CONSTRAINT access_keys_pk PRIMARY KEY (public_key, account_id, "action", receipt_hash)
);
CREATE INDEX access_keys_public_key_idx ON access_keys (public_key);
CREATE INDEX access_keys_account_id_idx ON access_keys (account_id);
CREATE INDEX access_keys_block_height_idx ON access_keys (block_height);
CREATE INDEX access_keys_receipt_hash_idx ON access_keys (receipt_hash);
