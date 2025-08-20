// cspell:ignore csprng
// This file is based on https://github.com/dalek-cryptography/curve25519-dalek/blob/curve25519-4.1.3/ed25519-dalek/benches/ed25519_benchmarks.rs
// Modifications:
// - Modified test for `safe_verify_batch_seven_signatures` to use `safe_verify_batch`.
// - Removed other tests.
// - Replaced messages with same message used in benchmarks
//
// This file is part of ed25519-dalek.
// Copyright (c) 2017-2019 isis lovecruft
// See LICENSE for licensing information.
//
// Authors:
// - isis agora lovecruft <isis@patternsinthevoid.net>

use ed25519_dalek::*;

mod integrations {
    use super::*;
    use near_crypto_ed25519_batch::safe_verify_batch;
    use near_crypto_ed25519_batch::test_utils::MESSAGE_TO_SIGN;
    use rand::rngs::OsRng;

    #[test]
    fn safe_verify_batch_seven_signatures() {
        let num_messages = 7;
        let messages: Vec<&[u8]> = (0..num_messages).map(|_| MESSAGE_TO_SIGN).collect();
        let mut csprng = OsRng;
        let mut verifying_keys: Vec<VerifyingKey> = Vec::new();
        let mut signatures: Vec<Signature> = Vec::new();

        for msg in &messages {
            let signing_key: SigningKey = SigningKey::generate(&mut csprng);
            signatures.push(signing_key.sign(msg));
            verifying_keys.push(signing_key.verifying_key());
        }

        let result = safe_verify_batch(&messages, &signatures, &verifying_keys);

        assert!(result.is_ok());
    }

    #[test]
    fn safe_verify_batch_extensive_security() {
        use serde::{Deserialize, Serialize};
        use serde_json::from_reader;
        use std::fs::File;
        use std::io::BufReader;

        // Same seven messages as safe_verify_batch_seven_signatures
        let num_messages = 7;
        let messages: Vec<&[u8]> = (0..num_messages).map(|_| MESSAGE_TO_SIGN).collect();
        let mut csprng = OsRng;
        let mut signing_keys: Vec<SigningKey> = Vec::new();
        let mut signatures: Vec<Signature> = Vec::new();

        #[derive(Debug, Serialize, Deserialize)]
        struct Data {
            message: String,
            pub_key: String,
            signature: String,
        }

        let test_instance_filename = "tests/extensive_test_instances.json";
        let Ok(file) = File::open(test_instance_filename) else {
            panic!("The test instances file could not be opened");
        };
        let reader = BufReader::new(file);
        let Ok(data): Result<Vec<Data>, _> = from_reader(reader) else {
            panic!("Problem in deserializing the test instances file");
        };

        for msg in &messages {
            let signing_key = SigningKey::generate(&mut csprng);
            signatures.push(signing_key.sign(msg));
            signing_keys.push(signing_key);
        }
        let mut verifying_keys: Vec<_> =
            signing_keys.iter().map(|key| key.verifying_key()).collect();

        for i in 0..data.len() {
            let mut messages = messages.to_vec();
            let sig_bytes = hex::decode(&data[i].signature).expect("Failed to construct signature");
            let pub_bytes =
                hex::FromHex::from_hex(&data[i].pub_key).expect("Failed to construct pub_key");
            let msg_bytes = hex::decode(&data[i].message).expect("Failed to construct message");

            verifying_keys.push(VerifyingKey::from_bytes(&pub_bytes).unwrap());
            signatures.push(Signature::from_slice(&sig_bytes).unwrap());
            messages.push(&msg_bytes);

            let b = safe_verify_batch(&messages, &signatures, &verifying_keys);
            // Follow the first line of table 5 of [SSR:CGN20] with the exception that we reject
            // low order commitment R (based on the NIST note)
            if i < 3 || i > 5 {
                assert!(b.is_err(), "Should have rejected but did not");
            } else {
                assert!(b.is_ok(), "Should have accepted but did not");
            }
            verifying_keys.pop();
            signatures.pop();
            messages.pop();
        }
    }
}
