use ed25519_dalek::*;

mod integrations {
    use super::*;
    use near_crypto_ed25519_batch::batch::safe_verify_batch;
    use rand::rngs::OsRng;

    #[test]
    fn safe_verify_batch_seven_signatures() {
        let messages: [&[u8]; 7] = [
            b"Watch closely everyone, I'm going to show you how to kill a god.",
            b"I'm not a cryptographer I just encrypt a lot.",
            b"Still not a cryptographer.",
            b"This is a test of the tsunami alert system. This is only a test.",
            b"Fuck dumbin' it down, spit ice, skip jewellery: Molotov cocktails on me like accessories.",
            b"Hey, I never cared about your bucks, so if I run up with a mask on, probably got a gas can too.",
            b"And I'm not here to fill 'er up. Nope, we came to riot, here to incite, we don't want any of your stuff.", ];
        let mut csprng = OsRng;
        let mut verifying_keys: Vec<VerifyingKey> = Vec::new();
        let mut signatures: Vec<Signature> = Vec::new();

        for msg in messages {
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
        let messages: [&[u8]; 7] = [
            b"Watch closely everyone, I'm going to show you how to kill a god.",
            b"I'm not a cryptographer I just encrypt a lot.",
            b"Still not a cryptographer.",
            b"This is a test of the tsunami alert system. This is only a test.",
            b"Fuck dumbin' it down, spit ice, skip jewellery: Molotov cocktails on me like accessories.",
            b"Hey, I never cared about your bucks, so if I run up with a mask on, probably got a gas can too.",
            b"And I'm not here to fill 'er up. Nope, we came to riot, here to incite, we don't want any of your stuff.", ];
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
        let file = match File::open(test_instance_filename) {
            Ok(file) => file,
            _ => {
                assert!(false, "The test instances file could not be opened");
                unreachable!("Assert False preceeded");
            }
        };

        let reader = BufReader::new(file);
        let data: Vec<Data> = match from_reader(reader) {
            Ok(data) => data,
            _ => {
                assert!(false, "Problem in deserializing the test instances file");
                unreachable!("Assert False preceeded");
            }
        };

        for msg in messages {
            let signing_key: SigningKey = SigningKey::generate(&mut csprng);
            signatures.push(signing_key.sign(msg));
            signing_keys.push(signing_key);
        }
        let mut verifying_keys: Vec<VerifyingKey> =
            signing_keys.iter().map(|key| key.verifying_key()).collect();

        for i in 0..data.len() {
            let mut messages: Vec<&[u8]> = messages.to_vec();
            let sig_bytes = hex::decode(&data[i].signature).expect("Failed to construct signature");
            let pub_bytes =
                hex::FromHex::from_hex(&data[i].pub_key).expect("Failed to construct pub_key");
            let msg_bytes = hex::decode(&data[i].message).expect("Failed to construct message");
            let message: &[u8] = &msg_bytes;
            let signature: &[u8] = &sig_bytes;

            verifying_keys.push(VerifyingKey::from_bytes(&pub_bytes).unwrap());
            signatures.push(Signature::from_slice(&signature).unwrap());
            messages.push(message);

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
