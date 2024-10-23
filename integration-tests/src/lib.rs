use borsh::{BorshDeserialize, BorshSerialize};

pub mod node;
pub mod runtime_utils;
pub mod user;

#[cfg(test)]
mod test_loop;
#[cfg(test)]
mod tests;

#[derive(BorshSerialize, BorshDeserialize)]
struct Struct1 {
    numbers: Vec<i32>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct Struct2 {
    numbers: Box<Vec<i32>>,
}

#[test]
fn box_doesnt_change_serialization() {
    let s1 = Struct1 { numbers: vec![1, 2, 3, 4] };

    let ser = borsh::to_vec(&s1).unwrap();

    let deser: Struct2 = borsh::from_slice(ser.as_slice()).unwrap();

    assert_eq!(deser.numbers.as_slice(), &[1, 2, 3, 4]);
}
