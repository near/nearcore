use nbor::{nbor, Deserializable, Serializable};

#[derive(nbor, PartialEq, Debug)]
#[nbor_init(init)]
struct A {
    x: u64,
    b: B,
    y: f32,
    lazy: Option<u64>,
    #[nbor_skip]
    skipped: Option<u64>,
}

impl A {
    pub fn init(&mut self) {
        if let Some(v) = self.lazy.as_mut() {
            *v *= 10;
        }
    }
}

#[derive(nbor, PartialEq, Debug)]
struct B {
    x: u64,
    y: i32,
    c: C,
}

#[derive(nbor, PartialEq, Debug)]
enum C {
    C1,
    C2(u64),
    C3(u64, u64),
    C4 { x: u64, y: u64 },
    C5(D),
}

#[derive(nbor, PartialEq, Debug)]
struct D {
    x: u64,
}

#[test]
fn test_simple_struct() {
    let a = A {
        x: 1,
        b: B { x: 2, y: 3, c: C::C5(D { x: 1 }) },
        y: 4.0,
        lazy: Some(5),
        skipped: Some(6),
    };
    let encoded_a = a.to_vec().unwrap();
    let decoded_a = A::from_slice(&encoded_a).unwrap();
    let expected_a = A {
        x: 1,
        b: B { x: 2, y: 3, c: C::C5(D { x: 1 }) },
        y: 4.0,
        lazy: Some(50),
        skipped: None,
    };

    assert_eq!(expected_a, decoded_a);
}
