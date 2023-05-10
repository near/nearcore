use near_vm_types::partial_sum_map::{Error, PartialSumMap};

fn main() {
    bolero::check!().with_type::<(Vec<(u32, u32)>, Vec<u32>)>().for_each(|input| {
        let adds = &input.0;
        let tests = &input.1;
        let mut psm = PartialSumMap::new();
        let mut v = Vec::new();
        let mut size: u32 = 0;
        for a in adds {
            let push_res = psm.push(a.0, a.1);
            match size.checked_add(a.0) {
                None => {
                    assert_eq!(push_res, Err(Error::Overflow));
                    continue;
                }
                Some(new_size) => {
                    assert_eq!(push_res, Ok(()));
                    size = new_size;
                }
            }
            v.push(((size - a.0)..size, a.1));
        }
        for t in tests {
            let psm_answer = psm.find(*t);
            let iset_answer = v.iter().find(|(r, _)| r.contains(&t)).map(|(_, v)| v);
            assert_eq!(psm_answer, iset_answer);
        }
    });
}
