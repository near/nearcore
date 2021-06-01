pub fn bubble_sort() {
    let mut xs: Vec<u64> = (0..100).rev().collect();
    let n = xs.len();
    for i in 0..n {
        for j in 1..(n - i) {
            if xs[j - 1] > xs[j] {
                let tmp = xs[j - 1];
                xs[j - 1] = xs[j];
                xs[j] = tmp;
            }
        }
    }
}
