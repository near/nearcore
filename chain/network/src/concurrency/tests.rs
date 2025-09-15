use crate::concurrency::arc_mutex::ArcMutex;

#[test]
fn arc_mutex_update() {
    let v = 5;
    let v2 = 10;
    let v3 = 15;
    let m = ArcMutex::new(v);
    // Loaded value should be the same as constructor argument.
    assert_eq!(v, *m.load());

    // The extra result should be passed forward.
    assert_eq!(
        19,
        m.update(|x| {
            // Initial content of x should be the same as before update.
            assert_eq!(v, x);
            // Concurrent load() should be possible and should return the value from before the update.
            assert_eq!(v, *m.load());
            (19, v2)
        })
    );
    // After update, load() should return the new value.
    assert_eq!(v2, *m.load());

    // try_update returning an Error.
    assert_eq!(
        Err::<(), i32>(35),
        m.try_update(|x| {
            assert_eq!(v2, x);
            assert_eq!(v2, *m.load());
            Err(35)
        })
    );
    assert_eq!(v2, *m.load());

    // try_update returning Ok.
    assert_eq!(
        Ok::<i32, ()>(21),
        m.try_update(|x| {
            assert_eq!(v2, x);
            assert_eq!(v2, *m.load());
            Ok((21, v3))
        })
    );
    assert_eq!(v3, *m.load());
}
