/// Allow to clone an object that don't implement the Clone trait as an emtpy object.
pub struct CloneNone<U> {
    value: Option<U>,
}

impl<U> CloneNone<U> {
    pub fn new(value: U) -> Self {
        Self { value: Some(value) }
    }

    /// This method will panic if called after clone.
    pub fn value(&mut self) -> &mut U {
        self.value.as_mut().unwrap()
    }
}

impl<U> Clone for CloneNone<U> {
    fn clone(&self) -> Self {
        Self { value: None }
    }
}
