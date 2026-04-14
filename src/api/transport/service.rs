use crate::api::KeyRange;
use futures::Stream;
use std::ops::Bound;

impl<'a> KeyRange<'a> {
    pub fn unbounded() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
    pub fn from_start(start: &'a [u8]) -> Self {
        Self {
            start: Bound::Included(start),
            end: Bound::Unbounded,
        }
    }
    pub fn between(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> Self {
        Self { start, end }
    }
}
