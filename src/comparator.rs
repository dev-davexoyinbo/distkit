/// Comparison operator used by conditional counter writes.
///
/// The comparator is evaluated against the current observed counter value.
///
/// # Examples
///
/// ```rust
/// use distkit::CounterComparator;
///
/// assert!(CounterComparator::Eq(5).matches(5));
/// assert!(CounterComparator::Lt(5).matches(4));
/// assert!(CounterComparator::Gt(5).matches(6));
/// assert!(CounterComparator::Ne(5).matches(4));
/// assert!(CounterComparator::Nil.matches(42));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterComparator {
    /// Matches when the current value is equal to the embedded operand.
    Eq(i64),
    /// Matches when the current value is less than the embedded operand.
    Lt(i64),
    /// Matches when the current value is greater than the embedded operand.
    Gt(i64),
    /// Matches when the current value is not equal to the embedded operand.
    Ne(i64),
    /// Always matches.
    ///
    /// This is primarily useful for delegating unconditional APIs through the
    /// conditional write path, or for mixing guarded and unguarded writes in a
    /// single batch call.
    Nil,
}

impl CounterComparator {
    /// Returns whether `current` satisfies this comparator.
    pub fn matches(self, current: i64) -> bool {
        match self {
            Self::Eq(expected) => current == expected,
            Self::Lt(expected) => current < expected,
            Self::Gt(expected) => current > expected,
            Self::Ne(expected) => current != expected,
            Self::Nil => true,
        }
    }

    pub(crate) fn as_lua_parts(self) -> (&'static str, i64) {
        match self {
            Self::Eq(expected) => ("eq", expected),
            Self::Lt(expected) => ("lt", expected),
            Self::Gt(expected) => ("gt", expected),
            Self::Ne(expected) => ("ne", expected),
            Self::Nil => ("nil", 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CounterComparator;

    #[test]
    fn matches_uses_embedded_operand_and_nil_always_matches() {
        assert!(CounterComparator::Eq(5).matches(5));
        assert!(CounterComparator::Lt(5).matches(4));
        assert!(CounterComparator::Gt(5).matches(6));
        assert!(CounterComparator::Ne(5).matches(4));
        assert!(CounterComparator::Nil.matches(-99));

        assert!(!CounterComparator::Eq(5).matches(4));
        assert!(!CounterComparator::Lt(5).matches(5));
        assert!(!CounterComparator::Gt(5).matches(5));
        assert!(!CounterComparator::Ne(5).matches(5));
    }
}
