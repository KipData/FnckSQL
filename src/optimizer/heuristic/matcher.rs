#[derive(Clone, Copy)]
pub enum HepMatchOrder {
    /// Match from root down. A match attempt at an ancestor always precedes all match attempts at
    /// its descendants.
    TopDown,
    /// Match from leaves up. A match attempt at a descendant precedes all match attempts at its
    /// ancestors.
    BottomUp,
}