use std::ops::Deref;

/// Represents a qualified path to a potential subfield or index of a variant value.
#[derive(Debug, Clone)]
pub struct VariantPath(pub Vec<VariantPathElement>);

impl Deref for VariantPath {
    type Target = Vec<VariantPathElement>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Element of a path
#[derive(Debug, Clone)]
pub enum VariantPathElement {
    /// Access field with name `name`
    Field { name: String },
    /// Access the list element at `index`
    Index { index: usize },
}
