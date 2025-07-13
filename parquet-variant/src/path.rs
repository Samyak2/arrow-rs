use std::ops::Deref;

/// Represents a qualified path to a potential subfield or index of a variant value.
#[derive(Debug, Clone)]
pub struct VariantPath(Vec<VariantPathElement>);

impl VariantPath {
    pub fn new(path: Vec<VariantPathElement>) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Vec<VariantPathElement> {
        &self.0
    }
}

impl From<Vec<VariantPathElement>> for VariantPath {
    fn from(value: Vec<VariantPathElement>) -> Self {
        Self::new(value)
    }
}

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

impl VariantPathElement {
    pub fn field(name: String) -> VariantPathElement {
        VariantPathElement::Field { name }
    }

    pub fn index(index: usize) -> VariantPathElement {
        VariantPathElement::Index { index }
    }
}
