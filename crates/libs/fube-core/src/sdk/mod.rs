//! Shared SDK declarations. Concrete partition/reporting implementations and
//! native invocation authority are deliberately owned by the runtime.
mod reporting;
mod stateful;
mod stateless;

pub use reporting::*;
pub use stateful::*;
pub use stateless::*;
