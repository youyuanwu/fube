//! Trusted local package mechanics and Linux owned-process supervision.
//! No native Service Fabric libraries, Runtime, node or test-host dependency.
#[cfg(not(target_os = "linux"))]
compile_error!("fube-local-host supports Linux only");

pub mod deployment;
pub mod manifest;
pub mod process;

#[derive(Debug)]
pub struct HostError(pub String);
pub type Result<T> = std::result::Result<T, HostError>;
impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for HostError {}
impl From<std::io::Error> for HostError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}
fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(HostError(message.into()))
    }
}
