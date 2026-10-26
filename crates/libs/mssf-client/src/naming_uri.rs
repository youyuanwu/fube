use std::{fmt, str::FromStr};

use mssf_transport::serialization::serde::{CountedStrings, Untagged};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A checked naming URI in the client's deliberately narrow native profile.
///
/// Supports literal `fabric:/` followed by nonempty ASCII `[A-Za-z0-9_-]+`
/// segments, at most 4096 bytes. Root, escapes, authority, query, fragment and
/// normalization are unsupported. JSON uses a string; native Serde uses the
/// structured NamingUri object, including its redundant path segments.
///
/// ```
/// use mssf_client::NamingUri;
/// let name: NamingUri = "fabric:/App/Service".parse()?;
/// assert_eq!(name.as_str(), "fabric:/App/Service");
/// # Ok::<(), mssf_client::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamingUri(String);

impl NamingUri {
    pub fn parse(value: &str) -> Result<Self> {
        validate(value)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for NamingUri {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl TryFrom<String> for NamingUri {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        validate(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for NamingUri {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl AsRef<str> for NamingUri {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for NamingUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for NamingUri {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return self.0.serialize(serializer);
        }
        Untagged(Components {
            uri_type: 1,
            scheme: "fabric".into(),
            authority: String::new(),
            host_type: 0,
            host: String::new(),
            port: -1,
            path: self.0["fabric:".len()..].into(),
            query: String::new(),
            fragment: String::new(),
            segments: CountedStrings(
                self.0["fabric:/".len()..]
                    .split('/')
                    .map(str::to_owned)
                    .collect(),
            ),
        })
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NamingUri {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            return Self::try_from(String::deserialize(deserializer)?)
                .map_err(serde::de::Error::custom);
        }
        let Untagged(components) = Untagged::<Components>::deserialize(deserializer)?;
        components.into_uri().map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize, Deserialize)]
struct Components {
    uri_type: i64,
    scheme: String,
    authority: String,
    host_type: i64,
    host: String,
    port: i32,
    path: String,
    query: String,
    fragment: String,
    segments: CountedStrings<Vec<String>>,
}

impl Components {
    fn into_uri(self) -> Result<NamingUri> {
        if self.uri_type != 1
            || self.scheme != "fabric"
            || !self.authority.is_empty()
            || self.host_type != 0
            || !self.host.is_empty()
            || self.port != -1
        {
            return Err(Error::Protocol("unsupported native naming URI profile"));
        }
        if !self.query.is_empty() || !self.fragment.is_empty() {
            return Err(Error::Protocol("naming URI has query or fragment"));
        }
        let value = format!("fabric:{}", self.path);
        let uri = NamingUri::try_from(value)
            .map_err(|_| Error::Protocol("unsupported native naming URI value"))?;
        if !self.path[1..]
            .split('/')
            .eq(self.segments.0.iter().map(String::as_str))
        {
            return Err(Error::Protocol("naming URI segments disagree with path"));
        }
        Ok(uri)
    }
}

fn validate(name: &str) -> Result<()> {
    if name.len() > 4096 {
        return Err(Error::InvalidInput("naming URI exceeds 4096 bytes"));
    }
    let suffix = name
        .strip_prefix("fabric:/")
        .ok_or(Error::InvalidInput("naming URI must start with fabric:/"))?;
    if suffix.split('/').any(|segment| {
        segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    }) {
        return Err(Error::InvalidInput(
            "naming URI requires nonempty ASCII alphanumeric, underscore or hyphen segments",
        ));
    }
    Ok(())
}
