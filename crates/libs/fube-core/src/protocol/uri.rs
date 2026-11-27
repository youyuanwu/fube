//! Common::Uri classifiers at native revision 3988b451, not URI parsing or routing.

native_enum!(NativeUriType {
    AuthorityAbEmpty = 0,
    Absolute = 1,
    Rootless = 2,
    Empty = 3,
});

native_enum!(NativeUriHostType {
    None = 0,
    Ipv4 = 1,
    Ipv6 = 2,
    RegName = 3,
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::check_scalar;

    #[test]
    fn uri_classifiers_preserve_signed_long64_numeric_json_and_unknown_codes() {
        for (kind, code) in [
            (NativeUriType::AuthorityAbEmpty, 0),
            (NativeUriType::Absolute, 1),
            (NativeUriType::Rootless, 2),
            (NativeUriType::Empty, 3),
        ] {
            check_scalar(kind, code);
        }
        for (kind, code) in [
            (NativeUriHostType::None, 0),
            (NativeUriHostType::Ipv4, 1),
            (NativeUriHostType::Ipv6, 2),
            (NativeUriHostType::RegName, 3),
        ] {
            check_scalar(kind, code);
        }
        for code in [i64::MIN, -1, 4, i64::MAX] {
            check_scalar(NativeUriType::from(code), code);
            check_scalar(NativeUriHostType::from(code), code);
        }
    }
}
