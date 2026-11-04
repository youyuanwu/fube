use std::{env, fs, os::unix::fs::PermissionsExt, path::PathBuf};

/// Runtime locations survive nextest target remapping. The separate fixed app
/// is an explicit archive.include artifact next to the integration-test binary.
pub fn binary(name: &str, override_name: &str) -> PathBuf {
    let path = env::var_os(override_name)
        .or_else(|| env::var_os(format!("CARGO_BIN_EXE_{name}")))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let current = env::current_exe().expect("current archived test executable");
            let deps = current.parent().expect("test directory");
            assert_eq!(deps.file_name().unwrap(), "deps", "unexpected test layout");
            deps.parent()
                .expect("remapped profile directory")
                .join(name)
        });
    let metadata = fs::metadata(&path).unwrap_or_else(|error| {
        panic!(
            "missing prebuilt {name} at {}: {error}; build workspace binaries before archiving",
            path.display()
        )
    });
    assert!(metadata.is_file() && metadata.permissions().mode() & 0o111 != 0);
    path.canonicalize().expect("prebuilt executable path")
}

pub fn parent() -> PathBuf {
    let path = env::var_os("FUBE_CLUSTER_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    assert!(
        path.is_absolute() && path.is_dir(),
        "existing test temporary parent required"
    );
    path
}
