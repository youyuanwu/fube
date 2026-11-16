use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// Runtime locations survive nextest target remapping. Separate fixed apps
/// are explicit archive.include artifacts in the remapped profile directory.
pub fn binary(name: &str, override_name: &str) -> PathBuf {
    let path = env::var_os(override_name)
        .or_else(|| env::var_os(format!("CARGO_BIN_EXE_{name}")))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let current = env::current_exe().expect("current archived test executable");
            profile_binary(&current, name)
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

fn profile_binary(executable: &Path, name: &str) -> PathBuf {
    let directory = executable.parent().expect("test directory");
    let artifacts = match directory.file_name().and_then(|name| name.to_str()) {
        Some("deps") => directory,
        Some("out") => {
            // Cargo 1.100 uses <profile>/build/<package>/<unit>/out.
            let build = directory
                .parent()
                .and_then(Path::parent)
                .and_then(Path::parent)
                .expect("test build directory");
            assert_eq!(
                build.file_name().unwrap(),
                "build",
                "unexpected test layout"
            );
            build
        }
        _ => panic!("unexpected test layout: {}", executable.display()),
    };
    artifacts
        .parent()
        .expect("remapped profile directory")
        .join(name)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_paths_follow_remapped_profiles_in_both_cargo_layouts() {
        for profile in [
            "/relocated/target/debug",
            "/relocated/target/release",
            "/relocated/target/x86_64-unknown-linux-gnu/ci",
        ] {
            let profile = Path::new(profile);
            for relative in ["deps/test-hash", "build/package/unit-hash/out/test-hash"] {
                assert_eq!(
                    profile_binary(&profile.join(relative), "hosting-local-app"),
                    profile.join("hosting-local-app")
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "unexpected test layout")]
    fn binary_paths_reject_unrecognized_out_directories() {
        profile_binary(
            Path::new("/relocated/target/debug/other/package/unit-hash/out/test-hash"),
            "hosting-local-app",
        );
    }

    #[test]
    #[should_panic(expected = "unexpected test layout")]
    fn binary_paths_reject_unrecognized_test_directories() {
        profile_binary(
            Path::new("/relocated/target/debug/unknown/test-hash"),
            "hosting-local-app",
        );
    }
}
