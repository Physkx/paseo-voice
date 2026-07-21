use std::path::PathBuf;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("control-plane crate is inside the repository")
        .to_owned()
}

#[test]
fn production_and_console_entry_points_are_rust_only() {
    let root = repository_root();
    let package = std::fs::read_to_string(root.join("package.json")).expect("package.json");
    let package: serde_json::Value = serde_json::from_str(&package).expect("valid package.json");
    let scripts = package["scripts"].as_object().expect("scripts object");

    for name in ["start", "console", "build", "test"] {
        let command = scripts[name].as_str().expect("script command");
        assert!(
            command.contains("cargo") || command.contains("rust:"),
            "{name} must select Rust: {command}"
        );
        assert!(!command.contains("src/"));
        assert!(!command.contains("dist/"));
    }

    for dependency in ["ws", "zod", "typescript", "vitest", "@types/node"] {
        assert!(
            package["dependencies"].get(dependency).is_none()
                && package["devDependencies"].get(dependency).is_none(),
            "legacy backend dependency remains: {dependency}"
        );
    }
}

#[test]
fn legacy_typescript_backend_and_configuration_are_absent() {
    let root = repository_root();
    for path in ["tsconfig.json", "vitest.config.ts"] {
        assert!(!root.join(path).exists(), "legacy file remains: {path}");
    }
    let source = root.join("src");
    if source.exists() {
        let entries = std::fs::read_dir(source).expect("read legacy src directory");
        assert_eq!(
            entries.count(),
            0,
            "legacy backend source directory is not empty"
        );
    }
    for asset in ["public/index.html", "public/app.js", "public/style.css"] {
        assert!(root.join(asset).is_file(), "browser asset missing: {asset}");
    }
}
