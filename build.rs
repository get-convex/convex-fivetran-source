use std::{
    io::Result,
    path::Path,
};

cfg_if::cfg_if! {
    if #[cfg(target_os = "macos")] {
        const PROTOC_BINARY_NAME: &str = "protoc-macos-universal";
    } else if #[cfg(all(target_os = "linux", target_arch = "aarch64"))] {
        const PROTOC_BINARY_NAME: &str = "protoc-linux-aarch64";
    } else if #[cfg(all(target_os = "linux", target_arch = "x86_64"))] {
        const PROTOC_BINARY_NAME: &str = "protoc-linux-x86_64";
    } else {
        panic!("no protoc binary available for this architecture");
    }
}

fn set_protoc_path() {
    let root = Path::new("../pb/protoc");
    if root.exists() {
        let include_path = std::fs::canonicalize(root.join("include"))
            .expect("Failed to canonicalize protoc include path");
        std::env::set_var("PROTOC_INCLUDE", include_path);
        let binary_path = std::fs::canonicalize(root.join(PROTOC_BINARY_NAME))
            .expect("Failed to canonicalize protoc path");
        std::env::set_var("PROTOC", binary_path);
    }
}

fn main() -> Result<()> {
    set_protoc_path();
    println!("cargo:rerun-if-changed=protos");
    let source = Path::new("protos/fivetran_sdk.proto");
    if !source.exists() {
        panic!("Please add {}", source.display());
    }
    tonic_build::configure().compile(&[source], &["protos/"])?;
    Ok(())
}
