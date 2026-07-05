fn main() {
    if std::env::var("CARGO_CFG_WINDOWS").is_ok() {
        let is_msvc = std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc");
        if is_msvc {
            println!(
                "cargo:rustc-link-arg-bin=codex-remote-gateway=/MANIFESTINPUT:packaging/windows/codex-remote-gateway.exe.manifest"
            );
            println!("cargo:rustc-link-arg-bin=codex-remote-gateway=/MANIFEST:EMBED");
        }
        println!("cargo:rerun-if-changed=packaging/windows/codex-remote-gateway.rc");
        println!("cargo:rerun-if-changed=packaging/icons/AppIcon.ico");
        embed_resource::compile(
            "packaging/windows/codex-remote-gateway.rc",
            embed_resource::NONE,
        )
        .manifest_optional()
        .unwrap();
    }
}
