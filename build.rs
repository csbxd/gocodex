use std::{env, fs, path::PathBuf};

fn main() {
    let sdk = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("codex/codex-rs/websocket-client/src");
    let output = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    // The SDK's subprocess tests name their original crate-root module. Adapt
    // that test-only name to the nested module used by this bridge.
    let tests = fs::read_to_string(sdk.join("dialer_tests.rs"))
        .unwrap()
        .replace("dialer::tests::", "websocket_sdk::dialer::tests::");
    fs::write(output.join("dialer_tests.rs"), tests).unwrap();
    let dialer = fs::read_to_string(sdk.join("dialer.rs")).unwrap().replace(
        "#[path = \"dialer_tests.rs\"]",
        &format!("#[path = {:?}]", output.join("dialer_tests.rs")),
    );
    fs::write(output.join("dialer.rs"), dialer).unwrap();
    // Compile the pinned SDK source with one visibility extension. Keep the
    // submodule unchanged and retain its proxy, TLS, cookie and protocol logic.
    let mut source = fs::read_to_string(sdk.join("lib.rs")).unwrap();
    let private = "    async fn connect_with_route(";
    assert_eq!(source.matches(private).count(), 1, "SDK route API changed");
    source = source.replace(private, "    pub(crate) async fn connect_with_route(");
    source = source.replace("//!", "//");
    source = source.replace(
        "mod dialer;",
        &format!("#[path = {:?}]\nmod dialer;", output.join("dialer.rs")),
    );
    for file in ["lib_tests.rs", "cookie_tests.rs"] {
        source = source.replace(
            &format!("#[path = \"{file}\"]"),
            &format!("#[path = {:?}]", sdk.join(file)),
        );
    }
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("websocket_sdk.rs"),
        source,
    )
    .unwrap();
    println!("cargo:rerun-if-changed=codex/codex-rs/websocket-client/src");
    println!("cargo:rerun-if-changed=build.rs");
}
