use std::{fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/bindings/src");
    fs::create_dir_all(&directory)?;
    fs::write(
        directory.join("generated.d.ts"),
        kotoconn_script::typescript_declarations(),
    )?;
    Ok(())
}
