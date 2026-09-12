use std::{fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/bindings/src/generated.d.ts");
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(path, kotoconn_script::typescript_declarations())?;
    Ok(())
}
