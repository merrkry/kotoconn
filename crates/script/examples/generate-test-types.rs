#[path = "../tests/support/types.rs"]
mod types;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/bindings/tests/generated.d.ts");
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(path, types::declarations())?;
    Ok(())
}
