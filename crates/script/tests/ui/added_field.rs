use structural_convert::StructuralConvert;

struct Model {
    value: String,
    added: bool,
}

#[derive(StructuralConvert)]
#[convert(into(Model))]
struct Adapter {
    value: String,
}

fn main() {}
