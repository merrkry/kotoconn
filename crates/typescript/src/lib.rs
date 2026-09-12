//! TypeScript module transpilation. Type checking belongs to `tsc`.

use std::path::Path;

use oxc::{
    allocator::Allocator,
    codegen::Codegen,
    parser::Parser,
    semantic::SemanticBuilder,
    span::SourceType,
    transformer::{TransformOptions, Transformer},
};

#[derive(Debug, thiserror::Error)]
#[error("{filename}: {diagnostics}")]
pub struct Error {
    pub filename: String,
    pub diagnostics: String,
}

/// Transpile one TypeScript ES module, preserving its import specifiers.
pub fn transpile(filename: &str, source: &str) -> Result<String, Error> {
    let allocator = Allocator::default();
    let fail = |errors: oxc::diagnostics::Diagnostics| Error {
        filename: filename.into(),
        diagnostics: errors
            .into_iter()
            .map(|error| format!("{error:?}"))
            .collect::<Vec<_>>()
            .join("\n"),
    };

    let parsed = Parser::new(&allocator, source, SourceType::ts()).parse();
    if !parsed.diagnostics.is_empty() {
        return Err(fail(parsed.diagnostics));
    }

    let mut program = parsed.program;
    let semantic = SemanticBuilder::new()
        .with_check_syntax_error(true)
        .with_enum_eval(true)
        .build(&program);
    if !semantic.diagnostics.is_empty() {
        return Err(fail(semantic.diagnostics));
    }

    let transformed = Transformer::new(
        &allocator,
        Path::new(filename),
        &TransformOptions::default(),
    )
    .build_with_scoping(semantic.semantic.into_scoping(), &mut program);
    if !transformed.diagnostics.is_empty() {
        return Err(fail(transformed.diagnostics));
    }

    Ok(Codegen::new().build(&program).code)
}
