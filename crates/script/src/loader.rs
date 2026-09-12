use crate::host::Host;
use rquickjs::{
    Class, Ctx, Module, Object, Result,
    module::{Declarations, Exports, ModuleDef},
};
use std::{
    collections::HashMap,
    path::{Component, Path},
};

pub(crate) const MODULE_NAME: &str = "@kotoconn/bindings";

pub(crate) struct Native;

impl ModuleDef for Native {
    fn declare(declarations: &Declarations) -> Result<()> {
        declarations.declare("kotoconn")?;
        Ok(())
    }

    fn evaluate<'js>(ctx: &Ctx<'js>, exports: &Exports<'js>) -> Result<()> {
        exports.export("kotoconn", Class::instance(ctx.clone(), Host::new())?)?;
        Ok(())
    }
}

pub(crate) fn host<'js>(ctx: &Ctx<'js>) -> Result<Class<'js, Host<'js>>> {
    Module::import(ctx, MODULE_NAME)?
        .finish::<Object>()?
        .get("kotoconn")
}

pub(crate) struct Resolver;

impl rquickjs::loader::Resolver for Resolver {
    fn resolve<'js>(
        &mut self,
        _: &Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<rquickjs::loader::ImportAttributes<'js>>,
    ) -> Result<String> {
        if name == MODULE_NAME {
            return Ok(name.into());
        }

        let path = if name.starts_with("./") || name.starts_with("../") {
            Path::new(base).parent().unwrap_or(Path::new("")).join(name)
        } else {
            Path::new(name).to_path_buf()
        };

        let mut components = Vec::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => components.push(part.to_string_lossy().into_owned()),
                Component::CurDir => {}
                Component::ParentDir if !components.is_empty() => {
                    components.pop();
                }
                _ => {
                    return Err(rquickjs::Error::new_resolving_message(
                        base,
                        name,
                        "module name must stay relative to the source root",
                    ));
                }
            }
        }

        Ok(components.join("/"))
    }
}

pub(crate) struct Loader(pub HashMap<String, String>);

impl rquickjs::loader::Loader for Loader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<rquickjs::loader::ImportAttributes<'js>>,
    ) -> Result<Module<'js>> {
        let source = self
            .0
            .get(name)
            .ok_or_else(|| rquickjs::Error::new_loading_message(name, "module was not supplied"))?;

        let source = kotoconn_typescript::transpile(name, source)
            .map_err(|error| rquickjs::Error::new_loading_message(name, error.to_string()))?;

        Module::declare(ctx.clone(), name, source)
    }
}
