// Shared by Rust interoperability tests and the TypeScript test declaration generator.
#![allow(dead_code)]

use rquickjs::{FromJs, IntoJs, Result};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
};
use structural_convert::StructuralConvert;
use ts_rs::TS;

#[path = "../../src/api.rs"]
mod api;

#[path = "../../src/handler.rs"]
pub mod handler;

#[path = "../../src/task.rs"]
pub mod task;

use api::api;
use handler::Handler;

pub mod model {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, TS)]
    #[ts(type = "{ readonly __brand: unique symbol }")]
    pub struct Reference(pub NonZeroU64);

    #[derive(TS)]
    #[ts(type = "{ readonly __brand: unique symbol }")]
    pub struct OtherReference(pub NonZeroU64);

    #[derive(Clone, Debug, PartialEq, Eq, TS)]
    pub struct Details {
        pub label: String,
        pub enabled: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq, TS)]
    pub struct Options {
        pub details: Details,
        pub parent: Option<Reference>,
        pub values: Vec<u16>,
    }
}

#[derive(
    Clone, rquickjs::class::Trace, rquickjs::JsLifetime, derive_more::From, derive_more::Into, TS,
)]
#[rquickjs::class]
#[ts(as = "model::Reference")]
pub struct Reference {
    #[qjs(skip_trace)]
    pub value: model::Reference,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(model::Details), into(model::Details))]
#[ts(as = "model::Details")]
pub struct Details {
    pub label: String,
    pub enabled: bool,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(model::Options), into(model::Options))]
#[ts(as = "model::Options")]
pub struct Options {
    pub details: Details,
    pub parent: Option<Reference>,
    pub values: Vec<u16>,
}

#[derive(Debug, PartialEq, FromJs, IntoJs, TS)]
pub struct Reply {
    pub label: String,
    pub count: u32,
}

#[derive(TS)]
#[ts(rename_all = "lowercase")]
pub enum Mode {
    Enabled,
    Disabled,
}

#[derive(TS)]
#[ts(tag = "kind", content = "value", rename_all = "lowercase")]
pub enum Choice {
    Empty,
    Named { name: String },
    Pair(String, u16),
}

#[derive(TS)]
pub struct Collections {
    pub table: HashMap<String, Vec<u16>>,
    pub tags: HashSet<String>,
}

#[derive(TS)]
#[ts(
    type = "({ readonly version: 4 } | { readonly version: 6 }) & { readonly __brand: unique symbol }"
)]
pub struct NativeUnion;

#[derive(rquickjs::JsLifetime)]
#[rquickjs::class]
pub struct Service<'js> {
    pub callbacks: std::cell::RefCell<Vec<rquickjs::Function<'js>>>,
}

impl<'js> rquickjs::class::Trace<'js> for Service<'js> {
    fn trace<'a>(&self, tracer: rquickjs::class::Tracer<'a, 'js>) {
        self.callbacks.borrow().trace(tracer);
    }
}

api! {
    Service as Service {
        fn count(self, options: Options) -> u32 {
            Ok(options.values.len().try_into().unwrap())
        }

        fn echo(self, reply: Reply) -> Reply {
            Ok(reply)
        }

        fn register(self, handler: Handler<'js, Reply, Reply>) -> u32 {
            self.callbacks.borrow_mut().push(handler.function);
            Ok(self.callbacks.borrow().len().try_into().unwrap())
        }
    }

    async {
        fn echo_later(self, reply: Reply) -> Reply {
            let owner = std::thread::current().id();
            task::run(async move {
                assert_ne!(owner, std::thread::current().id());
                tokio::task::yield_now().await;
                reply
            }).await
        }

        fn fail(self) -> Reply {
            task::run(async {
                tokio::task::yield_now().await;
                Err(rquickjs::Error::new_from_js_message("native", "reply", "native failed"))
            }).await?
        }
    }
}

pub fn declarations() -> String {
    let cfg = ts_rs::Config::default();
    let types = [
        Reference::decl(&cfg),
        model::OtherReference::decl(&cfg),
        Details::decl(&cfg),
        Options::decl(&cfg),
        Reply::decl(&cfg),
        Mode::decl(&cfg),
        Choice::decl(&cfg),
        Collections::decl(&cfg),
        NativeUnion::decl(&cfg),
        Handler::<(), ()>::decl(&cfg),
    ];

    format!(
        "// Generated test cases; not application configuration.\n{}\n{}",
        types
            .into_iter()
            .map(|t| format!("export {t}"))
            .collect::<Vec<_>>()
            .join("\n"),
        Service::declaration(&cfg)
    )
}
