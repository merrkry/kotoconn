// rquickjs binds methods; ts-rs supplies their types. Keep names and signatures in one place.
macro_rules! api {
    (
        $class:ident as $interface:ident {
            $(fn $name:ident($this:ident $(, $arg:ident: $ty:ty)*) -> $ret:ty $body:block)*
        }
        $(async {
            $(fn $async_name:ident($async_this:ident $(, $async_arg:ident: $async_ty:ty)*) -> $async_ret:ty $async_body:block)*
        })?
    ) => {
        #[rquickjs::methods]
        impl<'js> $class<'js> {
            $(
                fn $name(&$this, $($arg: $ty),*) -> Result<$ret> $body
            )*
            $($(
                async fn $async_name(&$async_this, $($async_arg: $async_ty),*) -> Result<$async_ret> $async_body
            )*)?
        }

        impl<'js> $class<'js> {
            pub(crate) fn declaration(cfg: &ts_rs::Config) -> String {
                let methods = vec![$({
                    let args: Vec<String> = vec![$(
                        <$ty as Parameter>::declaration(stringify!($arg), cfg)
                    ),*];

                    format!(
                        "{}({}): {};",
                        stringify!($name),
                        args.join(", "),
                        <$ret as TS>::name(cfg)
                    )
                }),* $(, $({
                    let args: Vec<String> = vec![$(
                        <$async_ty as Parameter>::declaration(stringify!($async_arg), cfg)
                    ),*];
                    format!(
                        "{}({}): Promise<{}>;",
                        stringify!($async_name), args.join(", "), <$async_ret as TS>::name(cfg)
                    )
                }),*)?];

                format!(
                    "export interface {} {{ {} }}\n",
                    stringify!($interface),
                    methods.join("\n")
                )
            }
        }
    };
}

pub(crate) use api;

use rquickjs::{
    function::{FromParam, Opt, ParamRequirement, ParamsAccessor},
    promise::Promised,
};
use std::{future::Future, pin::Pin};
use ts_rs::TS;

/// A trailing argument that JS callers may omit.
pub(crate) struct Optional<T>(pub Option<T>);

impl<'js, T: rquickjs::FromJs<'js>> FromParam<'js> for Optional<T> {
    fn param_requirement() -> ParamRequirement {
        Opt::<T>::param_requirement()
    }

    fn from_param<'a>(params: &mut ParamsAccessor<'a, 'js>) -> rquickjs::Result<Self> {
        Opt::from_param(params).map(|Opt(value)| Self(value))
    }
}

/// The result of a method that returns no value.
pub(crate) struct Void;

// Named only inline: a declared alias would export a `Void` type.
impl TS for Void {
    type WithoutGenerics = Self;
    type OptionInnerType = Self;

    fn name(_: &ts_rs::Config) -> String {
        "void".into()
    }

    fn inline(cfg: &ts_rs::Config) -> String {
        Self::name(cfg)
    }
}

impl<'js> rquickjs::IntoJs<'js> for Void {
    fn into_js(self, ctx: &rquickjs::Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        Ok(rquickjs::Value::new_undefined(ctx.clone()))
    }
}

/// Declares one method parameter; omittable arguments become optional parameters.
pub(crate) trait Parameter {
    fn declaration(name: &str, cfg: &ts_rs::Config) -> String;
}

impl<T: TS> Parameter for T {
    fn declaration(name: &str, cfg: &ts_rs::Config) -> String {
        format!("{name}: {}", T::name(cfg))
    }
}

impl<T: TS> Parameter for Optional<T> {
    fn declaration(name: &str, cfg: &ts_rs::Config) -> String {
        format!("{name}?: {}", T::name(cfg))
    }
}

/// A promise for native work that starts when the method is called. An async
/// method only starts its work once the runtime first polls it.
pub(crate) struct Started<'js, T>(Pin<Box<dyn Future<Output = rquickjs::Result<T>> + 'js>>);

impl<'js, T> Started<'js, T> {
    pub fn new(work: impl Future<Output = rquickjs::Result<T>> + 'js) -> Self {
        Self(Box::pin(work))
    }
}

// Named only inline: a declared alias would export a `Started` type.
impl<T: TS> TS for Started<'_, T> {
    type WithoutGenerics = Self;
    type OptionInnerType = Self;

    fn name(cfg: &ts_rs::Config) -> String {
        format!("Promise<{}>", T::name(cfg))
    }

    fn inline(cfg: &ts_rs::Config) -> String {
        Self::name(cfg)
    }
}

impl<'js, T: rquickjs::IntoJs<'js> + 'js> rquickjs::IntoJs<'js> for Started<'js, T> {
    fn into_js(self, ctx: &rquickjs::Ctx<'js>) -> rquickjs::Result<rquickjs::Value<'js>> {
        Promised(self.0).into_js(ctx)
    }
}
