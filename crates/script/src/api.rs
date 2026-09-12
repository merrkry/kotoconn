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
                        format!("{}: {}", stringify!($arg), <$ty as TS>::name(cfg))
                    ),*];

                    format!(
                        "{}({}): {};",
                        stringify!($name),
                        args.join(", "),
                        <$ret as TS>::name(cfg)
                    )
                }),* $(, $({
                    let args: Vec<String> = vec![$(
                        format!("{}: {}", stringify!($async_arg), <$async_ty as TS>::name(cfg))
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
