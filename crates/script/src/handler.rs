use rquickjs::{Ctx, FromJs, Function, Result, Value};
use std::marker::PhantomData;
use ts_rs::TS;

#[derive(TS)]
#[ts(
    type = "(input: Input) => Output | Promise<Output>",
    bound = "Input: TS, Output: TS"
)]
pub(crate) struct Handler<'js, Input, Output> {
    pub function: Function<'js>,
    marker: PhantomData<fn(Input) -> Output>,
}

impl<'js, Input, Output> FromJs<'js> for Handler<'js, Input, Output> {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        Ok(Self {
            function: Function::from_js(ctx, value)?,
            marker: PhantomData,
        })
    }
}

impl<'js, Input: rquickjs::IntoJs<'js>, Output: FromJs<'js>> Handler<'js, Input, Output> {
    pub fn new(function: Function<'js>) -> Self {
        Self {
            function,
            marker: PhantomData,
        }
    }

    pub async fn call(self, input: Input) -> Result<Output> {
        self.function
            .call::<_, rquickjs::promise::MaybePromise>((input,))?
            .into_future()
            .await
    }
}
