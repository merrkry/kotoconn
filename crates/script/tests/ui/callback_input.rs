#[path = "../../src/handler.rs"]
mod handler;

fn wrong_input(callback: handler::Handler<'_, String, u32>) {
    let _ = callback.call(true);
}

fn main() {}
