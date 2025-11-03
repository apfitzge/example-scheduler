use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn main() {
    let exit = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, exit.clone())
        .expect("failed to register signal handler");

    while !exit.load(Ordering::Relaxed) {}
}
