use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use agave_scheduler_bindings::{
    PackToWorkerMessage, ProgressMessage, TpuToPackMessage, WorkerToPackMessage,
};
use agave_scheduling_utils::handshake::{ClientLogon, client::ClientSession};

const NUM_WORKERS: usize = 4;

fn main() {
    // Collect command line arguments
    let args: Vec<String> = std::env::args().collect();

    // Expect a single argument (besides the program name)
    if args.len() < 2 {
        eprintln!("Usage: {} <path>", args[0]);
        std::process::exit(1);
    }
    let path = &args[1];

    let exit = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, exit.clone())
        .expect("failed to register signal handler");

    let ClientSession {
        allocators,
        tpu_to_pack,
        progress_tracker,
        workers,
    } = agave_scheduling_utils::handshake::client::connect(
        path,
        ClientLogon {
            worker_count: NUM_WORKERS,
            allocator_size: 30 * 1024 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_size: shaq::minimum_file_size::<TpuToPackMessage>(64 * 1024 * 1024),
            progress_tracker_size: shaq::minimum_file_size::<ProgressMessage>(64 * 1024),
            pack_to_worker_size: shaq::minimum_file_size::<PackToWorkerMessage>(64 * 1024),
            worker_to_pack_size: shaq::minimum_file_size::<WorkerToPackMessage>(64 * 1024),
        },
        Duration::from_secs(2),
    )
    .expect("failed to connect to agave");
    let allocator = &allocators[0];

    while !exit.load(Ordering::Relaxed) {}
}
