use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage, SharablePubkeys,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    WorkerToPackMessage,
    pack_message_flags::{self, check_flags},
    processed_codes, worker_message_types,
};
use agave_scheduling_utils::{
    handshake::{
        ClientLogon,
        client::{ClientSession, ClientWorkerSession},
    },
    transaction_ptr::{TransactionPtr, TransactionPtrBatch},
};
use agave_transaction_view::{
    transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
};
use core::ptr::NonNull;
use rts_alloc::Allocator;
use shaq::Consumer;
use solana_pubkey::Pubkey;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

const NUM_WORKERS: usize = 5;
const QUEUE_CAPACITY: usize = 100_000;

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
        mut tpu_to_pack,
        mut progress_tracker,
        mut workers,
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

    let mut queue = VecDeque::with_capacity(QUEUE_CAPACITY);

    while !exit.load(Ordering::Relaxed) {
        handle_tpu_messages(
            allocator,
            &mut tpu_to_pack,
            &mut workers[NUM_WORKERS - 1],
            &mut queue,
        );

        for worker in workers.iter_mut() {
            handle_worker_messages(&allocator, worker)
        }
    }
}

struct PubkeysPtr {
    ptr: NonNull<Pubkey>,
    count: usize,
}

impl PubkeysPtr {
    unsafe fn from_sharable_pubkeys(
        sharable_pubkeys: &SharablePubkeys,
        allocator: &Allocator,
    ) -> Self {
        let ptr = allocator.ptr_from_offset(sharable_pubkeys.offset).cast();
        Self {
            ptr,
            count: sharable_pubkeys.num_pubkeys as usize,
        }
    }

    fn as_slice(&self) -> &[Pubkey] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }

    unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr.cast()) };
    }
}

struct TransactionEntry {
    view: SanitizedTransactionView<TransactionPtr>,
    loaded_addresses: Option<PubkeysPtr>,
}

fn handle_tpu_messages(
    allocator: &Allocator,
    tpu_to_pack: &mut Consumer<TpuToPackMessage>,
    resolving_worker: &mut ClientWorkerSession,
    queue: &mut VecDeque<TransactionEntry>,
) {
    tpu_to_pack.sync();

    let mut txs_to_resolve = Vec::with_capacity(MAX_TRANSACTIONS_PER_MESSAGE);

    while let Some(message) = tpu_to_pack.try_read() {
        let message = unsafe { message.as_ref() };

        // If at capacity, we will just drop the transaction here.
        let tx_ptr = unsafe {
            TransactionPtr::from_sharable_transaction_region(&message.transaction, allocator)
        };
        if queue.len() >= QUEUE_CAPACITY {
            unsafe { tx_ptr.free(allocator) };
            continue;
        }

        let Ok(view) = SanitizedTransactionView::try_new_sanitized(
            unsafe {
                TransactionPtr::from_sharable_transaction_region(&message.transaction, allocator)
            },
            true,
        ) else {
            unsafe { tx_ptr.free(allocator) };
            continue;
        };

        // V0 transactions get sent off for resolution.
        if matches!(view.version(), TransactionVersion::V0) {
            txs_to_resolve.push(unsafe { tx_ptr.to_sharable_transaction_region(allocator) });
            if txs_to_resolve.len() == MAX_TRANSACTIONS_PER_MESSAGE {
                send_resolve_requests(allocator, resolving_worker, &txs_to_resolve);
                txs_to_resolve.clear();
            }
            continue;
        }

        // Non-ALT transactions go immediately to queue.
        queue.push_back(TransactionEntry {
            view,
            loaded_addresses: None,
        });
    }

    if !txs_to_resolve.is_empty() {
        send_resolve_requests(allocator, resolving_worker, &txs_to_resolve);
        txs_to_resolve.clear();
    }

    tpu_to_pack.finalize();
}

fn handle_worker_messages(allocator: &Allocator, worker: &mut ClientWorkerSession) {
    worker.worker_to_pack.sync();

    while let Some(message) = worker.worker_to_pack.try_read() {
        let message = unsafe { message.as_ref() };

        let batch = unsafe {
            TransactionPtrBatch::from_sharable_transaction_batch_region(&message.batch, allocator)
        };

        let processed = match message.processed_code {
            processed_codes::PROCESSED => true,
            processed_codes::MAX_WORKING_SLOT_EXCEEDED => false,
            processed_codes::INVALID => {
                panic!("We produced a message agave did not understand!");
            }
            _ => {
                panic!("agave produced a message we do not understand!")
            }
        };

        match message.responses.tag {
            worker_message_types::EXECUTION_RESPONSE => {
                todo!("handle execution response");
            }
            worker_message_types::CHECK_RESPONSE => {
                todo!("handle check response");
            }
            _ => {
                panic!("agave sent a message with tag we do not understand!");
            }
        }
    }

    worker.worker_to_pack.finalize();
}

fn send_resolve_requests(
    allocator: &Allocator,
    worker: &mut ClientWorkerSession,
    txs: &[SharableTransactionRegion],
) {
    let Some(message_ptr) = worker.pack_to_worker.reserve() else {
        panic!("agave too far behind");
    };

    let Some(batch_ptr) =
        allocator.allocate((core::mem::size_of::<SharableTransactionRegion>() * txs.len()) as u32)
    else {
        panic!("failed to allocate");
    };
    unsafe { core::ptr::copy_nonoverlapping(txs.as_ptr(), batch_ptr.cast().as_ptr(), txs.len()) };

    unsafe {
        message_ptr.write(PackToWorkerMessage {
            flags: pack_message_flags::CHECK | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            max_working_slot: u64::MAX,
            batch: SharableTransactionBatchRegion {
                num_transactions: txs.len() as u8,
                transactions_offset: allocator.offset(batch_ptr),
            },
        })
    };
    worker.pack_to_worker.commit();
}
