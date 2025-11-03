use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    WorkerToPackMessage,
    pack_message_flags::{self, check_flags},
    processed_codes,
    worker_message_types::{
        self, not_included_reasons, parsing_and_sanitization_flags, resolve_flags,
    },
};
use agave_scheduling_utils::{
    handshake::{
        ClientLogon,
        client::{ClientSession, ClientWorkerSession},
    },
    thread_aware_account_locks::{ThreadAwareAccountLocks, ThreadSet},
    transaction_ptr::{TransactionPtr, TransactionPtrBatch},
};
use agave_transaction_view::{
    transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
};
use rts_alloc::Allocator;
use shaq::Consumer;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use utils::*;

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
            progress_tracker_size: shaq::minimum_file_size::<ProgressMessage>(20 * 64),
            pack_to_worker_size: shaq::minimum_file_size::<PackToWorkerMessage>(64 * 1024),
            worker_to_pack_size: shaq::minimum_file_size::<WorkerToPackMessage>(64 * 1024),
        },
        Duration::from_secs(2),
    )
    .expect("failed to connect to agave");
    let allocator = &allocators[0];

    let mut queue = VecDeque::with_capacity(QUEUE_CAPACITY);
    let mut account_locks = ThreadAwareAccountLocks::new(workers.len());
    let mut in_progress = vec![0; workers.len()];
    let mut offset_to_entry = HashMap::with_capacity(QUEUE_CAPACITY);

    let mut is_leader = false;
    while !exit.load(Ordering::Relaxed) {
        handle_tpu_messages(
            allocator,
            &mut tpu_to_pack,
            &mut workers[NUM_WORKERS - 1],
            &mut queue,
            &mut offset_to_entry,
        );

        for (worker_index, worker) in workers.iter_mut().enumerate() {
            handle_worker_messages(
                allocator,
                worker_index,
                worker,
                &mut queue,
                &mut offset_to_entry,
                &mut account_locks,
                &mut in_progress,
            );
        }

        if let Some(new_is_leader) = handle_progress_message(&mut progress_tracker) {
            is_leader = new_is_leader;
        }

        if is_leader {
            schedule(
                allocator,
                &mut workers[..NUM_WORKERS - 1],
                &mut queue,
                &offset_to_entry,
                &mut account_locks,
                &mut in_progress,
            );
        }
    }
}

struct TransactionEntry {
    sharable_transaction: SharableTransactionRegion,
    view: SanitizedTransactionView<TransactionPtr>,
    loaded_addresses: Option<PubkeysPtr>,
}

impl TransactionEntry {
    pub fn writable_account_keys(&self) -> impl Iterator<Item = &Pubkey> + Clone {
        self.view
            .static_account_keys()
            .iter()
            .chain(
                self.loaded_addresses
                    .iter()
                    .flat_map(|loaded_addresses| loaded_addresses.as_slice().iter()),
            )
            .enumerate()
            .filter(|(index, _)| self.requested_write(*index as u8))
            .map(|(_index, key)| key)
    }

    pub fn readonly_account_keys(&self) -> impl Iterator<Item = &Pubkey> + Clone {
        self.view
            .static_account_keys()
            .iter()
            .chain(
                self.loaded_addresses
                    .iter()
                    .flat_map(|loaded_addresses| loaded_addresses.as_slice().iter()),
            )
            .enumerate()
            .filter(|(index, _)| !self.requested_write(*index as u8))
            .map(|(_index, key)| key)
    }

    #[inline(always)]
    fn requested_write(&self, index: u8) -> bool {
        if index >= self.view.num_static_account_keys() {
            let loaded_address_index = index.wrapping_sub(self.view.num_static_account_keys());
            loaded_address_index < self.view.total_writable_lookup_accounts() as u8
        } else {
            index
                < self
                    .view
                    .num_signatures()
                    .wrapping_sub(self.view.num_readonly_signed_static_accounts())
                || (index >= self.view.num_signatures()
                    && index
                        < (self.view.static_account_keys().len() as u8)
                            .wrapping_sub(self.view.num_readonly_unsigned_static_accounts()))
        }
    }
}

fn handle_tpu_messages(
    allocator: &Allocator,
    tpu_to_pack: &mut Consumer<TpuToPackMessage>,
    resolving_worker: &mut ClientWorkerSession,
    queue: &mut VecDeque<usize>,
    offset_to_entry: &mut HashMap<usize, TransactionEntry>,
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
        queue.push_back(message.transaction.offset);
        offset_to_entry.insert(
            message.transaction.offset,
            TransactionEntry {
                sharable_transaction: SharableTransactionRegion {
                    offset: message.transaction.offset,
                    length: message.transaction.length,
                },
                view,
                loaded_addresses: None,
            },
        );
    }

    if !txs_to_resolve.is_empty() {
        send_resolve_requests(allocator, resolving_worker, &txs_to_resolve);
        txs_to_resolve.clear();
    }

    tpu_to_pack.finalize();
}

fn handle_worker_messages(
    allocator: &Allocator,
    worker_index: usize,
    worker: &mut ClientWorkerSession,
    queue: &mut VecDeque<usize>,
    offset_to_entry: &mut HashMap<usize, TransactionEntry>,
    account_locks: &mut ThreadAwareAccountLocks,
    in_progress: &mut [u64],
) {
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
                if processed {
                    let execution_responses_ptr = unsafe {
                        ExecutionResponsesPtr::from_transaction_response_region(
                            &message.responses,
                            allocator,
                        )
                    };

                    // Unlock and push back into queue.
                    for (tx_ptr, response) in batch.iter().zip(execution_responses_ptr.iter()) {
                        let offset =
                            unsafe { tx_ptr.to_sharable_transaction_region(allocator).offset };
                        let entry = offset_to_entry.get(&offset).unwrap();
                        account_locks.unlock_accounts(
                            entry.writable_account_keys(),
                            entry.readonly_account_keys(),
                            worker_index,
                        );
                        in_progress[worker_index] -= 1;

                        // If rejected because of a cost-tracking error, we'll retry, but only if
                        // queue is also under capacity.
                        match response.not_included_reason {
                            not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT
                            | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT
                            | not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
                            | not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
                            | not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
                                if queue.len() < QUEUE_CAPACITY =>
                            {
                                queue.push_back(offset);
                            }
                            _ => {
                                let entry = offset_to_entry.remove(&offset).unwrap();
                                if let Some(loaded_addresses) = entry.loaded_addresses {
                                    unsafe { loaded_addresses.free(allocator) };
                                }
                                unsafe { tx_ptr.free(allocator) };
                            }
                        };
                    }

                    execution_responses_ptr.free_wrapper();
                } else {
                    // Unlock and push back into queue IF there is room.
                    for tx_ptr in batch.iter() {
                        let offset =
                            unsafe { tx_ptr.to_sharable_transaction_region(allocator).offset };
                        let entry = offset_to_entry.get(&offset).unwrap();
                        account_locks.unlock_accounts(
                            entry.writable_account_keys(),
                            entry.readonly_account_keys(),
                            worker_index,
                        );
                        in_progress[worker_index] -= 1;

                        if queue.len() >= QUEUE_CAPACITY {
                            let entry = offset_to_entry.remove(&offset).unwrap();
                            if let Some(loaded_addresses) = entry.loaded_addresses {
                                unsafe { loaded_addresses.free(allocator) };
                            }
                            unsafe { tx_ptr.free(allocator) };
                            continue;
                        }
                        queue.push_back(offset);
                    }
                }
            }
            worker_message_types::CHECK_RESPONSE => {
                assert!(processed);
                let responses = unsafe {
                    CheckResponsesPtr::from_transaction_response_region(
                        &message.responses,
                        allocator,
                    )
                };

                for (tx_ptr, response) in batch.iter().zip(responses.iter()) {
                    // FIFO-demo only requests resolving of pubkeys,
                    // just assert that other flags are not set.
                    assert_eq!(response.fee_payer_balance_flags, 0);
                    assert_eq!(response.status_check_flags, 0);

                    // If the transaction failed to parse/sanitize drop it here.
                    // If resolving failed, we drop.
                    if response.parsing_and_sanitization_flags
                        & parsing_and_sanitization_flags::FAILED
                        != 0
                        || response.resolve_flags & resolve_flags::FAILED != 0
                    {
                        unsafe { tx_ptr.free(allocator) };
                        continue;
                    }

                    // Ensure we had successful resolution.
                    assert_eq!(
                        response.resolve_flags,
                        resolve_flags::REQUESTED | resolve_flags::PERFORMED
                    );

                    let loaded_addresses = if response.resolved_pubkeys.num_pubkeys != 0 {
                        Some(unsafe {
                            PubkeysPtr::from_sharable_pubkeys(&response.resolved_pubkeys, allocator)
                        })
                    } else {
                        None
                    };

                    // If queue is full we drop it.
                    if queue.len() >= QUEUE_CAPACITY {
                        if let Some(addresses) = loaded_addresses {
                            unsafe { addresses.free(allocator) };
                        }
                        unsafe { tx_ptr.free(allocator) };
                        continue;
                    }

                    let offset = unsafe { tx_ptr.to_sharable_transaction_region(allocator) }.offset;
                    let entry = TransactionEntry {
                        sharable_transaction: unsafe {
                            tx_ptr.to_sharable_transaction_region(allocator)
                        },
                        view: SanitizedTransactionView::try_new_sanitized(tx_ptr, true)
                            .expect("message corrupted"),
                        loaded_addresses,
                    };

                    queue.push_back(offset);
                    offset_to_entry.insert(offset, entry);
                }

                responses.free_wrapper();
            }
            _ => {
                panic!("agave sent a message with tag we do not understand!");
            }
        }
    }

    worker.worker_to_pack.finalize();
}

fn handle_progress_message(progress_tracker: &mut Consumer<ProgressMessage>) -> Option<bool> {
    progress_tracker.sync();

    let mut new_is_leader = None;
    let message_count = progress_tracker.len();
    for _ in 0..(message_count - 1) {
        let _ = progress_tracker.try_read();
    }
    if let Some(most_recent_message) = progress_tracker.try_read() {
        let message = unsafe { most_recent_message.as_ref() };
        new_is_leader = Some(message.leader_state == agave_scheduler_bindings::IS_LEADER);
    }

    progress_tracker.finalize();
    new_is_leader
}

fn schedule(
    allocator: &Allocator,
    workers: &mut [ClientWorkerSession],
    queue: &mut VecDeque<usize>,
    offset_to_entry: &HashMap<usize, TransactionEntry>,
    account_locks: &mut ThreadAwareAccountLocks,
    in_progress: &mut [u64],
) {
    if queue.is_empty() {
        return;
    }

    workers
        .iter_mut()
        .for_each(|worker| worker.pack_to_worker.sync());

    let mut attempted = 0;
    let mut working_batches = vec![None; workers.len()];
    const MAX_ATTEMPTED: usize = 10_000;

    while attempted < MAX_ATTEMPTED {
        let Some(offset) = queue.pop_front() else {
            break;
        };
        let entry = offset_to_entry.get(&offset).unwrap();

        attempted += 1;

        let Ok(thread_index) = account_locks.try_lock_accounts(
            entry.writable_account_keys(),
            entry.readonly_account_keys(),
            ThreadSet::any(workers.len()),
            |thread_set| {
                thread_set
                    .contained_threads_iter()
                    .min_by(|a, b| in_progress[*a].cmp(&in_progress[*b]))
                    .unwrap()
            },
        ) else {
            queue.push_back(offset);
            continue;
        };

        let working_batch = &mut working_batches[thread_index];
        if working_batch.is_none() {
            let pack_to_worker = &mut workers[thread_index].pack_to_worker;
            let mut reserve_attempt = pack_to_worker.reserve();
            if reserve_attempt.is_none() {
                pack_to_worker.sync();
                pack_to_worker.commit();
                reserve_attempt = pack_to_worker.reserve();
            }
            let msg = reserve_attempt.map(|mut msg| {
                // allocate a max-sized batch, and we just push in there.
                let batch_region = allocator
                    .allocate(
                        (core::mem::size_of::<SharableTransactionRegion>()
                            * MAX_TRANSACTIONS_PER_MESSAGE) as u32,
                    )
                    .expect("failed to allocate");
                {
                    let msg = unsafe { msg.as_mut() };
                    msg.batch.num_transactions = 0;
                    msg.batch.transactions_offset = unsafe { allocator.offset(batch_region) };
                    msg.flags = pack_message_flags::EXECUTE;
                    msg.max_working_slot = u64::MAX;
                }
                msg
            });

            *working_batch = msg;
        }

        let Some(message) = working_batch.as_mut() else {
            account_locks.unlock_accounts(
                entry.writable_account_keys(),
                entry.readonly_account_keys(),
                thread_index,
            );
            queue.push_back(offset);
            continue;
        };
        let message = unsafe { message.as_mut() };

        unsafe {
            allocator
                .ptr_from_offset(message.batch.transactions_offset)
                .cast::<SharableTransactionRegion>()
                .add(usize::from(message.batch.num_transactions))
                .write(SharableTransactionRegion {
                    offset: entry.sharable_transaction.offset,
                    length: entry.sharable_transaction.length,
                })
        };
        message.batch.num_transactions += 1;
        in_progress[thread_index] += 1;

        if usize::from(message.batch.num_transactions) >= MAX_TRANSACTIONS_PER_MESSAGE {
            *working_batch = None;
            workers[thread_index].pack_to_worker.commit();
        };
    }

    workers
        .iter_mut()
        .for_each(|worker| worker.pack_to_worker.commit());
}

fn send_resolve_requests(
    allocator: &Allocator,
    worker: &mut ClientWorkerSession,
    txs: &[SharableTransactionRegion],
) {
    let Some(message_ptr) = worker.pack_to_worker.reserve() else {
        panic!("agave too far behind");
    };

    let Some(batch_ptr) = allocator.allocate(core::mem::size_of_val(txs) as u32) else {
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

mod utils {
    use std::ptr::NonNull;

    use agave_scheduler_bindings::{
        SharablePubkeys, TransactionResponseRegion,
        worker_message_types::{self, CheckResponse, ExecutionResponse},
    };
    use rts_alloc::Allocator;
    use solana_pubkey::Pubkey;

    pub struct PubkeysPtr {
        ptr: NonNull<Pubkey>,
        count: usize,
    }

    impl PubkeysPtr {
        pub unsafe fn from_sharable_pubkeys(
            sharable_pubkeys: &SharablePubkeys,
            allocator: &Allocator,
        ) -> Self {
            let ptr = allocator.ptr_from_offset(sharable_pubkeys.offset).cast();
            Self {
                ptr,
                count: sharable_pubkeys.num_pubkeys as usize,
            }
        }

        pub fn as_slice(&self) -> &[Pubkey] {
            unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
        }

        pub unsafe fn free(self, allocator: &Allocator) {
            unsafe { allocator.free(self.ptr.cast()) };
        }
    }

    pub struct ExecutionResponsesPtr<'a> {
        ptr: NonNull<ExecutionResponse>,
        count: usize,
        allocator: &'a Allocator,
    }

    impl<'a> ExecutionResponsesPtr<'a> {
        pub unsafe fn from_transaction_response_region(
            transaction_response_region: &TransactionResponseRegion,
            allocator: &'a Allocator,
        ) -> Self {
            debug_assert!(
                transaction_response_region.tag == worker_message_types::EXECUTION_RESPONSE
            );

            Self {
                ptr: allocator
                    .ptr_from_offset(transaction_response_region.transaction_responses_offset)
                    .cast(),
                count: transaction_response_region.num_transaction_responses as usize,
                allocator,
            }
        }

        pub fn iter(&self) -> impl Iterator<Item = &ExecutionResponse> {
            unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }.iter()
        }

        pub fn free_wrapper(self) {
            unsafe { self.allocator.free(self.ptr.cast()) }
        }
    }

    pub struct CheckResponsesPtr<'a> {
        ptr: NonNull<CheckResponse>,
        count: usize,
        allocator: &'a Allocator,
    }

    impl<'a> CheckResponsesPtr<'a> {
        pub unsafe fn from_transaction_response_region(
            transaction_response_region: &TransactionResponseRegion,
            allocator: &'a Allocator,
        ) -> Self {
            debug_assert!(transaction_response_region.tag == worker_message_types::CHECK_RESPONSE);

            Self {
                ptr: allocator
                    .ptr_from_offset(transaction_response_region.transaction_responses_offset)
                    .cast(),
                count: transaction_response_region.num_transaction_responses as usize,
                allocator,
            }
        }

        pub fn iter(&self) -> impl Iterator<Item = &CheckResponse> {
            unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }.iter()
        }

        pub fn free_wrapper(self) {
            unsafe { self.allocator.free(self.ptr.cast()) }
        }
    }
}
