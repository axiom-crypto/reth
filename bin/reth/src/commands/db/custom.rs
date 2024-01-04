use std::{fs::File, ptr, slice};

use alloy_rlp::Encodable;
use itertools::Itertools;
use rayon::prelude::*;
use reth_db::{
    database::Database, table::Decompress, transaction::DbTx, AccountsTrie, DatabaseEnv, RawTable,
    Transactions,
};
use reth_mdbx_sys::*;
use reth_primitives::{TransactionSignedNoHash, TxNumber};

use crate::utils::{DbTool, ListFilter};

const MAX_RETRIES: usize = 10;

pub fn get_tx_data<'a>(tool: DbTool<'a, DatabaseEnv>) -> eyre::Result<()> {
    tool.db.view(|tx| {
        let table_db = tx.inner.open_db(Some("Transactions")).unwrap();
        let stats = tx.inner.db_stat(&table_db).unwrap();
        let total_entries = stats.entries();
        println!("Total tx entries: {}", total_entries);
        let page_size = stats.page_size() as usize;
        println!("Page size: {}", page_size);

        let mut txn: *mut MDBX_txn = tx.inner.inner.txn.txn;
        let cursor = tx.cursor_read::<Transactions>().unwrap();
        let mut cursor: *mut MDBX_cursor = cursor.inner.cursor;

        let mut current_page = Vec::with_capacity(page_size);

        // Start from the last entry in the database
        let mut key = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        let mut data = MDBX_val { iov_len: 0, iov_base: ptr::null_mut() };

        unsafe {
            if mdbx_cursor_get(cursor, &mut key, &mut data, MDBX_LAST) == MDBX_SUCCESS {
                loop {
                    // Process the data
                    current_page.push((key, data));

                    if current_page.len() == page_size {
                        process_page(&current_page);
                        current_page.clear();
                    }

                    // Move cursor to the previous item
                    if mdbx_cursor_get(cursor, &mut key, &mut data, MDBX_PREV) != MDBX_SUCCESS {
                        break
                    }
                }
            }
        }

        unsafe fn process_page(page: &Vec<(MDBX_val, MDBX_val)>) {
            if page.is_empty() {
                return
            }
            let res: Vec<_> = page
                .into_iter()
                .map(|(key, data)| {
                    let key =
                        slice::from_raw_parts(key.iov_base as *const u8, key.iov_len as usize);
                    let value =
                        slice::from_raw_parts(data.iov_base as *const u8, data.iov_len as usize);
                    (key, value)
                })
                .collect();
            let res: Vec<_> = res
                .into_par_iter()
                .map(|(key, value)| {
                    let db_index = TxNumber::decompress(key).unwrap();

                    let tx = TransactionSignedNoHash::decompress(value).unwrap();
                    let len = if let Some(access_list) = tx.transaction.access_list() {
                        let mut buf = vec![];
                        access_list.encode(&mut buf);
                        buf.len()
                    } else {
                        0
                    };
                    (db_index, tx.tx_type(), tx.hash(), len)
                })
                .collect();
            let begin = res.last().unwrap().0;
            println!("Db tx numbers: {}-{}, Page size: {}", begin, res[0].0, page.len());
            let f = File::create(format!("data/tx_access_list_lens.rev.{begin}.csv")).unwrap();
            let mut wtr = csv::Writer::from_writer(f);
            for (_, tx_type, hash, len) in &res {
                wtr.write_record(&[
                    (*tx_type as isize).to_string(),
                    hash.to_string(),
                    len.to_string(),
                ])
                .unwrap();
            }
        }
    })?;
    Ok(())
}

pub fn get_state_trie_depth<'a>(tool: DbTool<'a, DatabaseEnv>) -> eyre::Result<()> {
    tool.db.view(|tx| {
        let table_db = tx.inner.open_db(Some("AccountsTrie")).unwrap();
        let stats = tx.inner.db_stat(&table_db).unwrap();
        let total_entries = stats.entries();
        println!("Total AccountsTrie entries: {}", total_entries);
        let page_size = stats.page_size() as usize;
        println!("Page size: {}", page_size);
        let mut start = 0;
        let mut global_max_depth = 0;
        while start < total_entries {
            let filter = ListFilter {
                skip: start,
                len: page_size,
                search: vec![],
                min_row_size: 0,
                min_key_size: 0,
                min_value_size: 0,
                reverse: false,
                only_count: false,
            };
            let max_depth_in_page = retry_until_success(|| {
                let mut max_depth = 0;
                let (entries, _hits) = tool.list::<AccountsTrie>(&filter)?;
                for (stored_nibbles, _stored_branch_node) in entries {
                    let mut mask_bits = 0;
                    for i in 0..16 {
                        if _stored_branch_node.0.state_mask.is_bit_set(i) {
                            mask_bits += 1;
                        }
                    }
                    if mask_bits <= 1 {
                        println!(
                            "Stored nibbles: {:?}, branch node: {:?}",
                            stored_nibbles, _stored_branch_node
                        );
                    }
                    let num_nibbles = stored_nibbles.0.len();
                    max_depth = max_depth.max(num_nibbles);
                }
                Ok(max_depth)
            });
            println!("Max depth in page: {}", max_depth_in_page);
            global_max_depth = global_max_depth.max(max_depth_in_page);
            start += page_size;
        }
        println!("Global AccountTrie max depth: {}", global_max_depth);
    })?;
    Ok(())
}

fn retry_until_success<F, T>(f: F) -> T
where
    F: Fn() -> eyre::Result<T>,
{
    let mut retries = 0;
    loop {
        match f() {
            Ok(t) => return t,
            Err(e) => {
                if retries >= MAX_RETRIES {
                    panic!("Max retries exceeded: {}", e);
                }
                retries += 1;
                println!("Retrying: {}", e);
            }
        }
    }
}
