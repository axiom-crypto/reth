use reth_db::{database::Database, AccountsTrie, DatabaseEnv, Transactions};

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
        let filter = ListFilter {
            skip: 0,
            len: page_size,
            search: vec![],
            min_row_size: 0,
            min_key_size: 0,
            min_value_size: 0,
            reverse: false,
            only_count: false,
        };
        let (entries, hits) = tool.list::<Transactions>(&filter).unwrap();
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
