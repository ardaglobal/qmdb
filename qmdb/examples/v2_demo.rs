use parking_lot::RwLock;
use qmdb::config::Config;
use qmdb::def::{DEFAULT_ENTRY_SIZE, IN_BLOCK_IDX_BITS, OP_CREATE};
use qmdb::entryfile::EntryBz;
use qmdb::tasks::TasksManager;
use qmdb::test_helper::SimpleTask;
use qmdb::utils::byte0_to_shard_id;
use qmdb::utils::changeset::ChangeSet;
use qmdb::utils::hasher;
use qmdb::{AdsCore, AdsWrap, ADS};
use std::sync::Arc;

#[cfg(all(not(target_env = "msvc"), feature = "tikv-jemallocator"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), feature = "tikv-jemallocator"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let ads_dir = "ADS";
    let config = Config::from_dir(ads_dir);
    
    println!("Initializing new QMDB instance at {}", ads_dir);
    // Always initialize fresh for this demo to avoid recovery complexity
    let _ = std::fs::remove_dir_all(ads_dir);
    AdsCore::init_dir(&config);
    
    let mut ads = AdsWrap::new(&config);

    println!("QMDB v2 Demo - Data Persistence Within Session");
    println!("==============================================");

    // Process multiple blocks to demonstrate persistence
    for block_num in 1..=3 {
        let current_height = ads.get_metadb().read().get_curr_height();
        let next_height = current_height + 1;
        
        println!("\n--- Processing Block {} ---", next_height);
        
        // Show existing data from previous blocks
        if block_num > 1 {
            println!("Reading data from previous block(s):");
            let mut buf = [0; DEFAULT_ENTRY_SIZE];
            let mut k = [0u8; 32];
            k[0] = ((next_height - 1) & 0xFF) as u8;
            k[1] = (((next_height - 1) >> 8) & 0xFF) as u8;
            k[2] = 0;  // i=0
            k[3] = 0;  // j=0
            k[4] = 2;  // n=2
            let kh = hasher::hash(&k[..]);
            let shared_ads = ads.get_shared();
            let (n, ok) = shared_ads.read_entry(-1, &kh[..], &[], &mut buf);
            if ok {
                let e = EntryBz { bz: &buf[..n] };
                println!("  Found entry from block {}: key={:?} value={:?}", 
                         next_height - 1, &k[..5], &e.value()[..3]);
            }
        }

        // for each block, the Create/Update/Delete operation must be organized into a ordered task list
        let mut task_list = Vec::with_capacity(5);
        for i in 0..5 {  // Fewer tasks per block for cleaner output
            let mut cset_list = Vec::with_capacity(1);
            let mut cset = ChangeSet::new();
            let mut k = [0u8; 32];
            let mut v = [1u8; 32];
            
            // Use block height to make keys unique across blocks
            k[0] = (next_height & 0xFF) as u8;
            k[1] = ((next_height >> 8) & 0xFF) as u8;
            k[2] = i as u8;
            k[3] = 0;
            
            for n in 0..3 {  // Fewer entries per task
                k[4] = n as u8;
                v[0] = n as u8;
                v[1] = (next_height & 0xFF) as u8;  // Include height in value
                v[2] = block_num as u8;  // Include block number
                let kh = hasher::hash(&k[..]);
                let shard_id = byte0_to_shard_id(kh[0]) as u8;
                // add a Create operation into the changeset
                cset.add_op(OP_CREATE, shard_id, &kh, &k[..], &v[..], None);
            }
            // the operations in changeset must be ordered too
            cset.sort();
            cset_list.push(cset);
            let task = SimpleTask::new(cset_list);
            task_list.push(RwLock::new(Some(task)));
        }

        let height = next_height;
        let task_count = task_list.len() as i64;
        //task id's high 40 bits is block height and low 24 bits is task index
        let last_task_id = (height << IN_BLOCK_IDX_BITS) | (task_count - 1);
        //add the tasks into QMDB
        ads.start_block(height, Arc::new(TasksManager::new(task_list, last_task_id)));
        //multiple shared_ads can be shared by different threads
        let shared_ads = ads.get_shared();
        //you can associate some extra data in json format to each block
        shared_ads.insert_extra_data(height, format!("Block {} data", block_num));
        for idx in 0..task_count {
            let task_id = (height << IN_BLOCK_IDX_BITS) | idx;
            //pump tasks into QMDB's pipeline
            shared_ads.add_task(task_id);
        }

        //flush QMDB's pipeline to make sure the Create operations are done
        ads.flush();

        println!("Block {} complete. Created {} entries.", height, task_count * 3);

        // Show some newly created data
        let mut buf = [0; DEFAULT_ENTRY_SIZE];
        let mut k = [0u8; 32];
        k[0] = (height & 0xFF) as u8;
        k[1] = ((height >> 8) & 0xFF) as u8;
        k[2] = 1;  // i=1
        k[3] = 0;  // j=0
        k[4] = 1;  // n=1
        let kh = hasher::hash(&k[..]);
        let shared_ads = ads.get_shared();
        let (n, ok) = shared_ads.read_entry(-1, &kh[..], &[], &mut buf);
        if ok {
            let e = EntryBz { bz: &buf[..n] };
            println!("  Sample new entry: key={:?} value={:?}", &k[..5], &e.value()[..3]);
        }
    }
    
    println!("\n==============================================");
    println!("QMDB demo complete! Processed 3 blocks with persistent data.");
    println!("Data is stored in '{}' directory using the Twig Merkle Tree.", ads_dir);
    println!("Each block's data remains accessible as new blocks are added.");
}
