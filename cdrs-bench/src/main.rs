#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;
extern crate uuid;
extern crate time;

use anyhow::Result;
use getopts::Options;
// use scylla::frame::response::result::CQLValue;
// use scylla::transport::session::Session;
// use scylla::transport::Compression;
use std::env;
use std::sync::Arc;
use tokio::sync::Semaphore;


use cdrs::{
  authenticators::NoneAuthenticator,
  cluster::{
    session::{
      new as new_session,
      Session,
    },
    ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool,
  },
  load_balancing::SingleNode,
  query::*,
  Result as CDRSResult,
};

pub type CurrentSession = Session<SingleNode<TcpConnectionPool<NoneAuthenticator>>>;

pub fn create_db_session() -> CDRSResult<CurrentSession> {
  let auth = NoneAuthenticator;
  let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth).build();
  let cluster_config = ClusterTcpConfig(vec![node]);
  
  new_session(&cluster_config, SingleNode::new())
}

fn connect_to_db() -> CurrentSession {
    let mut session = create_db_session().expect("create db session error");
    session
}


#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Workload {
    Reads,
    Writes,
    ReadsAndWrites,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("", "node", "cluster contact node", "ADDRESS");
    opts.optopt("", "concurrency", "workload concurrency", "COUNT");
    opts.optopt("", "tasks", "task count", "COUNT");
    opts.optopt("", "replication-factor", "replication factor", "FACTOR");
    opts.optopt("", "consistency", "consistency level", "CONSISTENCY");
    opts.optopt(
        "",
        "compression",
        "compression algorithm to use (none, lz4 or snappy)",
        "ALGORITHM",
    );

    opts.optopt(
        "",
        "workload",
        "workload type (write, read or mixed)",
        "TYPE",
    );

    opts.optflag("", "prepare", "prepare keyspace and table before the bench");
    opts.optflag("", "run", "run the benchmark");

    opts.optflag("h", "help", "print this help menu");
    let matches = opts.parse(&args[..]).unwrap();

    if matches.opt_present("prepare") == matches.opt_present("run") {
        return Err(anyhow!(
            "please specify exactly one of the following flags: --prepare, --run"
        ));
    }

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return Ok(());
    }

    let node = matches.opt_str("node").unwrap_or_else(|| "127.0.0.1:9042".to_owned());
    

    let concurrency = matches.opt_get_default("concurrency", 256)?;
    let tasks = matches.opt_get_default("tasks", 1_000_000u64)?;
    let replication_factor = matches.opt_get_default("replication-factor", 3)?;

    
    let mut session = connect_to_db();

    if matches.opt_present("prepare") {
        setup_schema(session, replication_factor); }
    // } else {
    //     run_bench(session, concurrency, tasks, workload).await?;
    // }

    Ok(())
}


fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}


fn setup_schema(session: CurrentSession, replication_factor: u32) -> Result<()> {
    use std::time::Duration;

    let create_keyspace_text = format!("CREATE KEYSPACE IF NOT EXISTS ks_rust_scylla_bench WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", replication_factor);
    session.query(create_keyspace_text).map(|_| (()));
    // tokio::time::sleep(Duration::from_secs(1)).await;

    session.query("DROP TABLE IF EXISTS ks_rust_scylla_bench.t").map(|_| (()));
    
    session
        .query(
            "CREATE TABLE ks_rust_scylla_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)").map(|_| (()));

    // tokio::time::sleep(Duration::from_secs(1)).await;

    println!("Schema set up!");
    Ok(())
}



async fn run_bench(
    session: CurrentSession,
    concurrency: u64,
    mut tasks: u64,
    workload: Workload,
) -> Result<()> {
    if workload == Workload::ReadsAndWrites {
        tasks /= 2;
    }

    let sem = Arc::new(Semaphore::new(concurrency as usize));
    /*let session = Arc::new(session);*/

    let mut prev_percent = -1;

    // let stmt_read = session
    //     .prepare("SELECT v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?")
    //     .await?;
    // let stmt_write = session
    //     .prepare("INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)")
    //     .await?;

    let mut i = 0;
    let batch_size = 256;

    while i < tasks {
        let curr_percent = (100 * i) / tasks;
        if prev_percent < curr_percent as i32 {
            prev_percent = curr_percent as i32;
            println!("Progress: {}%", curr_percent);
        }
        // let session = session.clone();
        let permit = sem.clone().acquire_owned().await;


        // let stmt_read = stmt_read.clone();
        // let stmt_write = stmt_write.clone();
        tokio::task::spawn(async move {
            let begin = i;
            let end = std::cmp::min(begin + batch_size, tasks);

            for i in begin..end {
                if workload == Workload::Writes || workload == Workload::ReadsAndWrites {

                    
                        let insert_struct_cql = "INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)";

                        session
                            .query_with_values(insert_struct_cql, row.into_query_values())
                            .expect("insert");
                //     // let result = session
                //     //     .execute(&stmt_write, &scylla::values!(i, 2 * i, 3 * i))
                //     //     .await;
                //     // if result.is_err() {
                //     //     eprintln!("Error: {:?}", result.unwrap_err());
                //     //     continue; // The row may not be available for reading, so skip
                //     // }
                }

                // if workload == Workload::Reads || workload == Workload::ReadsAndWrites {
                //     let result = session.execute(&stmt_read, &scylla::values!(i)).await;
                //     match result {
                //         Ok(result) => {
                //             let row = result.unwrap().into_iter().next().unwrap();
                //             assert_eq!(
                //                 row.columns,
                //                 vec![
                //                     Some(CQLValue::BigInt(2 * i as i64)),
                //                     Some(CQLValue::BigInt(3 * i as i64))
                //                 ]
                //             )
                //         }
                //         Err(err) => {
                //             eprintln!("Error: {:?}", err);
                //         }
                //     }
                // }
            }

            let _permit = permit;
        });

        // i += batch_size;
    }

    // Wait for all in-flight requests to finish
    for _ in 0..concurrency {
        sem.acquire().await.forget();
    }

    println!("Done!");
    Ok(())
}