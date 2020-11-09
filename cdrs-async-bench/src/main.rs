#[macro_use]
extern crate anyhow;

use anyhow::Result;
use getopts::Options;
use std::env;
use std::sync::Arc;

use async_std::sync::Mutex;
use cassandra_proto::{query::QueryValues, types::value::Value};
use cdrs_async::{
    authenticators::NoneAuthenticator, query::QueryExecutor, Compression, Session, TransportTcp,
};
use std::collections::HashMap;
use std::pin::Pin;
// TODO - find better semaphore for async_std because this one doesnt have acquire_owned() :(
use async_weighted_semaphore::Semaphore;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Workload {
    Reads,
    Writes,
    ReadsAndWrites,
}

fn main() {
    async_std::task::block_on(async {
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

        let node = matches
            .opt_str("node")
            .unwrap_or_else(|| "127.0.0.1:9042".to_owned());
        let concurrency = matches.opt_get_default("concurrency", 256)?;
        let tasks = matches.opt_get_default("tasks", 1_000_000u64)?;
        let replication_factor = matches.opt_get_default("replication-factor", 3)?;
        // TODO: consistency
        let compression = match matches.opt_str("compression").as_deref() {
            Some("lz4") => Compression::Lz4,
            Some("snappy") => Compression::Snappy,
            Some("none") => Compression::None,
            None => Compression::None,
            Some(c) => return Err(anyhow!("bad compression: {}", c)),
        };
        let workload = match matches.opt_str("workload").as_deref() {
            Some("write") => Workload::Writes,
            Some("read") => Workload::Reads,
            Some("mixed") => Workload::ReadsAndWrites,
            None => Workload::Writes,
            Some(c) => return Err(anyhow!("bad workload type: {}", c)),
        };

        let authenticator_strategy = NoneAuthenticator {};
        let session =
            Session::connect("127.0.0.1:9042", compression, authenticator_strategy.into())
                .await
                .expect("session connect failed!");

        if matches.opt_present("prepare") {
            setup_schema(session, replication_factor).await?;
        } else {
            run_bench(session, concurrency, tasks, workload).await?;
        }

        Ok(())
    });
}

async fn setup_schema(mut session: Session<TransportTcp>, replication_factor: u32) -> Result<()> {
    use std::time::Duration;

    let create_keyspace_text = format!("CREATE KEYSPACE IF NOT EXISTS ks_rust_scylla_bench WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", replication_factor);

    Pin::new(&mut session).query(create_keyspace_text).await?;
    async_std::task::sleep(Duration::from_secs(1)).await;

    Pin::new(&mut session)
        .query("DROP TABLE IF EXISTS ks_rust_scylla_bench.t")
        .await?;
    async_std::task::sleep(Duration::from_secs(1)).await;

    Pin::new(&mut session)
        .query("CREATE TABLE ks_rust_scylla_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)")
        .await?;
    async_std::task::sleep(Duration::from_secs(1)).await;

    println!("Schema set up!");
    Ok(())
}

async fn run_bench(
    session: Session<TransportTcp>,
    concurrency: u64,
    mut tasks: u64,
    workload: Workload,
) -> Result<()> {
    if workload == Workload::ReadsAndWrites {
        tasks /= 2;
    }

    let sem = Arc::new(Semaphore::new(concurrency as usize));
    let session = Arc::new(Mutex::new(session));

    let mut prev_percent = -1;

    /*
    let stmt_read = session
        .prepare("SELECT v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?")
        .await?;
    let stmt_write = session
        .prepare("INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)")
        .await?;
    */

    let mut i = 0;
    let batch_size = 256;

    while i < tasks {
        let curr_percent = (100 * i) / tasks;
        if prev_percent < curr_percent as i32 {
            prev_percent = curr_percent as i32;
            println!("Progress: {}%", curr_percent);
        }
        let session = session.clone();
        //let permit = sem.clone().acquire().await;

        //let stmt_read = stmt_read.clone();
        //let stmt_write = stmt_write.clone();
        async_std::task::spawn(async move {
            let begin = i;
            let end = std::cmp::min(begin + batch_size, tasks);

            for i in begin..end {
                if workload == Workload::Writes || workload == Workload::ReadsAndWrites {
                    let query_values = {
                        let mut values_map: HashMap<String, Value> = HashMap::new();
                        values_map.insert("pk".to_string(), Value::from(i));
                        values_map.insert("v1".to_string(), Value::from(2 * i));
                        values_map.insert("v2".to_string(), Value::from(3 * i));
                        QueryValues::NamedValues(values_map)
                    };

                    let locked_session: &mut Session<TransportTcp> = &mut *session.lock().await;
                    let insert_struct_cql =
                        "INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)";
                    let query_future =
                        Pin::new(locked_session).query_with_values(insert_struct_cql, query_values);

                    /* This is impossible because .await still uses the lock()
                    // That means no multiple streams possible??
                    let query_future = {
                        let locked_session: &mut Session<TransportTcp> = &mut *session.lock().await;
                        let insert_struct_cql = "INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)";
                        Pin::new(locked_session).query_with_values(insert_struct_cql, query_values)
                    };
                    */

                    let result = query_future.await;

                    if result.is_err() {
                        eprintln!("Error: {:?}", result.unwrap_err());
                        continue; // The row may not be available for reading, so skip
                    }
                }

                /*
                if workload == Workload::Reads || workload == Workload::ReadsAndWrites {
                    let result = session.execute(&stmt_read, &scylla::values!(i)).await;
                    match result {
                        Ok(result) => {
                            let row = result.unwrap().into_iter().next().unwrap();
                            assert_eq!(
                                row.columns,
                                vec![
                                    Some(CQLValue::BigInt(2 * i as i64)),
                                    Some(CQLValue::BigInt(3 * i as i64))
                                ]
                            )
                        }
                        Err(err) => {
                            eprintln!("Error: {:?}", err);
                        }
                    }
                }
                */
            }

            //let _permit = permit;
        });

        i += batch_size;
    }

    // Wait for all in-flight requests to finish
    for _ in 0..concurrency {
        //sem.acquire().await.forget();
    }

    println!("Done!");
    Ok(())
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}
