#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
extern crate maplit;
extern crate time;
extern crate uuid;
use crate::cdrs::types::from_cdrs::FromCDRSByName;
use anyhow::Result;
use cdrs::types::prelude::TryFromRow;
use getopts::Options;
use std::env;
use std::sync::Arc;
use std::thread;

use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::{
        session::{new as new_session, Session},
        ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool,
    },
    load_balancing::RoundRobin,
    query::*,
    Result as CDRSResult,
};

pub type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

pub fn create_db_session(nodes: Vec<String>) -> CDRSResult<CurrentSession> {
    let node_configs = nodes
        .iter()
        .map(|addr| {
            NodeTcpConfigBuilder::new(addr, NoneAuthenticator)
                .max_size(100)
                .build()
        })
        .collect();

    let cluster_config = ClusterTcpConfig(node_configs);
    new_session(&cluster_config, RoundRobin::new())
}

fn connect_to_db(nodes: Vec<String>) -> CurrentSession {
    create_db_session(nodes).expect("create db session error")
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Workload {
    Reads,
    Writes,
    ReadsAndWrites,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("", "nodes", "comma-separated addresses of nodes to use", "ADDRESSES");
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

    let nodes = matches
        .opt_str("nodes")
        .unwrap_or_else(|| "127.0.0.1:9042".to_owned())
        .split(',')
        .map(str::to_string)
        .collect::<Vec<String>>();

    let node_count = nodes.len();

    let concurrency = matches.opt_get_default("concurrency", 256)?;
    let tasks = matches.opt_get_default("tasks", 1_000_000u64)?;
    let replication_factor = matches.opt_get_default("replication-factor", 3)?;

    let workload = match matches.opt_str("workload").as_deref() {
        Some("write") => Workload::Writes,
        Some("read") => Workload::Reads,
        Some("mixed") => Workload::ReadsAndWrites,
        None => Workload::Writes,
        Some(c) => return Err(anyhow!("bad workload type: {}", c)),
    };

    let session = connect_to_db(nodes);

    if matches.opt_present("prepare") {
        setup_schema(session, replication_factor)?;
    } else {
        println!("Start benchmark");
        run_bench(session, concurrency, tasks, workload, node_count)?;
    }

    Ok(())
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

fn setup_schema(session: CurrentSession, replication_factor: u32) -> Result<()> {
    let create_keyspace_text = format!("CREATE KEYSPACE IF NOT EXISTS ks_rust_scylla_bench WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", replication_factor);
    session.query(create_keyspace_text)?;

    session.query("DROP TABLE IF EXISTS ks_rust_scylla_bench.t")?;

    session.query(
        "CREATE TABLE ks_rust_scylla_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
    )?;

    println!("Schema set up!");
    Ok(())
}

#[derive(Debug, TryFromRow)]
struct RowStruct {
    pk: i64,
    v1: i64,
    v2: i64,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("pk" => self.pk, "v1" => self.v1, "v2" => self.v2)
    }
}

fn run_bench(
    session: CurrentSession,
    concurrency: u64,
    mut tasks: u64,
    workload: Workload,
    node_count: usize,
) -> Result<()> {
    if workload == Workload::ReadsAndWrites {
        tasks /= 2;
    }

    let insert_struct_cql = "INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)";
    for _ in 0..node_count {
        // Workaround: cdrs does not prepare statements on all nodes, so we need to do it manally
        // Each time, round-robin policy will assign a different node
        session
            .prepare(insert_struct_cql)
            .expect("Prepare querry error");
    }
    let stmt_insert = Arc::new(
        session
            .prepare(insert_struct_cql)
            .expect("Prepare query error"),
    );
    let slect_cql = "SELECT pk, v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?";
    for _ in 0..node_count {
        session.prepare(slect_cql).expect("Prepare querry error");
    }
    let stmt_select = Arc::new(session.prepare(slect_cql).expect("Prepare querry error"));

    let _batch_size = 256;

    let mut children = vec![];

    let session = Arc::new(session);

    for i in 0..concurrency {
        let my_session = session.clone();
        let stmt_insert = stmt_insert.clone();
        let stmt_select = stmt_select.clone();

        children.push(thread::spawn(move || {
            let mut j = i;
            while j <= tasks {
                if workload == Workload::Writes || workload == Workload::ReadsAndWrites {
                    let row = RowStruct {
                        pk: j as i64,
                        v1: 2 * j as i64,
                        v2: 3 * j as i64,
                    };

                    my_session
                        .exec_with_values(&stmt_insert, row.into_query_values())
                        .expect("exec_with_values error");
                }

                if workload == Workload::Reads || workload == Workload::ReadsAndWrites {
                    let rows = my_session
                        .exec_with_values(&stmt_select, query_values!(j))
                        .expect("exec_with_values error")
                        .get_body()
                        .expect("get body")
                        .into_rows()
                        .expect("into_rows");

                    for row in rows {
                        let my_row: RowStruct =
                            RowStruct::try_from_row(row).expect("into RowStruct");

                        assert_eq!(my_row.pk, j as i64);
                        assert_eq!(my_row.v1, 2 * j as i64);
                        assert_eq!(my_row.v2, 3 * j as i64);
                    }
                }
                j += concurrency;
            }
        }));
    }

    for child in children {
        let _ = child.join();
    }
    println!("Done!");
    Ok(())
}
