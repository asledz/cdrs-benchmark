#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate cdrs;
extern crate cdrs_helpers_derive;
extern crate maplit;
extern crate time;
extern crate uuid;
use std::sync::Arc;
use std::thread;
use tokio::sync::Semaphore;

use anyhow::Result;
use getopts::Options;
use std::env;

use cdrs::{
    authenticators::NoneAuthenticator,
    cluster::{
        session::{new as new_session, Session},
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

    let node = matches
        .opt_str("node")
        .unwrap_or_else(|| "127.0.0.1:9042".to_owned());

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

    let mut session = connect_to_db();

    if matches.opt_present("prepare") {
        setup_schema(session, replication_factor);
    } else {
        println!("Start benchmark");
        run_bench(session, concurrency, tasks, workload);
    }

    Ok(())
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

fn setup_schema(session: CurrentSession, replication_factor: u32) -> Result<()> {
    let create_keyspace_text = format!("CREATE KEYSPACE IF NOT EXISTS ks_rust_scylla_bench WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {}}}", replication_factor);
    session.query(create_keyspace_text).map(|_| (()));

    session
        .query("DROP TABLE IF EXISTS ks_rust_scylla_bench.t")
        .map(|_| (()));

    session
        .query("CREATE TABLE ks_rust_scylla_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)")
        .map(|_| (()));

    println!("Schema set up!");
    Ok(())
}

struct RowStruct {
    pk: u64,
    v1: u64,
    v2: u64,
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
) -> Result<()> {

    if workload == Workload::ReadsAndWrites {
        tasks /= 2;
    }

    let mut prev_percent = -1;

    let insert_struct_cql = "INSERT INTO ks_rust_scylla_bench.t (pk, v1, v2) VALUES (?, ?, ?)";
    let stmt_insert = Arc::new(
        session
            .prepare(insert_struct_cql)
            .expect("Prepare query error"),
    );
    let slect_cql = "SELECT pk, v1, v2 FROM ks_rust_scylla_bench.t WHERE pk = ?";
    let stmt_select = Arc::new(session.prepare(slect_cql).expect("Prepare querry error"));

    let mut i = 0;
    let batch_size = 256;

    let mut children = vec![];

    let session = Arc::new(session);

    for i in 0..concurrency {
        let my_session = session.clone();
        let stmt_insert = stmt_insert.clone();
        let stmt_select = stmt_select.clone();

        children.push(thread::spawn(move || {
            let mut j = i;
            while j <= tasks {
                println!("Thread: {} does task {}", i, j);
                if workload == Workload::Writes || workload == Workload::ReadsAndWrites {
                    let row = RowStruct {
                        pk: j,
                        v1: 2 * j,
                        v2: 3 * j,
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
