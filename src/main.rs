mod cli;
mod db;
use cli::Config;
use fallible_iterator::FallibleIterator;
use std::fs::File;
use std::io::Write;
use std::sync::mpsc::*;
use std::thread::{spawn, JoinHandle};
use structopt::StructOpt;

fn main() {
    let Config { table, field } = Config::from_args();
    let outputfile = format!("histogram--{table}--{field}.csv");
    let client = db::client();

    let (handle, rows) = query_field(field, table, client);

    let histo = histogram(rows);

    let mut output = File::create(outputfile).expect("to create outputfile");

    for value in histo {
        writeln!(&mut output, "{value}").expect("to write to outputfile");
    }

    handle.join().expect("join threads");
}

fn histogram(rows: Receiver<String>) -> Vec<u32> {
    let mut v = Vec::new();

    let mut count = 0;
    let mut last_value = String::new();

    for value in rows {
        if last_value == value {
            count += 1;
            continue;
        }

        v.push(count);
        last_value = value;
        count = 1;
    }

    v.sort_unstable();

    v
}

fn query_field(
    field: String,
    table: String,
    mut client: postgres::Client,
) -> (JoinHandle<()>, Receiver<String>) {
    let (sender, receiver) = channel();

    let handle = spawn(move || {
        let query = format!("SELECT {field} FROM {table} SORT BY {field}");
        let params: [String; 0] = [];
        let mut rows = client.query_raw(&query, params).expect("query rows");

        while let Some(row) = rows.next().unwrap() {
            let value: String = row.get(&field as &str);
            sender.send(value).expect("write to channel");
        }
    });

    (handle, receiver)
}
