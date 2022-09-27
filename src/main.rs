mod cli;
mod db;
use cli::Config;
use fallible_iterator::FallibleIterator;
use structopt::StructOpt;
use std::fs::File;
use std::sync::mpsc::*;
use std::thread::{spawn, JoinHandle};
use std::io::Write;

fn main() {
    let Config {table, field} = Config::from_args();
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

fn query_field(field: String, table: String, mut client: postgres::Client) -> (JoinHandle<()>, Receiver<String>) {
    let params: Vec<String> = vec![
        field.to_string(), table
    ];

    let (sender, receiver) = channel();

    let handle = spawn(move || {
        let mut rows = client.query_raw(
            "SELECT $0 FROM $1 SORT BY $0",
            params,
        ).expect("query rows");

        while let Some(row) = rows.next().unwrap() {
            let value: String = row.get(&field as &str );
            sender.send(value).expect("write to channel");
        }
    });

    (handle, receiver)
}
