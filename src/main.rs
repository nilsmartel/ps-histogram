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

    println!("writing to file {outputfile}");

    let mut output = File::create(outputfile).expect("to create outputfile");

    let max = histo.len() as f64;

    for (i, value) in histo.into_iter().enumerate() {
        writeln!(&mut output, "{value}").expect("to write to outputfile");

        if i & 0x3ffff == 0 {
            let f = i as f64 / max;
            let f = f * 100.0;
            println!("{:03}%", f);
        }
    }

    handle.join().expect("join threads");
}

fn histogram(rows: Receiver<String>) -> Vec<u32> {
    println!("building histogram");

    let mut v = Vec::new();

    let mut i = 0;
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

        i += 1;
        if i & 0xffff == 0 {
            print!(".");
        }
    }

    println!("\nsorting");

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
        let query = format!("SELECT {field} FROM {table} ORDER BY {field}");
        let params: [String; 0] = [];
        let mut rows = client.query_raw(&query, params).expect("query rows");

        while let Some(row) = rows.next().unwrap() {
            let value: String = row.get(&field as &str);
            sender.send(value).expect("write to channel");
        }
    });

    (handle, receiver)
}
