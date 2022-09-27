use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "postgres cardinality",
    about = "Measure cardinality of fields in postgres"
)]
pub struct Config {
    #[structopt(short, long)]
    pub table: String,

    #[structopt(short, long)]
    pub field: String,
}
