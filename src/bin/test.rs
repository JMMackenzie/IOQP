use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "create", about = "create ioqp indexes")]
struct Args {
    /// Path to ciff input file
    #[structopt(short, long, parse(from_os_str))]
    input: std::path::PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();

    let reader = ioqp::new_ciff::Reader::from_file(args.input)?;

    println!("header = {:?}", &reader.header);

    let msgs = reader.header.num_postings_lists + reader.header.num_docs;
    let pb = indicatif::ProgressBar::new(msgs as u64);
    pb.set_draw_delta(msgs as u64 / 200);
    pb.set_style(indicatif::ProgressStyle::default_bar().template(
        &format!("{}: {}","read ciff","{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta}, SPEED: {per_sec})")));
    for msg in reader {
        match msg {
            ioqp::new_ciff::CiffMessage::Header(_) => println!("got header"),
            ioqp::new_ciff::CiffMessage::PostingsList(_) => pb.inc(1),
            ioqp::new_ciff::CiffMessage::DocRecord(_) => pb.inc(1),
        }
    }

    Ok(())
}
