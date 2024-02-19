use async_std::task;
use clap::{app_from_crate, arg};

use find_dups::{launch_brokers, Config};

fn main() {
    let matches = app_from_crate!()
        .arg(
            arg!(-i --injest <path> ... "Path to injest")
                .required(false)
                .conflicts_with("check"),
        )
        .arg(
            arg!(-c --check <path> ... "Path to check")
                .required(false)
                .conflicts_with("injest"),
        )
        .arg(
            arg!(-m --missing "Report check/injest files which are missing from archive [default with --check]")
                .required(false)
                .conflicts_with("list")
                .conflicts_with("duplicate")
                .conflicts_with("present"),
        )
        .arg(
            arg!(-p --present "Report check/injest files which are present in archive")
                .required(false)
                .conflicts_with("list")
                .conflicts_with("duplicate")
                .conflicts_with("missing"),
        )
        .arg(
            arg!(-d --duplicate "Report archive files which match check or archive duplicates if not check")
                .required(false)
                .conflicts_with("list")
                .conflicts_with("missing")
                .conflicts_with("present"),
        )
        .arg(
            arg!(-l --list "List contents of archive after injest (if any)")
                .required(false)
                .conflicts_with("missing")
                .conflicts_with("present")
                .conflicts_with("duplicate"),
        )
        .arg(
            arg!(--prune "Prune non-injested files from archive")
                .required(false)
                .requires("injest")
        )
        .arg(
            arg!(-a --archive <path> "Path to archive")
                .required(false)
                .default_value("/tmp/finddups"),
        )
        .arg(arg!(-r --report "Produce a report summarizing duplicate files").required(false))
        .arg(arg!(-v --verbose ... "increase verbosity level").required(false))
        .arg(
            arg!(-t --timeout <sec> "Timeout after a certain time of no activity")
                .required(false)
                .default_value("600"),
        )
        .arg(
            arg!(--concurrency "Number of simultaneous directories to process")
                .required(false)
                .default_value("10"),
        )
        .get_matches();

    let paths = if matches.occurrences_of("check") > 0 {
        matches.values_of("check").unwrap().collect()
    } else if matches.occurrences_of("injest") > 0 {
        matches.values_of("injest").unwrap().collect()
    } else {
        Vec::new()
    };

    // Get the configuration
    let (config, dir_receiver) = Config::new(&matches);

    // Now start the loops
    task::block_on(async {
        launch_brokers(config.clone(), dir_receiver, paths.clone())
            .await
            .unwrap();
    });
    // All done!
}
