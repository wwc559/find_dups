use async_std::task;
use clap::{app_from_crate, arg};

use find_dupes::{launch_brokers, Config};

fn main() {
    let matches = app_from_crate!()
        .arg(
            arg!(-i --injest <path> ... "Path to injest")
                .required(false)
                .default_value("."),
        )
        .arg(
            arg!(-a --archive <path> "Path to archive")
                .required(false)
                .default_value("/tmp/finddupes"),
        )
        .arg(arg!(-c --check <path> "Path to check").required(false))
        .get_matches();

    let injests: Vec<&str> = matches.values_of("injest").unwrap().collect();

    // Get the configuration
    let (config, dir_receiver) = Config::new(&matches);

    // Now start the loops
    task::block_on(async {
        launch_brokers(config.clone(), dir_receiver, injests.clone())
            .await
            .unwrap();
    });
    // All done!
}
