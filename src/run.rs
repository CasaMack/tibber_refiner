use std::{env, rc, sync::Arc};

use chrono::Utc;
use influxdb::Client;
use tokio::time;
use tracing::{instrument, metadata::LevelFilter, Level};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{
    fmt::format::{DefaultFields, FmtSpan, Format},
    FmtSubscriber,
};

const DEFAULT_RETRIES: u32 = 10;
const DEFAULT_UPDATE_TIME: &str = "0";

use super::refiner::refine;

#[instrument]
pub fn get_db_info() -> (Arc<String>, Arc<String>) {
    let db_addr = env::var("INFLUXDB_ADDR").expect("INFLUXDB_ADDR not set");
    tracing::info!("INFLUXDB_ADDR: {}", db_addr);

    let db_name = env::var("INFLUXDB_DB_NAME").expect("INFLUXDB_DB_NAME not set");
    tracing::info!("INFLUXDB_DB_NAME: {}", db_name);

    (Arc::new(db_addr), Arc::new(db_name))
}

pub fn get_retries() -> u32 {
    let retries = env::var("RETRIES")
        .ok()
        .unwrap_or(DEFAULT_RETRIES.to_string());
    tracing::info!("RETRIES: {}", retries);

    retries.parse().unwrap_or_else(|e| {
        tracing::warn!(
            "Failed to parse {}, using default: {}",
            retries,
            DEFAULT_RETRIES
        );
        tracing::debug!("{}", e);
        10
    })
}

pub fn get_logger() -> (
    FmtSubscriber<DefaultFields, Format, LevelFilter, NonBlocking>,
    WorkerGuard,
) {
    let appender = tracing_appender::rolling::daily("./var/log", "tibber-status-server");
    let (non_blocking_appender, guard) = tracing_appender::non_blocking(appender);

    let level = match env::var("LOG_LEVEL") {
        Ok(l) => match l.as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        },
        Err(_) => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_span_events(FmtSpan::NONE)
        .with_ansi(false)
        .with_max_level(level)
        .with_writer(non_blocking_appender)
        // completes the builder.
        .finish();

    (subscriber, guard)
}

#[instrument(skip_all, level = "trace")]
pub async fn tick(db_addr: Arc<String>, db_name: Arc<String>) -> Result<(), String> {
    tracing::debug!("tick");
    let date = chrono::offset::Local::now().date().and_hms(0, 0, 0).to_rfc3339();
    let t_pos = date.find('T').unwrap();
    let date = &date[..t_pos];
    tracing::info!("Writing price info for {}", date);
    let client = Client::new(db_addr.as_str(), db_name.as_str());

    let mut handles = Vec::new();
    let client_ref = rc::Rc::new(client);
    for hour in 0..24 {
        let clone = client_ref.clone();
        handles.push(async move {
            refine(hour, clone.as_ref()).await.map_err(|e| {
                tracing::error!("Error in refining {}: {}", hour, e);
                e
            })
        });
    }
    tokio::join!(futures::future::join_all(handles));

    Ok(())
}

pub fn get_instant() -> time::Instant {
    let time = env::var("UPDATE_TIME")
        .ok()
        .unwrap_or(DEFAULT_UPDATE_TIME.to_string());
    let time = time.parse().unwrap();
    let when = chrono::offset::Local::now().date().succ().and_hms(time, 0, 0);
    tracing::info!("Next update time: {}", when);
    let next_day = when.signed_duration_since(chrono::offset::Local::now());
    let std_next_day = match next_day.to_std() {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("Failed to convert signed duration to std: {}", e);
            panic!("Failed to convert signed duration to std");
        }
    };

    let instant = match time::Instant::now().checked_add(std_next_day) {
        Some(i) => i,
        None => {
            tracing::error!("Failed to add signed duration to instant");
            panic!("Failed to add signed duration to instant");
        }
    };
    instant
}
