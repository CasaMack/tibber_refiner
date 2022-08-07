use tibber_refiner::run::{get_db_info, get_instant, get_logger, get_retries, tick};
use tokio::time;

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = get_logger();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global default subscriber");
    tracing::trace!("Log setup complete");

    let (db_addr, db_name) = get_db_info();
    let retries = get_retries();

    let res = tick(db_addr.clone(), db_name.clone()).await;
    match res {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("{}", e)
        }
    }
    //    loop {
    //        let instant = get_instant();
    //        time::sleep_until(instant).await;
    //        for i in 0..retries {
    //            let res = tick(
    //                db_addr.clone(),
    //                db_name.clone(),
    //            )
    //            .await;
    //            if res.is_ok() {
    //                break;
    //            } else {
    //                tracing::warn!("Failed attempt {} to tick: {}", i, res.err().unwrap());
    //                let backoff = 2_u64.pow(i);
    //                tracing::debug!("Exponential backoff: {} seconds", backoff);
    //                time::sleep(time::Duration::from_secs(backoff)).await;
    //            }
    //        tracing::error!("Unable to refine values after {} retires. Giving up", retries);
    //        }
    //    }
}
