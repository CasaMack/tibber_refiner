use influxdb::{Client, InfluxDbWriteable, ReadQuery};
use serde::Deserialize;
use tracing::instrument;

type HourPrice = (usize, f64);

#[derive(Copy, Clone, Debug)]
pub enum Day {
    Today,
    Tomorrow,
}

#[derive(Deserialize)]
struct QueryResults {
    pub results: Vec<Statement>,
}

#[derive(Deserialize)]
struct Statement {
    pub _statement_id: usize,
    pub series: Vec<Serie>,
}

#[derive(Deserialize)]
struct Serie {
    pub _name: String,
    pub _columns: Vec<String>,
    pub values: Vec<Value>,
}

#[derive(Deserialize)]
struct Value {
    _datetime: String,
    pub value: f64,
    pub hour: u32,
}

#[instrument(skip(client))]
pub async fn get_prices(day: Day, client: &Client) -> Result<Vec<HourPrice>, ()> {
    let date = match day {
        Day::Today => chrono::Local::now()
            .date()
            .to_string()
            .split('+')
            .into_iter()
            .next()
            .ok_or(())?
            .to_owned(),
        Day::Tomorrow => chrono::Local::now()
            .date()
            .succ()
            .to_string()
            .split('+')
            .into_iter()
            .next()
            .ok_or(())?
            .to_owned(),
    };
    let read_query = ReadQuery::new(format!(
        "SELECT price, hour FROM price_info WHERE date = '{}'",
        date
    ));

    let read_result = client.query(read_query).await;
    match read_result {
        Ok(result) => {
            println!("{}", result);
            let r: QueryResults = serde_json::from_str(&result).or(Err(()))?;
            Ok(r.results
                .get(0)
                .ok_or(())?
                .series
                .get(0)
                .ok_or(())?
                .values
                .iter()
                .map(|val| (val.hour as usize, val.value))
                .collect())
        }
        Err(e) => {
            eprintln!("{}", e);
            tracing::error!("{}", e);
            Err(())
        }
    }
}

pub async fn get_hour_price(day: Day, client: &Client) -> Result<Vec<HourPrice>, ()> {
    Ok(get_prices(day, client).await?)
}

pub fn price_now(now: usize, prices: &Vec<HourPrice>) -> Result<f64, ()> {
    Ok(prices.get(now).ok_or(())?.1.to_owned())
}

pub fn average(prices: &Vec<HourPrice>) -> Result<f64, ()> {
    Ok(prices.iter().map(|hour_price| hour_price.1).sum::<f64>() / 24.0)
}

pub fn price_ratio(now: usize, prices: &Vec<HourPrice>) -> Result<f64, ()> {
    Ok(price_now(now, prices)? / average(prices)?)
}

pub async fn highest(
    day: Day,
    count: usize,
    start: usize,
    stop: usize,
    client: &Client,
) -> Result<Vec<HourPrice>, ()> {
    let mut prices: Vec<HourPrice> = get_prices(day, client)
        .await?
        .into_iter()
        .filter(|hour_price| start <= hour_price.0 && hour_price.0 <= stop)
        .collect();
    prices.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(prices.into_iter().take(count).collect())
}

pub async fn lowest(
    day: Day,
    count: usize,
    start: usize,
    stop: usize,
    client: &Client,
) -> Result<Vec<HourPrice>, ()> {
    let mut prices: Vec<HourPrice> = get_prices(day, client)
        .await?
        .into_iter()
        .filter(|hour_price| start <= hour_price.0 && hour_price.0 <= stop)
        .collect();
    prices.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    Ok(prices.into_iter().take(count).collect())
}

pub async fn max(day: Day, client: &Client) -> Result<HourPrice, ()> {
    Ok(highest(day, 1, 0, 24, client)
        .await?
        .first()
        .take()
        .ok_or(())?
        .to_owned())
}

pub async fn min(day: Day, client: &Client) -> Result<HourPrice, ()> {
    Ok(lowest(day, 1, 0, 24, client)
        .await?
        .first()
        .take()
        .ok_or(())?
        .to_owned())
}

pub async fn rel_thresh(
    day: Day,
    mut low_thresh: f64,
    mut high_thresh: f64,
    prices: &Vec<HourPrice>,
    client: &Client,
) -> Result<Vec<HourPrice>, ()> {
    let avg = average(prices)?;
    if low_thresh > 1.0 {
        low_thresh /= 100.0;
    }
    let low_val = low_thresh * avg;

    if high_thresh > 1.0 {
        high_thresh /= 100.0;
    }
    let high_val = high_thresh * avg;
    Ok(get_hour_price(day, client)
        .await?
        .into_iter()
        .filter(|(_, price)| high_val > *price && *price > low_val)
        .collect())
}

pub async fn within_thresh(
    now: usize,
    low_thresh: f64,
    high_thresh: f64,
    prices: &Vec<HourPrice>,
    client: &Client,
) -> Result<bool, ()> {
    Ok(
        rel_thresh(Day::Today, low_thresh, high_thresh, prices, client)
            .await?
            .iter()
            .map(|hour_price| hour_price.0)
            .any(|hour| hour == now),
    )
}
pub async fn in_6_l_8(day: Day, now: usize, client: &Client) -> Result<bool, ()> {
    Ok(!(highest(day, 2, 0, 8, client)
        .await?
        .iter()
        .map(|hour_price| hour_price.0)
        .any(|hour| hour == now))
        && highest(day, 8, 0, 8, client)
            .await?
            .iter()
            .map(|hour_price| hour_price.0)
            .any(|hour| hour == now))
}

pub async fn in_top(
    day: Day,
    now: usize,
    start: usize,
    stop: usize,
    client: &Client,
) -> Result<bool, ()> {
    Ok(highest(day, 3, start, stop, client)
        .await?
        .iter()
        .map(|hour_price| hour_price.0)
        .any(|hour| hour == now))
}

pub async fn in_8_low(now: usize, client: &Client) -> Result<bool, ()> {
    Ok(lowest(Day::Today, 8, 0, 8, client)
        .await?
        .iter()
        .map(|hour_price| hour_price.0)
        .any(|hour| hour == now))
}

#[derive(InfluxDbWriteable)]
struct Refined {
    time: chrono::DateTime<chrono::Utc>,
    #[influxdb(tag)]
    hour: u32,
    #[influxdb(tag)]
    date: String,
    pris_snitt_24: f64,
    in_6_l_8: bool,
    in_0_6_high: bool,
    in_6_12_high: bool,
    in_12_18_high: bool,
    in_18_24_high: bool,
    t90_115: bool,
    t60_90: bool,
    t0_60: bool,
    t115_140: bool,
    t140_999: bool,
    i8h_low: bool,
    pris_time: f64,
    pris_forhold_24: f64,
    pris_max: u32,
    pris_min: u32,
}

pub async fn refine(hour: usize, client: &Client) -> Result<(), ()> {
    let prices = get_prices(Day::Today, client).await?;

    let fut_in_6_l_8 = in_6_l_8(Day::Today, hour, client);
    let fut_in_0_6_high = in_top(Day::Today, hour, 0, 6, client);
    let fut_in_6_12_high = in_top(Day::Today, hour, 6, 12, client);
    let fut_in_12_18_high = in_top(Day::Today, hour, 12, 18, client);
    let fut_in_18_24_high = in_top(Day::Today, hour, 18, 24, client);
    let fut_t90_115 = within_thresh(hour, 90.0, 115.0, &prices, client);
    let fut_t60_90 = within_thresh(hour, 60.0, 90.0, &prices, client);
    let fut_t0_60 = within_thresh(hour, 0.0, 60.0, &prices, client);
    let fut_t115_140 = within_thresh(hour, 115.0, 140.0, &prices, client);
    let fut_t140_999 = within_thresh(hour, 140.0, 999.0, &prices, client);
    let fut_i8h_low = in_8_low(hour, client);
    let fut_pris_max = max(Day::Today, client);
    let fut_pris_min = min(Day::Today, client);

    let (
        in_6_l_8,
        in_0_6_high,
        in_6_12_high,
        in_12_18_high,
        in_18_24_high,
        t90_115,
        t60_90,
        t0_60,
        t115_140,
        t140_999,
        i8h_low,
        pris_max,
        pris_min,
    ) = tokio::join!(
        fut_in_6_l_8,
        fut_in_0_6_high,
        fut_in_6_12_high,
        fut_in_12_18_high,
        fut_in_18_24_high,
        fut_t90_115,
        fut_t60_90,
        fut_t0_60,
        fut_t115_140,
        fut_t140_999,
        fut_i8h_low,
        fut_pris_max,
        fut_pris_min
    );

    let refined = Refined {
        time: chrono::Utc::now().date().and_hms(hour as u32, 0, 0),
        hour: hour as u32,
        date: chrono::Utc::now()
            .date()
            .and_hms(0, 0, 0)
            .to_rfc3339()
            .split('T')
            .into_iter()
            .next()
            .unwrap()
            .to_string(),
        pris_snitt_24: average(&prices)?,
        pris_time: price_now(hour, &prices)?,
        pris_forhold_24: price_ratio(hour, &prices)?,
        pris_max: pris_max?.0 as u32,
        pris_min: pris_min?.0 as u32,
        in_6_l_8: in_6_l_8?,
        in_0_6_high: in_0_6_high?,
        in_6_12_high: in_6_12_high?,
        in_12_18_high: in_12_18_high?,
        in_18_24_high: in_18_24_high?,
        t90_115: t90_115?,
        t60_90: t60_90?,
        t0_60: t0_60?,
        t115_140: t115_140?,
        t140_999: t140_999?,
        i8h_low: i8h_low?,
    };

    let write_query = refined.into_query("refined");

    let write_result = client.query(write_query).await;

    match write_result {
        Ok(_) => Ok(()),
        Err(_) => Err(()),
    }
}
