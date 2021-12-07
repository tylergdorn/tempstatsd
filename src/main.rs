#![deny(warnings)]

use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Result};
use serde_derive::{Deserialize, Serialize};
use warp::Filter;

type Pool = r2d2::Pool<SqliteConnectionManager>;
type PoolConn = r2d2::PooledConnection<SqliteConnectionManager>;

#[derive(Deserialize, Serialize, Debug)]
struct Stats {
    temp: f32,
    humidity: f32,
    sensor: String,
}

#[tokio::main]
async fn main() {
    let manager = SqliteConnectionManager::file("./temperature.db");
    let pool = r2d2::Pool::new(manager).unwrap();
    init_tables(pool.get().unwrap()).unwrap();

    let error_path = warp::path("log")
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(with_pool(pool.clone()))
        .and_then(post_log);

    let temperature_stats = warp::path("temperature")
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(with_pool(pool.clone()))
        .and_then(post_temp);

    let post_path = warp::post().and(error_path).or(temperature_stats);
    warp::serve(post_path).run(([127, 0, 0, 1], 3030)).await;
}

fn init_tables(mut conn: PoolConn) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("CREATE TABLE IF NOT EXISTS temperature (sensor TEXT, temperature REAL, humidity REAL, time TEXT)", params![])?;
    tx.execute(
        "CREATE TABLE IF NOT EXISTS logs (sensor TEXT, message TEXT, time TEXT)",
        params![],
    )?;
    tx.commit()
}

#[derive(Deserialize, Serialize, Debug)]
struct ErrorMessage {
    message: String,
}
impl warp::reject::Reject for ErrorMessage {}

async fn post_temp(stat: Stats, pool: Pool) -> Result<impl warp::Reply, warp::Rejection> {
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            return Err(warp::reject::custom(ErrorMessage {
                message: e.to_string(),
            }));
        }
    };
    match insert_temp(conn, &stat) {
        Ok(_) => println!("inserted row: {}", stat),
        Err(e) => {
            return Err(warp::reject::custom(ErrorMessage {
                message: e.to_string(),
            }));
        }
    }
    Ok(warp::reply())
}

async fn post_log(log: Log, pool: Pool) -> Result<impl warp::Reply, warp::Rejection> {
    let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            return Err(warp::reject::custom(ErrorMessage {
                message: e.to_string(),
            }));
        }
    };
    match insert_log(conn, &log) {
        Ok(_) => println!("inserted row: {}", log),
        Err(e) => {
            return Err(warp::reject::custom(ErrorMessage {
                message: e.to_string(),
            }));
        }
    }
    Ok(warp::reply())
}

fn with_pool(
    pool: Pool,
) -> impl Filter<Extract = (Pool,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || pool.clone())
}

fn insert_temp(conn: PoolConn, s: &Stats) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "INSERT INTO temperature (sensor, temperature, humidity, time) VALUES (?1, ?2, ?3, datetime('now'))",
        params![s.sensor, s.humidity, s.temp],
    )
}

#[derive(Deserialize, Serialize, Debug)]
struct Log {
    message: String,
    sensor: String,
}

impl std::fmt::Display for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "sensor: {} message: {}", self.sensor, self.message)
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "sensor: {} humidity: {} temperature: {}",
            self.sensor, self.humidity, self.temp
        )
    }
}

fn insert_log(conn: PoolConn, log: &Log) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "INSERT INTO logs (sensor, message, time) VALUES (?1, ?2, datetime('now'))",
        params![log.sensor, log.message],
    )
}
