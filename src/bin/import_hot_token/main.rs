//! Import the mint column from a hot-token CSV into `solana.hot_token`.
//!
//! The input is expected to have a header and at least two columns. Only the
//! second column (`mint_address` in the supplied report) is written; all other
//! columns are deliberately ignored. The target table must already exist.

use clap::Parser;
use clickhouse::{Client, Row};
use dotenvy::dotenv;
use percent_encoding::percent_decode_str;
use serde::Serialize;
use std::error::Error;
use std::path::{Path, PathBuf};
use url::Url;

const DATABASE: &str = "solana";
const TABLE: &str = "hot_token";

#[derive(Parser, Debug)]
#[clap(author, version, about = "Import mint addresses from a hot-token CSV")]
struct Args {
    /// CSV file containing the report (the second column must be the mint).
    #[clap(value_name = "CSV")]
    csv: PathBuf,
}

#[derive(Row, Serialize)]
struct HotTokenRow {
    mint: String,
}

struct ClickhouseConnection {
    endpoint: String,
    user: Option<String>,
    password: Option<String>,
}

fn main() {
    let args = Args::parse();
    dotenv().ok();

    let result = (|| -> Result<(), Box<dyn Error>> {
        let clickhouse_url = std::env::var("CLICKHOUSE_URL")
            .map_err(|_| "CLICKHOUSE_URL must be set in the environment or .env file")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(import_csv(&args.csv, &clickhouse_url))
    })();

    if let Err(error) = result {
        eprintln!("import_hot_token: {error}");
        std::process::exit(1);
    }
}

async fn import_csv(path: &Path, clickhouse_url: &str) -> Result<(), Box<dyn Error>> {
    let client = new_clickhouse_client(clickhouse_url)?;
    let mut reader = csv::Reader::from_path(path)?;
    let mut insert = client.insert::<HotTokenRow>(TABLE).await?;
    let mut imported = 0usize;

    for (index, record) in reader.records().enumerate() {
        let line = index + 2; // account for the header row
        let record = record.map_err(|error| format!("CSV line {line}: {error}"))?;
        let mint = record
            .get(1)
            .ok_or_else(|| format!("CSV line {line}: expected a second column (mint_address)"))?
            .trim();
        if mint.is_empty() {
            return Err(format!("CSV line {line}: mint_address is empty").into());
        }

        insert
            .write(&HotTokenRow {
                mint: mint.to_owned(),
            })
            .await?;
        imported += 1;
    }

    insert.end().await?;
    println!("imported {imported} mint addresses into solana.{TABLE}");
    Ok(())
}

fn new_clickhouse_client(connection_url: &str) -> Result<Client, Box<dyn Error>> {
    let connection = parse_clickhouse_connection_url(connection_url)?;
    let mut client = Client::default()
        .with_url(connection.endpoint)
        .with_database(DATABASE)
        // The row contains only the mint column; use RowBinary and let
        // ClickHouse apply defaults for enable/version.
        .with_validation(false);

    if let Some(user) = connection.user {
        client = client.with_user(user);
    }
    if let Some(password) = connection.password {
        client = client.with_password(password);
    }
    Ok(client)
}

fn parse_clickhouse_connection_url(
    connection_url: &str,
) -> Result<ClickhouseConnection, Box<dyn Error>> {
    let mut endpoint = Url::parse(connection_url)?;
    let has_userinfo = !endpoint.username().is_empty() || endpoint.password().is_some();
    let user = (!endpoint.username().is_empty())
        .then(|| decode_url_component(endpoint.username()))
        .transpose()?;
    let password = endpoint.password().map(decode_url_component).transpose()?;

    if has_userinfo {
        endpoint
            .set_username("")
            .map_err(|_| "CLICKHOUSE_URL has invalid username information")?;
        endpoint
            .set_password(None)
            .map_err(|_| "CLICKHOUSE_URL has invalid password information")?;
    }

    Ok(ClickhouseConnection {
        endpoint: endpoint.to_string(),
        user,
        password,
    })
}

fn decode_url_component(value: &str) -> Result<String, Box<dyn Error>> {
    Ok(percent_decode_str(value).decode_utf8()?.into_owned())
}
