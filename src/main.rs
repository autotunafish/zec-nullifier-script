use std::{
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use reqwest::Client;
use serde_json::{json, Value};

const DEFAULT_RPC_URL: &str = "http://127.0.0.1:18232/";
const DEFAULT_OUTPUT_FILE: &str = "nullifiers.txt";
const DEFAULT_PROGRESS_INTERVAL: u64 = 100;
const DEFAULT_TX_CONCURRENCY: usize = 16;

#[derive(Clone)]
struct Config {
    rpc_url: String,
    output_file: PathBuf,
    start_height: u64,
    progress_interval: u64,
    tx_concurrency: usize,
}

#[derive(Default)]
struct Totals {
    blocks_scanned: u64,
    transactions_scanned: u64,
    nullifiers_found: u64,
}

#[derive(Debug)]
struct RpcError {
    message: String,
}

struct TempFileGuard {
    path: PathBuf,
    keep: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn persist(&mut self) {
        self.keep = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let client = Client::new();
    let tmp_path = temp_output_path(&config.output_file);
    let mut tmp_guard = TempFileGuard::new(tmp_path.clone());
    let mut writer = BufWriter::new(
        File::create(&tmp_path)
            .with_context(|| format!("failed to create temporary output {}", tmp_path.display()))?,
    );

    let mut height = config.start_height;
    let mut totals = Totals::default();
    let stop_reason;

    loop {
        let block_response =
            rpc_call(&client, &config.rpc_url, "getblock", json!([height.to_string()])).await;
        let block = match block_response {
            Ok(block) => block,
            Err(error) if error.message.contains("block height not in best chain") => {
                stop_reason = error.message;
                break;
            }
            Err(error) => bail!(
                "Error fetching block {}: {}",
                height,
                unknown_if_empty(&error.message)
            ),
        };

        let tx_hashes = tx_hashes(&block)?;
        totals.blocks_scanned += 1;

        let tx_results = stream::iter(tx_hashes.into_iter().map(|tx_hash| {
            let client = client.clone();
            let rpc_url = config.rpc_url.clone();
            async move {
                let tx = rpc_call(
                    &client,
                    &rpc_url,
                    "getrawtransaction",
                    json!([tx_hash.clone(), 1]),
                )
                    .await
                    .map_err(|error| anyhow!("{}: {}", tx_hash, unknown_if_empty(&error.message)))?;
                Ok::<_, anyhow::Error>(collect_nullifiers(&tx))
            }
        }))
        .buffered(config.tx_concurrency)
        .try_collect::<Vec<_>>()
        .await
        .with_context(|| format!("Error fetching transaction at block {}", height))?;

        for nullifiers in tx_results {
            totals.transactions_scanned += 1;
            for nullifier in nullifiers {
                if totals.nullifiers_found > 0 {
                    writer.write_all(b",")?;
                }
                writer.write_all(nullifier.as_bytes())?;
                totals.nullifiers_found += 1;
            }
        }

        if totals.blocks_scanned % config.progress_interval == 0 {
            println!(
                "Scanned {} blocks through height {}; transactions: {}; nullifiers: {}",
                totals.blocks_scanned, height, totals.transactions_scanned, totals.nullifiers_found
            );
        }

        height += 1;
    }

    writer.flush()?;
    drop(writer);
    fs::rename(&tmp_path, &config.output_file).with_context(|| {
        format!(
            "failed to move {} to {}",
            tmp_path.display(),
            config.output_file.display()
        )
    })?;
    tmp_guard.persist();

    let file_size_bytes = fs::metadata(&config.output_file)
        .with_context(|| format!("failed to read metadata for {}", config.output_file.display()))?
        .len();

    println!("Completed nullifier scan");
    println!("Stop reason: {}", stop_reason);
    println!("Start height: {}", config.start_height);
    println!("Last requested height: {}", height);
    println!("Blocks scanned: {}", totals.blocks_scanned);
    println!("Transactions scanned: {}", totals.transactions_scanned);
    println!("Nullifiers collected: {}", totals.nullifiers_found);
    println!("Output file: {}", config.output_file.display());
    println!("Output size: {} bytes", file_size_bytes);

    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            rpc_url: env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string()),
            output_file: env::var("OUTPUT_FILE")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(DEFAULT_OUTPUT_FILE)),
            start_height: parse_env_u64("START_HEIGHT", 0)?,
            progress_interval: parse_env_u64("PROGRESS_INTERVAL", DEFAULT_PROGRESS_INTERVAL)
                .and_then(|value| {
                    if value == 0 {
                        bail!("PROGRESS_INTERVAL must be a positive integer");
                    }
                    Ok(value)
                })?,
            tx_concurrency: parse_env_usize("TX_CONCURRENCY", DEFAULT_TX_CONCURRENCY).and_then(
                |value| {
                    if value == 0 {
                        bail!("TX_CONCURRENCY must be a positive integer");
                    }
                    Ok(value)
                },
            )?,
        })
    }
}

fn parse_env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{} must be a non-negative integer", name)),
        Err(_) => Ok(default),
    }
}

fn parse_env_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{} must be a positive integer", name)),
        Err(_) => Ok(default),
    }
}

async fn rpc_call(
    client: &Client,
    rpc_url: &str,
    method: &str,
    params: Value,
) -> std::result::Result<Value, RpcError> {
    let response = client
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "1.0",
            "id": "nullifier-scan",
            "method": method,
            "params": params,
        }))
        .send()
        .await
        .map_err(|error| RpcError {
            message: error.to_string(),
        })?;

    let body = response.json::<Value>().await.map_err(|error| RpcError {
        message: error.to_string(),
    })?;

    if !body["error"].is_null() {
        return Err(RpcError {
            message: body["error"]["message"]
                .as_str()
                .unwrap_or("unknown RPC error")
                .to_string(),
        });
    }

    Ok(body["result"].clone())
}

fn tx_hashes(block: &Value) -> Result<Vec<String>> {
    Ok(block["tx"]
        .as_array()
        .ok_or_else(|| anyhow!("block response did not contain a tx array"))?
        .iter()
        .filter_map(|tx| tx.as_str().map(ToString::to_string))
        .collect())
}

fn collect_nullifiers(value: &Value) -> Vec<String> {
    let mut nullifiers = Vec::new();
    collect_nullifiers_into(value, &mut nullifiers);
    nullifiers
}

fn collect_nullifiers_into(value: &Value, nullifiers: &mut Vec<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_nullifiers_into(value, nullifiers);
            }
        }
        Value::Object(object) => {
            if let Some(nullifier) = object.get("nullifier") {
                match nullifier {
                    Value::Array(values) => {
                        for value in values {
                            push_nullifier(value, nullifiers);
                        }
                    }
                    value => push_nullifier(value, nullifiers),
                }
            }

            for value in object.values() {
                collect_nullifiers_into(value, nullifiers);
            }
        }
        _ => {}
    }
}

fn push_nullifier(value: &Value, nullifiers: &mut Vec<String>) {
    match value {
        Value::String(nullifier) => nullifiers.push(nullifier.clone()),
        Value::Null => {}
        other => nullifiers.push(other.to_string()),
    }
}

fn temp_output_path(output_file: &Path) -> PathBuf {
    let pid = std::process::id();
    let file_name = output_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("nullifiers.txt");
    output_file.with_file_name(format!("{}.tmp.{}", file_name, pid))
}

fn unknown_if_empty(message: &str) -> &str {
    if message.is_empty() {
        "unknown RPC error"
    } else {
        message
    }
}
