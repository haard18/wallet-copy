use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use std::io::Write;
use std::sync::{mpsc, Arc};
use ethers::signers::{LocalWallet, Signer};
use hyperliquid_rust_sdk::{
    BaseUrl, ExchangeClient, ExchangeResponseStatus, MarketOrderParams, MarketCloseParams,
};

const MESSAGES_LOG: &str = "messages.log";
const STATUS_LOG: &str = "status.log";
const HEARTBEAT_SECS: u64 = 30;
const PING_SECS: u64 = 20;
const RECONNECT_SECS: u64 = 5;
const SLIPPAGE: f64 = 0.01;

struct ConnectionStats {
    message_count: u64,
    event_count: u64,
    order_count: u64,
    last_activity: std::time::Instant,
    last_heartbeat: std::time::Instant,
    started_at: std::time::Instant,
}

fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn log(path: &str, level: &str, msg: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{}|{}|{}", now(), level, msg);
    }
}

fn short_addr(addr: &str) -> String {
    if addr.len() > 8 { addr[addr.len()-8..].to_string() } else { addr.to_string() }
}

fn events_path(addr: &str) -> String {
    format!("events_{}.log", short_addr(addr))
}

#[inline(always)]
fn send_event(tx: &mpsc::Sender<String>, kind: &str, coin: Option<&str>, side: Option<&str>, size: Option<&str>, price: Option<&str>, time: Option<i64>) {
    let line = format!(
        "{}|{}|{}|{}|{}|{}|{}\n",
        now(), kind,
        coin.unwrap_or("-"), side.unwrap_or("-"),
        size.unwrap_or("-"), price.unwrap_or("-"),
        time.map(|t| t.to_string()).as_deref().unwrap_or("-")
    );
    let _ = tx.send(line);
}

#[inline(always)]
fn send_order(tx: &mpsc::Sender<String>, coin: &str, side: &str, price: &str, size: &str, oid: i64, status: &str, ts: i64, status_ts: i64) {
    let line = format!("{}|ORDER|{}|{}|{}|{}|{}|{}|{}|{}\n", now(), coin, side, size, price, oid, status, ts, status_ts);
    let _ = tx.send(line);
}

/// Copy a fill by executing the same trade on our account
async fn copy_fill(client: &ExchangeClient, fill: &Value) {
    let coin = fill.get("coin").and_then(|v| v.as_str()).unwrap_or("");
    let dir = fill.get("dir").and_then(|v| v.as_str()).unwrap_or("");
    let sz: f64 = fill.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);

    if coin.is_empty() || sz == 0.0 {
        return;
    }

    match dir {
        "Open Long" => {
            let params = MarketOrderParams {
                asset: coin,
                is_buy: true,
                sz,
                px: None,
                slippage: Some(SLIPPAGE),
                cloid: None,
                wallet: None,
            };
            match client.market_open(params).await {
                Ok(ExchangeResponseStatus::Ok(_)) => {
                    eprintln!("[{}] COPIED|OPEN_LONG|{}|{}", now(), coin, sz);
                    log(STATUS_LOG, "COPY", &format!("OPEN_LONG|{}|{}", coin, sz));
                }
                Ok(ExchangeResponseStatus::Err(e)) => {
                    eprintln!("[{}] ERR|OPEN_LONG|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("OPEN_LONG|{}|{}", coin, e));
                }
                Err(e) => {
                    eprintln!("[{}] ERR|OPEN_LONG|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("OPEN_LONG|{}|{}", coin, e));
                }
            }
        }
        "Open Short" => {
            let params = MarketOrderParams {
                asset: coin,
                is_buy: false,
                sz,
                px: None,
                slippage: Some(SLIPPAGE),
                cloid: None,
                wallet: None,
            };
            match client.market_open(params).await {
                Ok(ExchangeResponseStatus::Ok(_)) => {
                    eprintln!("[{}] COPIED|OPEN_SHORT|{}|{}", now(), coin, sz);
                    log(STATUS_LOG, "COPY", &format!("OPEN_SHORT|{}|{}", coin, sz));
                }
                Ok(ExchangeResponseStatus::Err(e)) => {
                    eprintln!("[{}] ERR|OPEN_SHORT|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("OPEN_SHORT|{}|{}", coin, e));
                }
                Err(e) => {
                    eprintln!("[{}] ERR|OPEN_SHORT|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("OPEN_SHORT|{}|{}", coin, e));
                }
            }
        }
        "Close Long" | "Close Short" => {
            let params = MarketCloseParams {
                asset: coin,
                sz: Some(sz),
                px: None,
                slippage: Some(SLIPPAGE),
                cloid: None,
                wallet: None,
            };
            match client.market_close(params).await {
                Ok(ExchangeResponseStatus::Ok(_)) => {
                    eprintln!("[{}] COPIED|CLOSE|{}|{}", now(), coin, sz);
                    log(STATUS_LOG, "COPY", &format!("CLOSE|{}|{}", coin, sz));
                }
                Ok(ExchangeResponseStatus::Err(e)) => {
                    eprintln!("[{}] ERR|CLOSE|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("CLOSE|{}|{}", coin, e));
                }
                Err(e) => {
                    eprintln!("[{}] ERR|CLOSE|{}|{}", now(), coin, e);
                    log(STATUS_LOG, "ERROR", &format!("CLOSE|{}|{}", coin, e));
                }
            }
        }
        _ => {
            log(STATUS_LOG, "DEBUG", &format!("Unknown dir: {} for {} sz={}", dir, coin, sz));
        }
    }
}

async fn connect_wallet(addr: &str, msg_tx: mpsc::Sender<String>, client: Arc<ExchangeClient>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let name = short_addr(addr);
    let (event_tx, event_rx) = mpsc::channel::<String>();

    let path = events_path(addr);
    std::thread::spawn(move || {
        let f = std::fs::OpenOptions::new().create(true).append(true).open(&path).expect("open events file");
        let mut w = std::io::BufWriter::new(f);
        let mut batch: Vec<String> = Vec::with_capacity(10);
        let mut flushed = std::time::Instant::now();

        loop {
            let line = match event_rx.try_recv() {
                Ok(l) => Some(l),
                Err(mpsc::TryRecvError::Empty) => {
                    if !batch.is_empty() && flushed.elapsed().as_millis() > 100 {
                        for l in &batch { let _ = w.write_all(l.as_bytes()); }
                        let _ = w.flush();
                        batch.clear();
                        flushed = std::time::Instant::now();
                    }
                    event_rx.recv().ok()
                }
                Err(mpsc::TryRecvError::Disconnected) => None,
            };

            if let Some(l) = line {
                batch.push(l);
                if batch.len() >= 10 {
                    for l in &batch { let _ = w.write_all(l.as_bytes()); }
                    let _ = w.flush();
                    batch.clear();
                    flushed = std::time::Instant::now();
                }
            } else {
                for l in &batch { let _ = w.write_all(l.as_bytes()); }
                let _ = w.flush();
                break;
            }
        }
    });

    let mut stats = ConnectionStats {
        message_count: 0,
        event_count: 0,
        order_count: 0,
        last_activity: std::time::Instant::now(),
        last_heartbeat: std::time::Instant::now(),
        started_at: std::time::Instant::now(),
    };

    log(STATUS_LOG, "STARTUP", &format!("monitor {} ({})", name, now()));
    eprintln!("[{}] Starting monitor for {}", now(), name);

    let (ws, _) = match connect_async("wss://api.hyperliquid.xyz/ws").await {
        Ok(s) => { log(STATUS_LOG, "CONNECTED", &name); eprintln!("[{}] ✓ Connected {}", now(), name); s }
        Err(e) => { log(STATUS_LOG, "ERROR", &format!("{}: {}", name, e)); return Err(e.into()); }
    };

    let (mut tx, mut rx) = ws.split();

    let sub_events = format!(r#"{{"method":"subscribe","subscription":{{"type":"userEvents","user":"{}"}}}}"#, addr.to_lowercase());
    tx.send(Message::Text(sub_events)).await?;
    log(STATUS_LOG, "SUBSCRIBED", &format!("userEvents {}", name));

    let sub_orders = format!(r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{}"}}}}"#, addr.to_lowercase());
    tx.send(Message::Text(sub_orders)).await?;
    log(STATUS_LOG, "SUBSCRIBED", &format!("orderUpdates {}", name));

    let mut ping_timer = tokio::time::interval(tokio::time::Duration::from_secs(PING_SECS));
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tx.send(Message::Ping(vec![])).await?;

    loop {
        tokio::select! {
            msg_opt = rx.next() => {
                let msg = match msg_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => { log(STATUS_LOG, "ERROR", &format!("{}: {}", name, e)); return Err(e.into()); }
                    None => return Err("stream ended".into()),
                };

                stats.message_count += 1;
                stats.last_activity = std::time::Instant::now();

                if stats.last_heartbeat.elapsed().as_secs() >= HEARTBEAT_SECS {
                    let hb = format!("[{}]: up={}s msgs={} events={} orders={}", name, stats.started_at.elapsed().as_secs(), stats.message_count, stats.event_count, stats.order_count);
                    log(STATUS_LOG, "HEARTBEAT", &hb);
                    eprintln!("[{}] {}", now(), hb);
                    stats.last_heartbeat = std::time::Instant::now();
                }

                let data = match &msg {
                    Message::Text(t) => { let _ = msg_tx.send(format!("{}|{}|TEXT|{}\n", now(), name, t)); t.as_bytes().to_vec() }
                    Message::Binary(b) => { let _ = msg_tx.send(format!("{}|{}|BINARY|{}\n", now(), name, b.len())); b.clone() }
                    Message::Ping(d) => { tx.send(Message::Pong(d.clone())).await?; continue; }
                    Message::Pong(_) => { stats.last_activity = std::time::Instant::now(); continue; }
                    Message::Close(_) => return Err("closed".into()),
                    Message::Frame(_) => continue,
                };

                let json: Value = match serde_json::from_slice(&data) { Ok(v) => v, Err(_) => continue };
                let channel = match json.get("channel").and_then(|v| v.as_str()) { Some(c) => c, None => continue };

                if channel == "subscriptionResponse" { continue; }

                if channel == "userEvents" {
                    if let Some(d) = json.get("data") {
                        if let Some(fills) = d.get("fills").and_then(|f| f.as_array()) {
                            for fill in fills {
                                send_event(&event_tx, "fill", fill.get("coin").and_then(|v| v.as_str()), fill.get("side").and_then(|v| v.as_str()), fill.get("sz").and_then(|v| v.as_str()), fill.get("px").and_then(|v| v.as_str()), fill.get("time").and_then(|v| v.as_i64()));
                                stats.event_count += 1;
                                // Copy the trade
                                eprintln!("[{}] FILL|{}", now(), fill);
                                copy_fill(&client, fill).await;
                            }
                        }
                        if let Some(funding) = d.get("funding") {
                            send_event(&event_tx, "funding", funding.get("coin").and_then(|v| v.as_str()), None, None, None, funding.get("time").and_then(|v| v.as_i64()));
                            stats.event_count += 1;
                        }
                        if d.get("liquidation").is_some() {
                            send_event(&event_tx, "liquidation", None, None, None, None, None);
                            stats.event_count += 1;
                        }
                        if let Some(cancels) = d.get("nonUserCancel").and_then(|c| c.as_array()) {
                            for c in cancels {
                                send_event(&event_tx, "nonUserCancel", c.get("coin").and_then(|v| v.as_str()), None, None, None, None);
                                stats.event_count += 1;
                            }
                        }
                    }
                } else if channel == "orderUpdates" {
                    if let Some(d) = json.get("data") {
                        let orders: Vec<&Value> = if let Some(arr) = d.as_array() { arr.iter().collect() } else if d.is_object() { vec![d] } else { vec![] };
                        for upd in orders {
                            if let Some(o) = upd.get("order") {
                                send_order(&event_tx,
                                    o.get("coin").and_then(|v| v.as_str()).unwrap_or("-"),
                                    o.get("side").and_then(|v| v.as_str()).unwrap_or("-"),
                                    o.get("limitPx").and_then(|v| v.as_str()).unwrap_or("-"),
                                    o.get("sz").and_then(|v| v.as_str()).unwrap_or("-"),
                                    o.get("oid").and_then(|v| v.as_i64()).unwrap_or(0),
                                    upd.get("status").and_then(|v| v.as_str()).unwrap_or("-"),
                                    o.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0),
                                    upd.get("statusTimestamp").and_then(|v| v.as_i64()).unwrap_or(0)
                                );
                                stats.order_count += 1;
                            }
                        }
                    }
                }
            }
            _ = ping_timer.tick() => {
                stats.last_activity = std::time::Instant::now();
                tx.send(Message::Ping(vec![])).await?;
            }
        }
    }
}

/// Watch wallets and copy their trades
pub async fn watch_and_copy(wallets: &[&str]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if wallets.is_empty() { return Ok(()); }

    // Initialize exchange client for copy trading
    let pk = std::env::var("PRIVATE_KEY")
        .map_err(|_| "PRIVATE_KEY env var not set")?
        .trim_start_matches("0x")
        .to_string();
    let wallet: LocalWallet = pk.parse()
        .map_err(|e| format!("Failed to parse private key: {}", e))?;
    eprintln!("[{}] Copy trading wallet: {:?}", now(), wallet.address());

    let client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None)
        .await
        .map_err(|e| format!("Failed to create exchange client: {}", e))?;
    let client = Arc::new(client);
    eprintln!("[{}] Exchange client ready", now());

    let (msg_tx, msg_rx) = mpsc::channel::<String>();

    std::thread::spawn(move || {
        let f = std::fs::OpenOptions::new().create(true).append(true).open(MESSAGES_LOG).expect("open messages");
        let mut w = std::io::BufWriter::new(f);
        let mut batch: Vec<String> = Vec::with_capacity(10);
        let mut flushed = std::time::Instant::now();

        loop {
            let line = match msg_rx.try_recv() {
                Ok(l) => Some(l),
                Err(mpsc::TryRecvError::Empty) => {
                    if !batch.is_empty() && flushed.elapsed().as_millis() > 100 {
                        for l in &batch { let _ = w.write_all(l.as_bytes()); }
                        let _ = w.flush();
                        batch.clear();
                        flushed = std::time::Instant::now();
                    }
                    msg_rx.recv().ok()
                }
                Err(mpsc::TryRecvError::Disconnected) => None,
            };

            if let Some(l) = line {
                batch.push(l);
                if batch.len() >= 10 {
                    for l in &batch { let _ = w.write_all(l.as_bytes()); }
                    let _ = w.flush();
                    batch.clear();
                    flushed = std::time::Instant::now();
                }
            } else {
                for l in &batch { let _ = w.write_all(l.as_bytes()); }
                let _ = w.flush();
                break;
            }
        }
    });

    log(STATUS_LOG, "STARTUP", &format!("{} wallet(s) at {}", wallets.len(), now()));
    eprintln!("[{}] Starting copy trading for {} wallet(s)", now(), wallets.len());

    let mut handles = vec![];
    for wallet_addr in wallets {
        let tx = msg_tx.clone();
        let addr = wallet_addr.to_string();
        let client = client.clone();
        let mut delay = RECONNECT_SECS;

        handles.push(tokio::spawn(async move {
            loop {
                match connect_wallet(&addr, tx.clone(), client.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        let n = short_addr(&addr);
                        log(STATUS_LOG, "ERROR", &format!("{}: {}. retry in {}s", n, e, delay));
                        eprintln!("[{}] ✗ {} lost. retry {}s", now(), n, delay);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
                        delay = (delay * 2).min(60);
                    }
                }
            }
        }));
    }

    for h in handles { let _ = h.await; }
    eprintln!("[{}] All connections closed", now());
    Ok(())
}
