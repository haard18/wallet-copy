use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use std::io::Write;
use std::sync::mpsc;

// Wallet addresses to monitor - add your addresses here
const WALLET_ADDRESSES: &[&str] = &[
    "0x50b309f78e774a756a2230e1769729094cac9f20",
    "0x162cc7c861ebd0c06b3d72319201150482518185",
    "0xf28e1b06e00e8774c612e31ab3ac35d5a720085f"
    // Add more wallet addresses here
];

// Output files
const EVENTS_FILE: &str = "events.log";      // Processed events (userEvents + orderUpdates)
const MESSAGES_FILE: &str = "messages.log";  // All raw WebSocket messages
const STATUS_FILE: &str = "status.log";

// Heartbeat interval (seconds) - log status to verify active listening
const HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// Statistics tracking for monitoring
struct Stats {
    messages_received: u64,
    events_processed: u64,
    orders_processed: u64,
    last_message_time: std::time::Instant,
    last_heartbeat: std::time::Instant,
    connection_start: std::time::Instant,
}

/// Log status messages to file (non-blocking, errors ignored)
fn log_status(file_path: &str, level: &str, message: &str) {
    use std::io::Write;
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
    {
        let _ = writeln!(
            file,
            "{}|{}|{}",
            get_timestamp(),
            level,
            message
        );
    }
}

/// Get current timestamp as Unix timestamp (seconds since epoch)
fn get_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Event handler - sends events to channel for async file writing
#[inline(always)]
fn handle_event(
    tx: &mpsc::Sender<String>,
    wallet: &str,
    event_type: &str,
    coin: Option<&str>,
    side: Option<&str>,
    size: Option<&str>,
    price: Option<&str>,
    time: Option<i64>,
) {
    let received_ts = get_timestamp();
    let line = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}\n",
        received_ts,
        wallet,
        event_type,
        coin.unwrap_or("-"),
        side.unwrap_or("-"),
        size.unwrap_or("-"),
        price.unwrap_or("-"),
        time.map(|t| t.to_string()).as_deref().unwrap_or("-")
    );
    let _ = tx.send(line);
}

/// Order handler - sends order updates to channel for async file writing
#[inline(always)]
fn handle_order(
    tx: &mpsc::Sender<String>,
    wallet: &str,
    coin: &str,
    side: &str,
    limit_px: &str,
    sz: &str,
    oid: i64,
    status: &str,
    timestamp: i64,
    status_timestamp: i64,
) {
    let received_ts = get_timestamp();
    let line = format!(
        "{}|{}|ORDER|{}|{}|{}|{}|{}|{}|{}\n",
        received_ts,
        wallet,
        coin,
        side,
        sz,
        limit_px,
        oid,
        status,
        status_timestamp
    );
    let _ = tx.send(line);
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Validate wallet addresses
    if WALLET_ADDRESSES.is_empty() {
        eprintln!("Error: No wallet addresses configured");
        return Ok(());
    }
    
    // Create channels for offloading file writes
    let (tx_events, rx_events) = mpsc::channel::<String>();
    let (tx_messages, rx_messages) = mpsc::channel::<String>();
    
    // Spawn dedicated thread for events file writing
    let events_file_path = EVENTS_FILE.to_string();
    std::thread::spawn(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&events_file_path)
            .expect("Failed to open events file");
        let mut writer = std::io::BufWriter::new(file);
        
        let mut batch: Vec<String> = Vec::with_capacity(10);
        let mut last_flush = std::time::Instant::now();
        
        loop {
            let line = match rx_events.try_recv() {
                Ok(line) => Some(line),
                Err(mpsc::TryRecvError::Empty) => {
                    if !batch.is_empty() && last_flush.elapsed().as_millis() > 100 {
                        for line in &batch {
                            let _ = writer.write_all(line.as_bytes());
                        }
                        let _ = writer.flush();
                        batch.clear();
                        last_flush = std::time::Instant::now();
                    }
                    match rx_events.recv() {
                        Ok(line) => Some(line),
                        Err(_) => None,
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => None,
            };
            
            if let Some(line) = line {
                batch.push(line);
                if batch.len() >= 10 {
                    for line in &batch {
                        let _ = writer.write_all(line.as_bytes());
                    }
                    let _ = writer.flush();
                    batch.clear();
                    last_flush = std::time::Instant::now();
                }
            } else {
                if !batch.is_empty() {
                    for line in &batch {
                        let _ = writer.write_all(line.as_bytes());
                    }
                    let _ = writer.flush();
                }
                break;
            }
        }
    });
    
    // Spawn dedicated thread for raw messages file writing
    let messages_file_path = MESSAGES_FILE.to_string();
    std::thread::spawn(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&messages_file_path)
            .expect("Failed to open messages file");
        let mut writer = std::io::BufWriter::new(file);
        
        let mut batch: Vec<String> = Vec::with_capacity(10);
        let mut last_flush = std::time::Instant::now();
        
        loop {
            let line = match rx_messages.try_recv() {
                Ok(line) => Some(line),
                Err(mpsc::TryRecvError::Empty) => {
                    if !batch.is_empty() && last_flush.elapsed().as_millis() > 100 {
                        for line in &batch {
                            let _ = writer.write_all(line.as_bytes());
                        }
                        let _ = writer.flush();
                        batch.clear();
                        last_flush = std::time::Instant::now();
                    }
                    match rx_messages.recv() {
                        Ok(line) => Some(line),
                        Err(_) => None,
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => None,
            };
            
            if let Some(line) = line {
                batch.push(line);
                if batch.len() >= 10 {
                    for line in &batch {
                        let _ = writer.write_all(line.as_bytes());
                    }
                    let _ = writer.flush();
                    batch.clear();
                    last_flush = std::time::Instant::now();
                }
            } else {
                if !batch.is_empty() {
                    for line in &batch {
                        let _ = writer.write_all(line.as_bytes());
                    }
                    let _ = writer.flush();
                }
                break;
            }
        }
    });
    
    // Statistics tracking
    let mut stats = Stats {
        messages_received: 0,
        events_processed: 0,
        orders_processed: 0,
        last_message_time: std::time::Instant::now(),
        last_heartbeat: std::time::Instant::now(),
        connection_start: std::time::Instant::now(),
    };
    
    // Log startup
    log_status(STATUS_FILE, "STARTUP", &format!(
        "Starting wallet monitor for {} wallet(s) at {}",
        WALLET_ADDRESSES.len(),
        get_timestamp()
    ));
    eprintln!("[{}] Starting wallet monitor for {} wallet(s)", get_timestamp(), WALLET_ADDRESSES.len());
    
    // Connect to Hyperliquid WebSocket
    let url = "wss://api.hyperliquid.xyz/ws";
    log_status(STATUS_FILE, "CONNECT", &format!("Connecting to {}", url));
    eprintln!("[{}] Connecting to {}...", get_timestamp(), url);
    
    let (ws_stream, _) = match connect_async(url).await {
        Ok(stream) => {
            log_status(STATUS_FILE, "CONNECTED", &format!("Successfully connected to {}", url));
            eprintln!("[{}] ✓ Connected to {}", get_timestamp(), url);
            stream
        }
        Err(e) => {
            log_status(STATUS_FILE, "ERROR", &format!("Connection failed: {}", e));
            eprintln!("[{}] ✗ Connection failed: {}", get_timestamp(), e);
            return Err(e.into());
        }
    };
    
    let (mut write, mut read) = ws_stream.split();
    
    // Subscribe to BOTH userEvents AND orderUpdates for all wallets
    log_status(STATUS_FILE, "SUBSCRIBE", &format!("Subscribing to {} wallet(s)", WALLET_ADDRESSES.len()));
    for wallet in WALLET_ADDRESSES {
        // Subscribe to userEvents (fills, funding, liquidations)
        let subscribe_events = format!(
            r#"{{"method":"subscribe","subscription":{{"type":"userEvents","user":"{}"}}}}"#,
            wallet.to_lowercase()
        );
        match write.send(Message::Text(subscribe_events)).await {
            Ok(_) => {
                log_status(STATUS_FILE, "SUBSCRIBED", &format!("Subscribed to userEvents for {}", wallet));
                eprintln!("[{}] ✓ Subscribed to userEvents for {}", get_timestamp(), wallet);
            }
            Err(e) => {
                log_status(STATUS_FILE, "ERROR", &format!("userEvents subscription failed for {}: {}", wallet, e));
                eprintln!("[{}] ✗ userEvents subscription failed for {}: {}", get_timestamp(), wallet, e);
            }
        }
        
        // Subscribe to orderUpdates (order status changes)
        let subscribe_orders = format!(
            r#"{{"method":"subscribe","subscription":{{"type":"orderUpdates","user":"{}"}}}}"#,
            wallet.to_lowercase()
        );
        match write.send(Message::Text(subscribe_orders)).await {
            Ok(_) => {
                log_status(STATUS_FILE, "SUBSCRIBED", &format!("Subscribed to orderUpdates for {}", wallet));
                eprintln!("[{}] ✓ Subscribed to orderUpdates for {}", get_timestamp(), wallet);
            }
            Err(e) => {
                log_status(STATUS_FILE, "ERROR", &format!("orderUpdates subscription failed for {}: {}", wallet, e));
                eprintln!("[{}] ✗ orderUpdates subscription failed for {}: {}", get_timestamp(), wallet, e);
            }
        }
    }
    
    // Message processing loop
    while let Some(msg) = read.next().await {
        stats.messages_received += 1;
        stats.last_message_time = std::time::Instant::now();
        
        // Periodic heartbeat
        if stats.last_heartbeat.elapsed().as_secs() >= HEARTBEAT_INTERVAL_SECS {
            let uptime = stats.connection_start.elapsed().as_secs();
            let time_since_last_msg = stats.last_message_time.elapsed().as_secs();
            let heartbeat_msg = format!(
                "HEARTBEAT: uptime={}s, messages={}, events={}, orders={}, last_msg={}s ago",
                uptime,
                stats.messages_received,
                stats.events_processed,
                stats.orders_processed,
                time_since_last_msg
            );
            log_status(STATUS_FILE, "HEARTBEAT", &heartbeat_msg);
            eprintln!("[{}] {}", get_timestamp(), heartbeat_msg);
            stats.last_heartbeat = std::time::Instant::now();
        }
        
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                log_status(STATUS_FILE, "ERROR", &format!("WebSocket error: {}", e));
                eprintln!("[{}] ✗ WebSocket error: {}", get_timestamp(), e);
                continue;
            }
        };
        
        // Log all messages to messages.log
        let raw_data = match &msg {
            Message::Text(text) => {
                let timestamp = get_timestamp();
                let log_line = format!("{}|TEXT|{}\n", timestamp, text);
                let _ = tx_messages.send(log_line);
                text.as_bytes().to_vec()
            }
            Message::Binary(bytes) => {
                let timestamp = get_timestamp();
                let hex_repr = bytes.iter().take(100).map(|b| format!("{:02x}", b)).collect::<String>();
                let log_line = format!("{}|BINARY|{} ({} bytes)\n", timestamp, hex_repr, bytes.len());
                let _ = tx_messages.send(log_line);
                bytes.clone()
            }
            Message::Ping(_) => {
                let _ = tx_messages.send(format!("{}|PING|\n", get_timestamp()));
                continue;
            }
            Message::Pong(_) => {
                let _ = tx_messages.send(format!("{}|PONG|\n", get_timestamp()));
                continue;
            }
            Message::Close(_) => {
                let _ = tx_messages.send(format!("{}|CLOSE|\n", get_timestamp()));
                break;
            }
            Message::Frame(_) => {
                let _ = tx_messages.send(format!("{}|FRAME|\n", get_timestamp()));
                continue;
            }
        };
        
        // Parse JSON
        let json: Value = match serde_json::from_slice(&raw_data) {
            Ok(v) => v,
            Err(e) => {
                log_status(STATUS_FILE, "WARN", &format!("JSON parse error: {}", e));
                continue;
            }
        };
        
        // Check channel field
        let channel = match json.get("channel").and_then(|v| v.as_str()) {
            Some(c) => c,
            None => {
                let timestamp = get_timestamp();
                let _ = tx_messages.send(format!("{}|NO_CHANNEL|\n", timestamp));
                continue;
            }
        };
        
        // Process userEvents channel
        if channel == "userEvents" {
            let wallet = json.get("data")
                .and_then(|d| d.get("user"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            
            // userEvents can contain: fills, funding, liquidation, or nonUserCancel
            let data = match json.get("data") {
                Some(d) => d,
                None => continue,
            };
            
            // Handle fills
            if let Some(fills) = data.get("fills").and_then(|f| f.as_array()) {
                for fill in fills {
                    let coin = fill.get("coin").and_then(|v| v.as_str());
                    let side = fill.get("side").and_then(|v| v.as_str());
                    let size = fill.get("sz").and_then(|v| v.as_str());
                    let price = fill.get("px").and_then(|v| v.as_str());
                    let time = fill.get("time").and_then(|v| v.as_i64());
                    
                    handle_event(&tx_events, wallet, "fill", coin, side, size, price, time);
                    stats.events_processed += 1;
                }
            }
            
            // Handle funding
            if let Some(funding) = data.get("funding") {
                let coin = funding.get("coin").and_then(|v| v.as_str());
                let time = funding.get("time").and_then(|v| v.as_i64());
                
                handle_event(&tx_events, wallet, "funding", coin, None, None, None, time);
                stats.events_processed += 1;
            }
            
            // Handle liquidation
            if let Some(_liquidation) = data.get("liquidation") {
                handle_event(&tx_events, wallet, "liquidation", None, None, None, None, None);
                stats.events_processed += 1;
            }
            
            // Handle nonUserCancel
            if let Some(cancels) = data.get("nonUserCancel").and_then(|c| c.as_array()) {
                for cancel in cancels {
                    let coin = cancel.get("coin").and_then(|v| v.as_str());
                    handle_event(&tx_events, wallet, "nonUserCancel", coin, None, None, None, None);
                    stats.events_processed += 1;
                }
            }
        }
        
        // Process orderUpdates channel
        else if channel == "orderUpdates" {
            let wallet = json.get("data")
                .and_then(|d| d.get("user"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            
            let orders = match json.get("data")
                .and_then(|d| d.as_array())
            {
                Some(arr) => arr,
                None => continue,
            };
            
            for order_update in orders {
                let order = match order_update.get("order") {
                    Some(o) => o,
                    None => continue,
                };
                
                let coin = order.get("coin").and_then(|v| v.as_str()).unwrap_or("-");
                let side = order.get("side").and_then(|v| v.as_str()).unwrap_or("-");
                let limit_px = order.get("limitPx").and_then(|v| v.as_str()).unwrap_or("-");
                let sz = order.get("sz").and_then(|v| v.as_str()).unwrap_or("-");
                let oid = order.get("oid").and_then(|v| v.as_i64()).unwrap_or(0);
                let timestamp = order.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                
                let status = order_update.get("status").and_then(|v| v.as_str()).unwrap_or("-");
                let status_timestamp = order_update.get("statusTimestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                
                handle_order(&tx_events, wallet, coin, side, limit_px, sz, oid, status, timestamp, status_timestamp);
                stats.orders_processed += 1;
            }
        }
    }
    
    // Connection closed
    log_status(STATUS_FILE, "DISCONNECTED", &format!(
        "Connection closed. Total: {} messages, {} events, {} orders processed",
        stats.messages_received,
        stats.events_processed,
        stats.orders_processed
    ));
    eprintln!("[{}] Connection closed. Processed {} messages, {} events, {} orders", 
        get_timestamp(), stats.messages_received, stats.events_processed, stats.orders_processed);
    
    Ok(())
}