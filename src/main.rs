use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use std::io::Write;
use std::sync::mpsc;

// Wallet addresses to monitor - add your addresses here
const WALLET_ADDRESSES: &[&str] = &[
    "0x50b309f78e774a756a2230e1769729094cac9f20",
    // Add more wallet addresses here
];

// Output file for event dumps
const EVENTS_FILE: &str = "events.log";

/// Event handler - sends events to channel for async file writing
/// Zero-allocation hot path: just sends string slices to channel
/// File I/O happens in background thread, not blocking event processing
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
    // Format event as string - this allocation happens here, not in hot path
    // Channel send is very fast (lock-free queue), only blocks if buffer full
    let line = format!(
        "{}|{}|{}|{}|{}|{}|{}\n",
        wallet,
        event_type,
        coin.unwrap_or("-"),
        side.unwrap_or("-"),
        size.unwrap_or("-"),
        price.unwrap_or("-"),
        time.map(|t| t.to_string()).as_deref().unwrap_or("-")
    );
    // Send to channel - non-blocking if buffer has space (default buffer is large)
    // Ignore errors (receiver dropped) - events will be lost but won't crash
    let _ = tx.send(line);
}

#[tokio::main(flavor = "current_thread")]  // Single-threaded: no context switching overhead
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Validate wallet addresses
    if WALLET_ADDRESSES.is_empty() {
        eprintln!("Error: No wallet addresses configured");
        return Ok(());
    }
    
    // Create channel for offloading file writes from hot path
    // Channel buffer is unbounded in practice (std::sync::mpsc is async channel)
    // Sender only blocks if receiver is slow, but with dedicated thread this is rare
    let (tx, rx) = mpsc::channel::<String>();
    
    // Spawn dedicated thread for file writing (runs in parallel, never blocks hot path)
    // This completely decouples I/O from event processing for minimal latency
    let file_path = EVENTS_FILE.to_string();
    std::thread::spawn(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .expect("Failed to open events file");
        let mut writer = std::io::BufWriter::new(file);
        
        // Batch writes: collect events before flushing to reduce syscalls
        // Smaller batch size (10) ensures more frequent writes for real-time tracking
        let mut batch: Vec<String> = Vec::with_capacity(10);
        let mut last_flush = std::time::Instant::now();
        
        loop {
            // Try to receive with non-blocking check first, then block if needed
            // This allows periodic flushing even when events are sparse
            let line = match rx.try_recv() {
                Ok(line) => Some(line),
                Err(mpsc::TryRecvError::Empty) => {
                    // No events available - check if we need to flush due to timeout
                    if !batch.is_empty() && last_flush.elapsed().as_millis() > 100 {
                        // Flush batch after 100ms timeout
                        for line in &batch {
                            let _ = writer.write_all(line.as_bytes());
                        }
                        let _ = writer.flush();
                        batch.clear();
                        last_flush = std::time::Instant::now();
                    }
                    // Block until next event arrives
                    match rx.recv() {
                        Ok(line) => Some(line),
                        Err(_) => None, // Channel closed
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => None, // Channel closed
            };
            
            if let Some(line) = line {
                batch.push(line);
                // Flush when batch reaches capacity (reduces I/O syscalls)
                if batch.len() >= 10 {
                    for line in &batch {
                        let _ = writer.write_all(line.as_bytes());
                    }
                    let _ = writer.flush();
                    batch.clear();
                    last_flush = std::time::Instant::now();
                }
            } else {
                // Channel closed - flush remaining events and exit
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
    
    // Connect to Hyperliquid WebSocket
    let url = "wss://api.hyperliquid.xyz/ws";
    let (ws_stream, _) = connect_async(url).await?;
    eprintln!("Connected to {}", url);
    
    let (mut write, mut read) = ws_stream.split();
    
    // Subscribe to userEvents for all wallets
    // Send multiple subscribe messages on the same connection
    for wallet in WALLET_ADDRESSES {
        let subscribe_msg = format!(
            r#"{{"method":"subscribe","subscription":{{"type":"userEvents","user":"{}"}}}}"#,
            wallet.to_lowercase()
        );
        write.send(Message::Text(subscribe_msg)).await?;
        eprintln!("Subscribed to userEvents for {}", wallet);
    }
    
    // Message processing loop - optimized for minimal latency
    while let Some(msg) = read.next().await {
        let msg = msg?;
        
        // Only process text messages
        let data = match msg {
            Message::Text(text) => text.into_bytes(),
            Message::Binary(bytes) => bytes,
            _ => continue,
        };
        
        // Parse JSON directly from bytes - avoids string allocation
        // from_slice is faster than from_str as it works directly with bytes
        let json: Value = match serde_json::from_slice(&data) {
            Ok(v) => v,
            Err(_) => continue,  // Skip malformed messages without logging
        };
        
        // Check channel field - using get() avoids allocation
        if let Some(channel) = json.get("channel").and_then(|v| v.as_str()) {
            if channel != "userEvents" {
                continue;
            }
        } else {
            continue;
        }
        
        // Extract wallet/user from data - needed to identify which wallet the event belongs to
        // The userEvents channel includes the user address in the data
        let wallet = json.get("data")
            .and_then(|d| d.get("user"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        // Extract data.events array
        // Using chained get() calls to minimize allocations
        let events = match json.get("data")
            .and_then(|d| d.get("events"))
            .and_then(|e| e.as_array()) 
        {
            Some(arr) => arr,
            None => continue,
        };
        
        // Process each event with zero-allocation field extraction
        for event in events {
            // Extract event type - critical for routing
            let event_type = event.get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            
            // Extract common fields using as_str() to avoid String allocation
            // Fields vary by event type but we attempt to extract all
            let coin = event.get("coin")
                .and_then(|v| v.as_str())
                .or_else(|| event.get("asset").and_then(|v| v.as_str()));
            
            let side = event.get("side")
                .and_then(|v| v.as_str());
            
            // For size/price: use as_str() to get raw string representation
            // Parsing to f64 would add latency; defer to handler if needed
            let size = event.get("sz")
                .and_then(|v| v.as_str())
                .or_else(|| event.get("size").and_then(|v| v.as_str()));
            
            let price = event.get("px")
                .and_then(|v| v.as_str())
                .or_else(|| event.get("price").and_then(|v| v.as_str()));
            
            let time = event.get("time")
                .and_then(|v| v.as_i64())
                .or_else(|| event.get("timestamp").and_then(|v| v.as_i64()));
            
            // Inline handler call - channel send is lock-free and non-blocking
            // File I/O happens in background, zero latency impact on hot path
            handle_event(&tx, wallet, event_type, coin, side, size, price, time);
        }
    }
    
    Ok(())
}

