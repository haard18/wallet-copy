use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};

// Replace with actual wallet address
const WALLET_ADDRESS: &str = "0x50b309f78e774a756a2230e1769729094cac9f20";

/// Event handler - inlined to avoid function call overhead
/// Uses zero-allocation string slices from the JSON
#[inline(always)]
fn handle_event(
    _event_type: &str,
    _coin: Option<&str>,
    _side: Option<&str>,
    _size: Option<&str>,
    _price: Option<&str>,
    _time: Option<i64>,
) {
    // In production: forward to trading logic, write to shared memory, etc.
    // For now: verify we can extract fields (commented out to avoid I/O in hot path)
    // eprintln!("Event: {} | Coin: {:?} | Side: {:?} | Size: {:?} | Price: {:?} | Time: {:?}",
    //     event_type, coin, side, size, price, time);
    
    // Example: actual trading logic would go here
    // match event_type {
    //     "fill" => process_fill(coin, side, size, price, time),
    //     "order" => process_order(...),
    //     _ => {}
    // }
}

#[tokio::main(flavor = "current_thread")]  // Single-threaded: no context switching overhead
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Hyperliquid WebSocket
    let url = "wss://api.hyperliquid.xyz/ws";
    let (ws_stream, _) = connect_async(url).await?;
    eprintln!("Connected to {}", url);
    
    let (mut write, mut read) = ws_stream.split();
    
    // Subscribe to userEvents immediately
    // JSON constructed as static string to avoid allocation
    let subscribe_msg = format!(
        r#"{{"method":"subscribe","subscription":{{"type":"userEvents","user":"{}"}}}}"#,
        WALLET_ADDRESS.to_lowercase()
    );
    write.send(Message::Text(subscribe_msg)).await?;
    eprintln!("Subscribed to userEvents for {}", WALLET_ADDRESS);
    
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
            
            // Inline handler call - compiler should optimize this completely
            handle_event(event_type, coin, side, size, price, time);
        }
    }
    
    Ok(())
}

