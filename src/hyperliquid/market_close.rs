use hyperliquid_rust_sdk::{ExchangeClient, ExchangeResponseStatus, MarketCloseParams};

pub async fn market_close(
    client: &ExchangeClient,
    asset: &str,
    sz: Option<f64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let params = MarketCloseParams {
        asset,
        sz,
        px: None,
        slippage: Some(0.01),
        cloid: None,
        wallet: None,
    };

    let response = client.market_close(params).await?;
    match response {
        ExchangeResponseStatus::Ok(_) => {
            eprintln!("CLOSE|{}|{:?}", asset, sz);
        }
        ExchangeResponseStatus::Err(e) => {
            eprintln!("CLOSE_ERR|{}", e);
        }
    }
    Ok(())
}
