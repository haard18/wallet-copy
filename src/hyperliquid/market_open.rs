use ethers::signers::{LocalWallet, Signer};
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient, ExchangeResponseStatus, MarketOrderParams};

pub async fn market_open(
    client: &ExchangeClient,
    asset: &str,
    is_buy: bool,
    sz: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let params = MarketOrderParams {
        asset,
        is_buy,
        sz,
        px: None,
        slippage: Some(0.01),
        cloid: None,
        wallet: None,
    };

    let response = client.market_open(params).await?;
    match response {
        ExchangeResponseStatus::Ok(_) => {
            eprintln!("OPEN|{}|{}|{}", asset, if is_buy { "B" } else { "A" }, sz);
        }
        ExchangeResponseStatus::Err(e) => {
            eprintln!("OPEN_ERR|{}", e);
        }
    }
    Ok(())
}

pub async fn create_client() -> Result<ExchangeClient, Box<dyn std::error::Error + Send + Sync>> {
    let pk = std::env::var("PRIVATE_KEY")?;
    let wallet: LocalWallet = pk.parse()?;
    println!("Using wallet address: {}", wallet.address());
    let client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None).await?;
    Ok(client)
}
