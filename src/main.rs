mod hyperliquid;

use hyperliquid::watch::watch_and_copy;

const WALLET_ADDRESSES: &[&str] = &[
    "0x50b309f78e774a756a2230e1769729094cac9f20",
    "0x162cc7c861ebd0c06b3d72319201150482518185",
    "0xf28e1b06e00e8774c612e31ab3ac35d5a720085f",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    watch_and_copy(WALLET_ADDRESSES).await?;
    Ok(())
}
