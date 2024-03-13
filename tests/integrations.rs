use futures::{StreamExt as _, TryStreamExt as _};
use openhedge_arbitrage::integrations::dydx::protocol;

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[tokio::test]
async fn test() {
    let (st, _http) = tokio_tungstenite::connect_async("wss://indexer.dydx.trade/v4/ws")
        .await
        .unwrap();
    let latest = protocol::<u32f32, u32f32>(st, "BTC-USD")
        .await
        .take(10)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    dbg!(latest);
}
