use futures::{StreamExt as _, TryStreamExt as _};
use openhedge_arbitrage::integrations::dydx;

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[tokio::test]
async fn dydx() {
    let (st, _http) = tokio_tungstenite::connect_async("wss://indexer.dydx.trade/v4/ws")
        .await
        .unwrap();
    let latest = dydx::protocol::<u32f32, u32f32>(st, "BTC-USD")
        .take(10)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    dbg!(latest);
}
