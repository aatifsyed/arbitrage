use std::{fmt::Debug, pin::pin};

use futures::{stream, Stream, StreamExt as _, TryStreamExt as _};
use num_traits::Zero;
use openhedge_arbitrage::integrations::{aevo, dydx, ExchangeMessage};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::client::IntoClientRequest;

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[tokio::test]
async fn dydx() {
    test(connect_websocket("wss://indexer.dydx.trade/v4/ws", |it| {
        dydx::protocol::<u32f32, u32f32>(it, "BTC-USD")
    }))
    .await;
}

#[tokio::test]
async fn aevo() {
    test(connect_websocket("wss://ws.aevo.xyz", |it| {
        aevo::protocol::<u32f32, u32f32>(it, "BTC-PERP")
    }))
    .await;
}

fn connect_websocket<F, S, T>(
    to: impl IntoClientRequest + Unpin, // TODO(aatifsyed): make a PR to tokio-tungstenite to relax `Unpin` bound,
    mut f: F,
) -> impl Stream<Item = Result<T, tungstenite::Error>>
where
    F: FnMut(WebSocketStream<MaybeTlsStream<TcpStream>>) -> S,
    S: Stream<Item = Result<T, tungstenite::Error>>,
{
    stream::once(tokio_tungstenite::connect_async(to))
        .map_ok(move |(st, _http)| f(st))
        .try_flatten()
}

async fn test<PriceT, QuantityT, E>(
    s: impl Stream<Item = Result<ExchangeMessage<PriceT, QuantityT>, E>>,
) where
    E: Debug,
    QuantityT: Zero,
{
    let mut seen_fulfilled_buy = false;
    let mut seen_fulfilled_sell = false;
    let mut s = pin!(s);

    while !(seen_fulfilled_buy && seen_fulfilled_sell) {
        match s.next().await.unwrap().unwrap() {
            ExchangeMessage::Buy { quantity, .. } if quantity.is_zero() => {
                seen_fulfilled_buy = true
            }
            ExchangeMessage::Sell { quantity, .. } if quantity.is_zero() => {
                seen_fulfilled_sell = true
            }
            _ => {}
        }
    }
}
