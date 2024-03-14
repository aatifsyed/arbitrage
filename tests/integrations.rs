use std::{fmt::Debug, pin::pin};

use futures::{Stream, StreamExt as _};
use num_traits::Zero;
use openhedge_arbitrage::integrations::{self, ExchangeMessage};

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[tokio::test]
async fn dydx() {
    test(integrations::dydx::<u32f32, u32f32>("BTC-USD")).await;
}

#[tokio::test]
async fn aevo() {
    test(integrations::aevo::<u32f32, u32f32>("BTC-PERP")).await;
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
