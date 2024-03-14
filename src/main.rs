use std::pin::pin;

use futures::{stream, StreamExt, TryStreamExt};
use openhedge_arbitrage::{
    integrations::{aevo, dydx, ExchangeMessage},
    ArbitrageFinder,
};

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
enum Exchange {
    Aevo,
    Dydx,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    _main().await
}

async fn _main() {
    let mut finder = ArbitrageFinder::<_, _, _>::default();
    let mut messages = pin!(stream::select(
        aevo::<u32f32, u32f32>("BTC-PERP").map_ok(|it| (Exchange::Aevo, it)),
        dydx("BTC-USD").map_ok(|it| (Exchange::Dydx, it))
    ));
    loop {
        match messages.next().await {
            None => unreachable!("stream always errors before terminating"),
            Some(Ok((exchange_id, msg))) => match msg {
                ExchangeMessage::Buy { price, quantity } => {
                    let _ = finder.buy(exchange_id, price, quantity);
                }
                ExchangeMessage::Sell { price, quantity } => {
                    let _ = finder.sell(exchange_id, price, quantity);
                }
            },
            Some(Err(_)) => todo!(),
        }
    }
}
