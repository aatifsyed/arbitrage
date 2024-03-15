use std::{cmp, pin::pin};

use clap::Parser;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use openhedge_arbitrage::{
    integrations::{aevo, dydx, ExchangeMessage},
    ArbitrageFinder,
};
use tracing::{error, info, trace};

#[allow(non_camel_case_types)]
type u32f32 = fixed::FixedU64<typenum::U32>;

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
enum Exchange {
    Aevo,
    Dydx,
}

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    /// Omit `TRACE` logs
    quiet: bool,
}

#[tokio::main]
async fn main() {
    use tracing_subscriber::{
        filter::{filter_fn, LevelFilter},
        layer::SubscriberExt as _,
        util::SubscriberInitExt as _,
    };
    let Args { quiet } = Args::parse();
    tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(match quiet {
            true => LevelFilter::INFO,
            false => LevelFilter::TRACE,
        })
        .without_time()
        .with_target(false)
        .finish()
        .with(filter_fn(|it| {
            it.file().is_some_and(|file| file == file!())
        }))
        .init();

    _main().await
}

async fn _main() {
    let mut finder = ArbitrageFinder::<_, _, _>::default();
    let mut messages = pin!(stream::select(
        aevo::<u32f32, u32f32>("BTC-PERP").map_ok(|it| (Exchange::Aevo, it)),
        dydx("BTC-USD").map_ok(|it| (Exchange::Dydx, it))
    ));
    let mut balance = u32f32::ZERO;
    loop {
        let msg = messages.next().await;
        if let Some(Ok((exchange, message))) = &msg {
            let (side, price, quantity) = match message {
                ExchangeMessage::Buy { price, quantity } => ("buy", price, quantity),
                ExchangeMessage::Sell { price, quantity } => ("sell", price, quantity),
            };
            trace!(?exchange, %side, %price, %quantity, "received message");
        }
        match msg {
            None => unreachable!("stream always errors before terminating"),
            Some(Ok((exchange_id, msg))) => match msg {
                ExchangeMessage::Buy { price, quantity } => {
                    if let Ok(Some((sell_exchange, sell_price, sell_quantity))) = finder
                        .buy(exchange_id, price, quantity)
                        .map(|mut it| it.next())
                    {
                        let spread = price - sell_price;
                        let quantity = cmp::min(quantity, *sell_quantity);
                        balance += spread * quantity;
                        info!(%spread, %quantity, new_balance = %balance, buy = ?exchange_id, sell = ?sell_exchange, "simulated arbitrage");
                    };
                }
                ExchangeMessage::Sell { price, quantity } => {
                    if let Ok(Some((buy_exchange, buy_price, buy_quantity))) = finder
                        .sell(exchange_id, price, quantity)
                        .map(|mut it| it.next())
                    {
                        let spread = buy_price - price;
                        let quantity = cmp::min(quantity, *buy_quantity);
                        balance += spread * quantity;
                        info!(%spread, %quantity, new_balance = %balance, sell = ?exchange_id, buy = ?buy_exchange, "simulated arbitrage");
                    };
                }
            },
            Some(Err(e)) => {
                error!(?e);
                return;
            }
        }
    }
}
