use std::{cmp, pin::pin};

use clap::Parser;
use futures::{stream, StreamExt as _};
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
    /// Omit `TRACE` logs
    #[arg(short, long)]
    quiet: bool,
    /// Don't stop at the first error - log and continue.
    #[arg(short, long)]
    r#continue: bool,
}

#[tokio::main]
async fn main() {
    use tracing_subscriber::{
        filter::{filter_fn, LevelFilter},
        layer::SubscriberExt as _,
        util::SubscriberInitExt as _,
    };
    let Args { quiet, r#continue } = Args::parse();
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

    _main(r#continue).await
}

async fn _main(no_fail_fast: bool) {
    let mut finder = ArbitrageFinder::<_, _, _>::default();
    let mut messages = pin!(stream::select(
        aevo::<u32f32, u32f32>("BTC-PERP").map(|it| (Exchange::Aevo, it)),
        dydx("BTC-USD").map(|it| (Exchange::Dydx, it))
    ));
    let mut balance = u32f32::ZERO;
    loop {
        let Some((src, msg)) = messages.next().await else {
            error!("both streams terminated, exiting application");
            std::process::exit(1);
        };

        let msg = match msg {
            Ok(msg) => {
                trace!(?src, ?msg, "received message");
                msg
            }
            Err(error) => {
                // Aevo seems to randomly set huge prices to zero, giving us a parse error.
                // e.g: "bids":[["115792089237316200000000000000000000000000000000000000000000000000000000","0"]]
                error!(?src, %error);
                match no_fail_fast {
                    true => continue,
                    false => std::process::exit(1),
                }
            }
        };

        // NOTE: we don't actually do any trading here, so we might farm a given arbitrage opportunity twice.
        //       but this is just a demo...
        match msg {
            ExchangeMessage::Buy { price, quantity } => {
                if let Ok(Some((sell_exchange, sell_price, sell_quantity))) =
                    finder.buy(src, price, quantity).map(|mut it| it.next())
                {
                    let spread = price - sell_price;
                    let quantity = cmp::min(quantity, *sell_quantity);
                    balance += spread * quantity;
                    info!(new_balance = %balance, %spread, %quantity, buy = ?src, sell = ?sell_exchange, "simulated arbitrage");
                };
            }
            ExchangeMessage::Sell { price, quantity } => {
                if let Ok(Some((buy_exchange, buy_price, buy_quantity))) =
                    finder.sell(src, price, quantity).map(|mut it| it.next())
                {
                    let spread = buy_price - price;
                    let quantity = cmp::min(quantity, *buy_quantity);
                    balance += spread * quantity;
                    info!(new_balance = %balance, %spread, %quantity, sell = ?src, buy = ?buy_exchange, "simulated arbitrage");
                };
            }
        }
    }
}
