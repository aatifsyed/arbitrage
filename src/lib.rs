use std::{
    collections::{btree_map::Entry as TreeEntry, hash_map::Entry as HashEntry, BTreeMap, HashMap},
    hash::{BuildHasher, Hash, RandomState},
    iter,
};

use itertools::Either;
use num_traits::Zero;

pub mod integrations {
    use std::{io, pin::pin};

    use futures::{future::Either, Sink, SinkExt as _, Stream, TryStreamExt as _};
    use io_extra::IoErrorExt as _;
    use serde::{de::DeserializeOwned, Deserialize, Serialize};

    pub mod aevo;
    pub mod dydx;

    type WsMessage = tungstenite::Message;
    type WsError = tungstenite::Error;
    type WsResult<T> = tungstenite::Result<T>;

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
    pub enum ExchangeMessage<PriceT, QuantityT> {
        Buy { price: PriceT, quantity: QuantityT },
        Sell { price: PriceT, quantity: QuantityT },
    }

    async fn send_json(
        s: impl Sink<WsMessage, Error = WsError>,
        t: impl Serialize,
    ) -> WsResult<()> {
        let msg = serde_json::to_vec(&t).map_err(|e| WsError::Io(io::Error::invalid_input(e)))?;
        pin!(s).send(WsMessage::Binary(msg)).await
    }

    async fn recv_json<T: DeserializeOwned>(
        s: impl Stream<Item = WsResult<WsMessage>>,
    ) -> WsResult<T> {
        let mut s = pin!(s);
        let message = loop {
            match s.try_next().await {
                Ok(Some(WsMessage::Binary(it))) => break Either::Left(it),
                Ok(Some(WsMessage::Text(it))) => break Either::Right(it),
                Ok(Some(WsMessage::Ping(_) | WsMessage::Pong(_))) => continue, // TODO(aatifsyed): do we need to respond to pings manually?
                Ok(Some(WsMessage::Frame(_))) => continue, // TODO(aatifsyed): is this unreachable?
                Ok(Some(WsMessage::Close(_)) | None) => {
                    return Err(WsError::Io(io::Error::unexpected_eof(
                        "underlying stream ended early",
                    )))
                }
                Err(e) => return Err(e),
            };
        };
        deserialize_json(&message)
    }

    fn deserialize_json<'a, T: Deserialize<'a>>(
        src: &'a Either<Vec<u8>, String>, // allow borrowing from the input
    ) -> WsResult<T> {
        match src {
            Either::Left(it) => serde_json::from_slice(it),
            Either::Right(it) => serde_json::from_str(it), // skip UTF-8 validation
        }
        .map_err(|it| WsError::Io(io::Error::invalid_data(it)))
    }

    macro_rules! bail {
        ($expr:expr) => {
            return futures::future::Either::Left(futures::stream::once(futures::future::ready(
                Err($crate::integrations::WsError::from($expr)),
            )))
        };
    }
    pub(crate) use bail;

    #[cfg(test)]
    #[allow(non_camel_case_types)]
    type u16f16 = fixed::FixedU32<typenum::U16>;

    #[cfg(test)]
    #[track_caller]
    fn round_trip<T>(repr: T, json: serde_json::Value)
    where
        T: DeserializeOwned + Serialize + PartialEq + std::fmt::Debug,
    {
        let repr2json = serde_json::to_value(&repr).unwrap();
        assert_eq!(repr2json, json);

        let json2repr = serde_json::from_value(json).unwrap();
        assert_eq!(repr, json2repr);
    }
}

/// Keeps track of arbitrage opportunities across exchanges.
#[derive(Debug, Clone)]
pub struct ArbitrageFinder<QuantityT, PriceT, ExchangeIdT, BuildHasherT = RandomState> {
    #[doc(alias = "buys")]
    bids: BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
    #[doc(alias = "sells")]
    asks: BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Error<ExchangeIdT> {
    /// An exchange needlessly stated that a price level was empty.
    ///
    /// This does not signal that the [`ArbitrageFinder`] is corrupt, but may indicate
    /// incoming data loss.
    Needless { exchange_id: ExchangeIdT },
}

impl<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
    ArbitrageFinder<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
where
    PriceT: Ord + Clone,
    QuantityT: Zero,
    ExchangeIdT: Eq + Hash,
    BuildHasherT: BuildHasher + Default,
{
    #[doc(alias = "bid")]
    pub fn buy(
        &mut self,
        exchange_id: ExchangeIdT,
        price: PriceT,
        quantity: QuantityT,
    ) -> Result<impl Iterator<Item = (&ExchangeIdT, &PriceT, &QuantityT)>, Error<ExchangeIdT>> {
        match quantity.is_zero() {
            false => {
                insert(&mut self.bids, price.clone(), exchange_id, quantity);
                let arbitrages = self
                    .asks
                    .iter() // cheapest first
                    .take_while(move |(ask, _)| *ask < &price)
                    .flat_map(|(ask, xcs)| xcs.iter().map(move |(xc, q)| (xc, ask, q)));
                Ok(Either::Left(arbitrages))
            }
            true => remove_price_from_exchange(&mut self.bids, price, exchange_id)
                .map(Err)
                .unwrap_or(Ok(Either::Right(iter::empty()))),
        }
    }
    #[doc(alias = "ask")]
    pub fn sell(
        &mut self,
        exchange_id: ExchangeIdT,
        price: PriceT,
        quantity: QuantityT,
    ) -> Result<impl Iterator<Item = (&ExchangeIdT, &PriceT, &QuantityT)>, Error<ExchangeIdT>> {
        match quantity.is_zero() {
            false => {
                insert(&mut self.asks, price.clone(), exchange_id, quantity);
                let arbitrages = self
                    .bids
                    .iter()
                    .rev() // most generous first
                    .take_while(move |(bid, _)| *bid > &price)
                    .flat_map(|(bid, xcs)| xcs.iter().map(move |(xc, q)| (xc, bid, q)));
                Ok(Either::Left(arbitrages))
            }
            true => remove_price_from_exchange(&mut self.bids, price, exchange_id)
                .map(Err)
                .unwrap_or(Ok(Either::Right(iter::empty()))),
        }
    }
}

fn insert<QuantityT, PriceT, ExchangeIdT, BuildHasherT>(
    side: &mut BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
    price: PriceT,
    exchange_id: ExchangeIdT,
    quantity: QuantityT,
) where
    PriceT: Ord,
    BuildHasherT: Default + BuildHasher,
    ExchangeIdT: Hash + Eq,
{
    // matching here allows us to `entry(..).and_modify(..).or_insert(..)` without adding a bunch of `Clone` bounds
    match side.entry(price) {
        TreeEntry::Vacant(it) => it
            .insert(HashMap::with_hasher(BuildHasherT::default()))
            .insert(exchange_id, quantity),
        TreeEntry::Occupied(mut it) => it.get_mut().insert(exchange_id, quantity),
    };
}

fn remove_price_from_exchange<QuantityT, PriceT, ExchangeIdT, BuildHasherT>(
    side: &mut BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
    price: PriceT,
    exchange_id: ExchangeIdT,
) -> Option<Error<ExchangeIdT>>
where
    PriceT: Ord,
    BuildHasherT: BuildHasher,
    ExchangeIdT: Eq + Hash,
{
    match side.entry(price) {
        TreeEntry::Vacant(_) => Some(Error::Needless { exchange_id }),
        TreeEntry::Occupied(mut price_level) => {
            let err = match price_level.get_mut().entry(exchange_id) {
                HashEntry::Occupied(exchange) => {
                    exchange.remove();
                    None
                }
                HashEntry::Vacant(exchange) => Some(Error::Needless {
                    exchange_id: exchange.into_key(),
                }),
            };
            if price_level.get().is_empty() {
                price_level.remove();
            }
            err
        }
    }
}

impl<QuantityT, PriceT, ExchangeIdT, BuildHasherT> Default
    for ArbitrageFinder<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
where
    BuildHasherT: Default,
{
    fn default() -> Self {
        Self {
            bids: Default::default(),
            asks: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::*;
    use itertools::assert_equal;

    type Finder<Q, P, E> = ArbitrageFinder<Q, P, E>;

    #[test]
    fn sell_highest_spread_first() {
        let mut arbitrage = Finder::default();
        assert_empty(arbitrage.buy("kraken", 10, 1).unwrap());
        assert_empty(arbitrage.buy("kraken", 20, 1).unwrap());
        assert_empty(arbitrage.buy("kraken", 30, 1).unwrap());
        assert_empty(arbitrage.buy("kraken", 40, 1).unwrap());

        assert_equal(
            arbitrage.sell("binance", 20, 1).unwrap(),
            [(&"kraken", &40, &1), (&"kraken", &30, &1)],
        );
    }

    #[test]
    fn buy_highest_spread_first() {
        let mut arbitrage = Finder::default();
        assert_empty(arbitrage.sell("kraken", 10, 1).unwrap());
        assert_empty(arbitrage.sell("kraken", 20, 1).unwrap());
        assert_empty(arbitrage.sell("kraken", 30, 1).unwrap());
        assert_empty(arbitrage.sell("kraken", 40, 1).unwrap());

        assert_equal(
            arbitrage.buy("binance", 30, 1).unwrap(),
            [(&"kraken", &10, &1), (&"kraken", &20, &1)],
        );
    }

    fn assert_empty<T>(it: impl IntoIterator<Item = T>)
    where
        T: Debug + PartialEq,
    {
        assert_equal(it, iter::empty())
    }
}
