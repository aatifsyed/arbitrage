use std::{
    collections::{btree_map::Entry as TreeEntry, hash_map::Entry as HashEntry, BTreeMap, HashMap},
    hash::{BuildHasher, Hash, RandomState},
    iter,
};

use itertools::Either;
use num_traits::Zero;

/// Keeps track of arbitrage opportunities across exchanges.
#[derive(Debug, Clone)]
pub struct Arbitrage<QuantityT, PriceT, ExchangeIdT, BuildHasherT = RandomState> {
    #[doc(alias = "buys")]
    bids: BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
    #[doc(alias = "sells")]
    asks: BTreeMap<PriceT, HashMap<ExchangeIdT, QuantityT, BuildHasherT>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Error<ExchangeIdT> {
    /// An exchange needlessly stated that a price level was empty.
    ///
    /// This does not signal that the [`Arbitrage`] is corrupt, but may indicate
    /// incoming data loss.
    Needless { exchange_id: ExchangeIdT },
}

impl<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
    Arbitrage<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
{
    #[doc(alias = "bid")]
    pub fn buy(
        &mut self,
        exchange_id: ExchangeIdT,
        price: PriceT,
        quantity: QuantityT,
    ) -> Result<impl Iterator<Item = (&PriceT, &ExchangeIdT, &QuantityT)>, Error<ExchangeIdT>>
    where
        PriceT: Ord + Clone,
        QuantityT: Zero,
        ExchangeIdT: Eq + Hash,
        BuildHasherT: BuildHasher + Default,
    {
        match quantity.is_zero() {
            false => {
                insert(&mut self.bids, price.clone(), exchange_id, quantity);
                let arbitrages = self
                    .asks
                    .iter() // cheapest first
                    .take_while(move |(ask, _)| *ask < &price)
                    .flat_map(|(ask, xcs)| xcs.iter().map(move |(xc, q)| (ask, xc, q)));
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
    ) -> Result<impl Iterator<Item = (&PriceT, &ExchangeIdT, &QuantityT)>, Error<ExchangeIdT>>
    where
        PriceT: Ord + Clone,
        QuantityT: Zero,
        ExchangeIdT: Eq + Hash,
        BuildHasherT: BuildHasher + Default,
    {
        match quantity.is_zero() {
            false => {
                insert(&mut self.asks, price.clone(), exchange_id, quantity);
                let arbitrages = self
                    .bids
                    .iter()
                    .rev() // most generous first
                    .take_while(move |(bid, _)| *bid > &price)
                    .flat_map(|(bid, xcs)| xcs.iter().map(move |(xc, q)| (bid, xc, q)));
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
    for Arbitrage<QuantityT, PriceT, ExchangeIdT, BuildHasherT>
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
mod tests {}
