//! Most of the comments in [`dydx`](crate::integrations::dydx) also apply here.

use std::{fmt::Display, io};

use futures::{future::Either, stream, Sink, Stream, StreamExt as _, TryStreamExt as _};
use io_extra::IoErrorExt as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

use super::{bail, recv_json, send_json, ExchangeMessage, WsError, WsMessage, WsResult};

/// Input channel should NOT have had messages sent over it...
/// `id` should be e.g `BTC-PERP`.
pub fn protocol<PriceT, QuantityT>(
    s: impl Stream<Item = WsResult<WsMessage>> + Sink<WsMessage, Error = WsError> + Unpin,
    id: impl Display,
) -> impl Stream<Item = WsResult<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    stream::once(_protocol(s, id)).flatten()
}

async fn _protocol<PriceT, QuantityT>(
    mut s: impl Stream<Item = WsResult<WsMessage>> + Sink<WsMessage, Error = WsError> + Unpin,
    id: impl Display,
) -> impl Stream<Item = WsResult<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    if let Err(e) = send_json(
        &mut s,
        json!({"op": "subscribe", "data": [format!("orderbook:{id}")]}),
    )
    .await
    {
        bail!(e)
    };
    match recv_json(&mut s).await {
        Ok(Data {
            data: DataInner::Snapshot { bids, asks },
        }) => match recv_json::<Data<Vec<String>>>(&mut s).await {
            Ok(_spurious) => {
                let initial_snapshot = orders2exchangemessages(bids, asks).map(Ok);
                let remaining_updates = stream::try_unfold(s, |mut it| async move {
                    match recv_json(&mut it).await {
                        Ok(Data {
                            data: DataInner::Update { bids, asks },
                        }) => Ok(Some((
                            Either::Left(orders2exchangemessages(bids, asks).map(Ok)),
                            it,
                        ))),
                        Ok(Data {
                            data: DataInner::Snapshot { .. },
                        }) => Ok(Some((Either::Right(stream::empty()), it))),
                        Err(e) => Err(e),
                    }
                })
                .try_flatten();
                Either::Right(initial_snapshot.chain(remaining_updates))
            }
            Err(e) => bail!(e),
        },
        Ok(_) => bail!(io::Error::invalid_data(r#"expected to receive "snapshot""#)),
        Err(e) => bail!(e),
    }
}

fn orders2exchangemessages<PriceT, QuantityT>(
    bids: Vec<(PriceT, QuantityT)>,
    asks: Vec<(PriceT, QuantityT)>,
) -> impl Stream<Item = ExchangeMessage<PriceT, QuantityT>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    let bids = bids
        .into_iter()
        .map(|(price, quantity)| ExchangeMessage::Buy { price, quantity });
    let asks = asks
        .into_iter()
        .map(|(price, quantity)| ExchangeMessage::Sell { price, quantity });
    stream::iter(bids.chain(asks))
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
struct Data<T> {
    data: T,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
enum DataInner<PriceT, QuantityT> {
    Update {
        bids: Vec<(PriceT, QuantityT)>,
        asks: Vec<(PriceT, QuantityT)>,
    },
    Snapshot {
        bids: Vec<(PriceT, QuantityT)>,
        asks: Vec<(PriceT, QuantityT)>,
    },
}

#[cfg(test)]
mod tests {
    use crate::integrations::{round_trip, u16f16};

    use super::*;

    use serde_json::json;

    #[test]
    fn deser() {
        round_trip(
            Data {
                data: DataInner::Snapshot {
                    bids: vec![
                        (u16f16::lit("123"), u16f16::lit("456")),
                        (u16f16::lit("789"), u16f16::lit("123")),
                    ],
                    asks: vec![],
                },
            },
            json!({"data": {"type": "snapshot", "bids": [["123", "456"], ["789", "123"]], "asks": []}}),
        );
    }
}
