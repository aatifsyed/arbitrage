//! Note the API docs are wrong.
//! E.g in [channel data](https://docs.dydx.exchange/developers/indexer/indexer_websocket#channel-data-1)
//! there is no `clobPairId` field in reality.

use std::io;

use futures::{future::Either, stream, Sink, Stream, StreamExt as _, TryStreamExt as _};
use io_extra::IoErrorExt as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

use super::{bail, recv_json, send_json, ExchangeMessage, WsError, WsMessage, WsResult};

/// Input channel should NOT have had messages sent over it...
/// `id` should be e.g `BTC-USD`.
pub fn protocol<PriceT, QuantityT>(
    // TODO(aatifsyed): could relax `Unpin` bound if we were willing to `Box::pin`
    //                  in `stream::try_unfold`.
    //                  This would also allow us to return an `Unpin` stream...
    s: impl Stream<Item = WsResult<WsMessage>> + Sink<WsMessage, Error = WsError> + Unpin,
    id: impl Into<String>,
) -> impl Stream<Item = WsResult<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    // return a bare stream rather than a future resolving to a stream...
    stream::once(_protocol(s, id.into())).flatten()
}

/// ```text
///           ┌────────┐          ┌───┐          
///           │exchange│          │bot│          
///           └───┬────┘          └─┬─┘          
///               │    connected    │            
///               │ ───────────────>│            
///               │                 │            
///               │    subscribe    │            
///               │ <───────────────│            
///               │                 │            
///               │    snapshot     │            
///               │ ───────────────>│            
///               │                 │            
///               │                 │            
/// ╔═══════╤═════╪═════════════════╪═══════════╗
/// ║ LOOP  │  infinite             │           ║
/// ╟───────┘     │                 │           ║
/// ║             │     update      │           ║
/// ║             │  ─ ─ ─ ─ ─ ─ ─ >│           ║
/// ╚═════════════╪═════════════════╪═══════════╝
///           ┌───┴────┐          ┌─┴─┐          
///           │exchange│          │bot│          
///           └────────┘          └───┘          
/// ```
/// <https://www.plantuml.com/plantuml/uml/POv12WD120Jlli8Fv0Dx2FiLnp5POQF3wa2U7tCOSeW7eRkheVT8kdA-Jf0t7sHFmTiTc-U6x6R2AHrAVjr5R1Yp1L_QvByLHYCEJpZT1wezr3G5iEx7BdYEJXMATTZhrOmF>
// TODO(aatifsyed): could check other invariants on borrowed messages...:
// - channel id doesn't change
// - sequence number is monotonic...
async fn _protocol<PriceT, QuantityT>(
    mut s: impl Stream<Item = WsResult<WsMessage>> + Sink<WsMessage, Error = WsError> + Unpin,
    id: String,
) -> impl Stream<Item = WsResult<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    match recv_json::<Message<(), ()>>(&mut s).await {
        Ok(Message::Connected) => {}
        Ok(_) => bail!(io::Error::invalid_data(
            r#"expected to receive "connected""#
        )),
        Err(e) => bail!(e),
    }
    if let Err(e) = send_json(
        &mut s,
        json!({"type": "subscribe", "channel": "v4_orderbook", "id": id}),
    )
    .await
    {
        bail!(e)
    }
    match recv_json(&mut s).await {
        Ok(Message::Subscribed(Subscribed { bids, asks })) => {
            let bids = bids
                .into_iter()
                .map(|Named { price, size }| ExchangeMessage::Buy {
                    price,
                    quantity: size,
                });
            let asks = asks
                .into_iter()
                .map(|Named { price, size }| ExchangeMessage::Sell {
                    price,
                    quantity: size,
                });

            let cont = stream::try_unfold(s, |mut it| async move {
                match recv_json(&mut it).await {
                    Ok(Message::Connected | Message::Subscribed(_)) => Err(WsError::Io(
                        io::Error::invalid_data(r#"expected "channel_data""#),
                    )),
                    Ok(Message::ChannelData(ChannelData { bids, asks })) => {
                        let bids = bids
                            .into_iter()
                            .map(|(price, quantity)| ExchangeMessage::Buy { price, quantity });
                        let asks = asks
                            .into_iter()
                            .map(|(price, quantity)| ExchangeMessage::Sell { price, quantity });
                        Ok(Some((stream::iter(bids.chain(asks).map(Ok)), it)))
                    }
                    Err(e) => Err(e),
                }
            })
            .try_flatten();
            Either::Right(stream::iter(bids.chain(asks).map(Ok)).chain(cont))
        }
        Ok(_) => bail!(io::Error::invalid_data(
            r#"expected to receive "subscribed""#
        )),
        Err(e) => bail!(e),
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(tag = "type", content = "contents", rename_all = "snake_case")]
enum Message<PriceT, QuantityT> {
    Connected,
    Subscribed(Subscribed<PriceT, QuantityT>),
    ChannelData(ChannelData<PriceT, QuantityT>),
}

// TODO(aatifsyed): this is typically a single item on one side, so could use e.g
//                  ArrayVec here to avoid an allocation, or an enum to drop
//                  struct size.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(bound(
    // serde infers a `T: Default` bound unecessarily...
    // break out the `bound(..)` because it's the only way to access the `'de` lifetime
    deserialize = "PriceT: Deserialize<'de>, QuantityT: Deserialize<'de>", 
    serialize = "PriceT: Serialize, QuantityT: Serialize"
))]
struct ChannelData<PriceT, QuantityT> {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    bids: Vec<(PriceT, QuantityT)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    asks: Vec<(PriceT, QuantityT)>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(bound(
    deserialize = "PriceT: Deserialize<'de>, QuantityT: Deserialize<'de>",
    serialize = "PriceT: Serialize, QuantityT: Serialize"
))]
struct Subscribed<PriceT, QuantityT> {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    bids: Vec<Named<PriceT, QuantityT>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    asks: Vec<Named<PriceT, QuantityT>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
struct Named<PriceT, QuantityT> {
    price: PriceT,
    size: QuantityT,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::integrations::{round_trip, u16f16};

    #[test]
    fn deser() {
        round_trip(
            Message::<u16f16, u16f16>::Connected,
            json!({"type": "connected"}),
        );
        round_trip(
            Message::ChannelData(ChannelData {
                bids: vec![(u16f16::lit("123"), u16f16::lit("456"))],
                asks: vec![],
            }),
            json!({"type": "channel_data", "contents": {"bids": [["123", "456"]]}}),
        );
        round_trip(
            Message::Subscribed(Subscribed {
                bids: vec![
                    Named {
                        price: u16f16::lit("123"),
                        size: u16f16::lit("456"),
                    },
                    Named {
                        price: u16f16::lit("789"),
                        size: u16f16::lit("123"),
                    },
                ],
                asks: vec![],
            }),
            json!({"type": "subscribed", "contents": {"bids": [{"price": "123", "size": "456"}, {"price": "789", "size": "123"}]}}),
        )
    }
}
