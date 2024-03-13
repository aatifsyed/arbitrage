//! Note the API docs are wrong.
//! E.g in [channel data](https://docs.dydx.exchange/developers/indexer/indexer_websocket#channel-data-1)
//! there is no `clobPairId` field in reality.

use std::io;

use futures::{
    future::{self, Either},
    stream, Sink, Stream, StreamExt as _, TryStreamExt as _,
};
use io_extra::IoErrorExt as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

use super::{recv_json, send_json, ExchangeMessage, WsError, WsMessage, WsResult};

macro_rules! bail {
    ($expr:expr) => {
        return Either::Left(stream::once(future::ready(Err(WsError::from($expr)))))
    };
}

pub async fn protocol<PriceT, QuantityT>(
    mut s: impl Stream<Item = WsResult<WsMessage>> + Sink<WsMessage, Error = WsError> + Unpin,
    id: &str,
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
    pub bids: Vec<(PriceT, QuantityT)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asks: Vec<(PriceT, QuantityT)>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(bound(
    deserialize = "PriceT: Deserialize<'de>, QuantityT: Deserialize<'de>",
    serialize = "PriceT: Serialize, QuantityT: Serialize"
))]
struct Subscribed<PriceT, QuantityT> {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bids: Vec<Named<PriceT, QuantityT>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asks: Vec<Named<PriceT, QuantityT>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
struct Named<PriceT, QuantityT> {
    pub price: PriceT,
    pub size: QuantityT,
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;

    #[allow(non_camel_case_types)]
    type u16f16 = fixed::FixedU32<typenum::U16>;

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

    #[track_caller]
    fn round_trip<T>(repr: T, json: serde_json::Value)
    where
        T: DeserializeOwned + Serialize + PartialEq + Debug,
    {
        let repr2json = serde_json::to_value(&repr).unwrap();
        assert_eq!(repr2json, json);

        let json2repr = serde_json::from_value(json).unwrap();
        assert_eq!(repr, json2repr);
    }
}
