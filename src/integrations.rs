use std::{
    fmt::{self, Display},
    io,
    pin::pin,
};

use bstr::BString;
use futures::{future::Either, stream, Sink, SinkExt as _, Stream, TryStreamExt as _};
use io_extra::IoErrorExt as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_path_to_error::Path;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::client::IntoClientRequest;

mod aevo;
mod dydx;

type WsMessage = tungstenite::Message;
type WsError = tungstenite::Error;
type WsResult<T> = tungstenite::Result<T>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ExchangeMessage<PriceT, QuantityT> {
    Buy { price: PriceT, quantity: QuantityT },
    Sell { price: PriceT, quantity: QuantityT },
}

pub fn dydx<PriceT, QuantityT>(
    id: impl Into<String>, // "BTC-USD"
) -> impl Stream<Item = tungstenite::Result<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    connect_websocket("wss://indexer.dydx.trade/v4/ws", move |it| {
        dydx::protocol(it, id.into())
    })
}

pub fn aevo<PriceT, QuantityT>(
    id: impl Display, // "BTC-PERP"
) -> impl Stream<Item = tungstenite::Result<ExchangeMessage<PriceT, QuantityT>>>
where
    PriceT: DeserializeOwned,
    QuantityT: DeserializeOwned,
{
    connect_websocket("wss://ws.aevo.xyz", move |it| aevo::protocol(it, id))
}

fn connect_websocket<F, S, T>(
    to: impl IntoClientRequest + Unpin, // TODO(aatifsyed): make a PR to tokio-tungstenite to relax `Unpin` bound,
    f: F,
) -> impl Stream<Item = Result<T, tungstenite::Error>>
where
    F: FnOnce(WebSocketStream<MaybeTlsStream<TcpStream>>) -> S,
    S: Stream<Item = Result<T, tungstenite::Error>>,
{
    let mut f = Some(f);
    stream::once(tokio_tungstenite::connect_async(to))
        .map_ok(move |(st, _http)| f.take().expect("stream::once only yields once")(st))
        .try_flatten()
}

async fn send_json(s: impl Sink<WsMessage, Error = WsError>, t: impl Serialize) -> WsResult<()> {
    let msg = serde_json::to_vec(&t).map_err(|e| WsError::Io(io::Error::invalid_input(e)))?;
    pin!(s).send(WsMessage::Binary(msg)).await
}

async fn recv_json<T: DeserializeOwned>(s: impl Stream<Item = WsResult<WsMessage>>) -> WsResult<T> {
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
        Either::Left(it) => {
            serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_slice(it))
        }
        // skip UTF-8 validation where possible
        Either::Right(it) => {
            serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_str(it))
        }
    }
    .map_err(|it| {
        WsError::Io(io::Error::invalid_data(SerializationError {
            path: it.path().clone(),
            inner: it.into_inner(),
            src: match src {
                Either::Left(it) => it[..].into(),
                Either::Right(it) => it[..].into(),
            },
            ty: {
                let name = std::any::type_name::<T>();
                match name.split_once('<') {
                    Some((name, _)) => name,
                    None => name,
                }
            },
        }))
    })
}

#[derive(Debug, thiserror::Error)]
struct SerializationError {
    path: Path,
    inner: serde_json::Error,
    src: BString,
    ty: &'static str,
}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            path,
            inner,
            src,
            ty,
        } = self;
        f.write_fmt(format_args!("couldn't serialize a {}\n\n", ty))?;
        f.write_fmt(format_args!("error: {}\n", inner))?;
        f.write_fmt(format_args!("location: {}\n", path))?;
        f.write_fmt(format_args!("source text: {}\n", src))?;
        Ok(())
    }
}

macro_rules! bail {
    ($expr:expr) => {
        return futures::future::Either::Left(futures::stream::once(futures::future::ready(Err(
            $crate::integrations::WsError::from($expr),
        ))))
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
