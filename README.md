# Check connectivity

## dydx

https://docs.dydx.exchange/developers/indexer/indexer_websocket#orderbooks

dydx.trade is blocked in the UK, so need to VPN...

```console
$ echo '{"type": "subscribe", "channel": "v4_orderbook", "id": "BTC-USD"}' | websocat wss://indexer.dydx.trade/v4/ws
```

Snapshot then updates

## aevo

```console
$ echo '{"op": "subscribe", "data": ["orderbook:BTC-PERP"]}' | websocat wss://ws.aevo.xyz
```
> Connection will time out after 15 minutes of inactivity. To keep the connection alive, perform a ping operation. Sending any other operation will also refresh the connection.

Snapshot then snapshots and updates
