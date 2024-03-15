# Arbitrage
## Overview
- Generic `ArbitrageFinder`.
- Exchange-specific protocol abstractions.
- Live integration tests.

```console
$ cargo run -- --help
Usage: openhedge-arbitrage [OPTIONS]

Options:
  -q, --quiet     Omit `TRACE` logs
  -c, --continue  Don't stop at the first error - log and continue
  -h, --help      Print help

$ cargo run
TRACE received message src=Aevo msg=Sell { price: 68117.8, quantity: 2.203 }
 INFO simulated arbitrage new_balance=0.4691999983 spread=4.2 quantity=0.0118 sell=Aevo buy=Dydx
TRACE received message src=Aevo msg=Buy { price: 68110.8, quantity: 0.003 }
TRACE received message src=Aevo msg=Sell { price: 68118.8, quantity: 3.3 }
 INFO simulated arbitrage new_balance=0.506959998 spread=3.2 quantity=0.0118 sell=Aevo buy=Dydx
TRACE received message src=Aevo msg=Buy { price: 68110.2, quantity: 0 }
```

## Check connectivity to exchanges
### dydx

https://docs.dydx.exchange/developers/indexer/indexer_websocket#orderbooks

dydx.trade is blocked in the UK, so need to VPN...

```console
$ echo '{"type": "subscribe", "channel": "v4_orderbook", "id": "BTC-USD"}' | websocat wss://indexer.dydx.trade/v4/ws
```

### aevo

```console
$ echo '{"op": "subscribe", "data": ["orderbook:BTC-PERP"]}' | websocat wss://ws.aevo.xyz
```
> Connection will time out after 15 minutes of inactivity. To keep the connection alive, perform a ping operation. Sending any other operation will also refresh the connection.

