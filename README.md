## Bitfinex API client for lua application server tarantool

This Lua library provides a public rest, public websocket and auth websocket api.
Websocket API provides non-blocking cb interface based on tarantool fibers.

#
## Installation
```bash 
 tarantoolctl rocks install https://raw.githubusercontent.com/golgote/neturl/master/rockspec/net-url-1.1-1.rockspec
 tarantoolctl rocks install https://raw.githubusercontent.com/r3l0c/lua-bitfinex/master/lua-bitfinex-scm-1.rockspec

```
```lua
local json = require 'json'
local bfx = require ("lua-bitfinex"):new(APIKEY, APISECRET)
local candles = bfx:Candles(bfx.TimeFrame.H1, 'tBTCUSD', 'hist')
print(json.encode(candles)) --- [[1646157600000,43663.22435776,43805,43904.72423777,43480,106.44998248],[1646154000000,43408,43667.53475113,43859,43284,271.17863414],..]

local candles = bfx:wsCandles('tBTCUSD', bfx.TimeFrame.H1, nil, nil, nil, function(channel_msg)
    print(json.encode(channel_msg)) -- 1st: snapshot, follow: updates
end)

-- for authenticated channels 
bfx.ws_authenticated_cb = function(msg)
    print('AUTH CHANNELS: ' .. json.encode(msg))
end
bfx:wsCalc({{'margin_sym_tETHUSD'}})
```
# REST API

### Status ()

## Bitfinex:Status
 Get the current status of the platform, “Operative” or “Maintenance”.  Maintenance periods generally last for a few minutes to a couple of hours and may be necessary from time to time during infrastructure upgrades.

### Candles (timeframe, symbol, section, period, limit_, start_, end_, sort_)

## Bitfinex:Candles.
 The Candles endpoint provides OCHL (Open, Close, High, Low) and volume data for the specified funding currency or trading pair. The endpoint provides the last 100 candles by default, but a limit and a start and/or end timestamp can be specified.

### Tickers (symbols)

## Bitfinex:Tickers.
 The Bitfinex.Tickers provides a high level overview of the state of the market.
 It shows the current best bid and ask, the last traded price,
 as well as information on the daily volume and price movement over the last day.
 The endpoint can retrieve multiple tickers with a single query.

### Ticker (symbol)

## Bitfinex:Ticker.
 The ticker endpoint provides a high level overview of the state of the market for a specified pair. It shows the current best bid and ask, the last traded price, as well as information on the daily volume and price movement over the last day.

### TickersHist (symbols, limit_, start_, end_)

## Bitfinex:TickersHist.
 History of recent trading tickers. Provides historic data of the best bid and ask at a 10-second interval.
 Historic data goes back 1 year. The oldest results have a 30-minute interval.

### Trades (symbol, limit_, start_, end_, sort_)

## Bitfinex:Trades.
 The trades endpoint allows the retrieval of past public trades and includes details such as price, size, and time.
 Optional parameters can be used to limit the number of results; you can specify a start and end timestamp, a limit, and a sorting method.

### Book (symbol, precision, len)

## Bitfinex:Book.
 The Public Books endpoint allows you to keep track of the state of Bitfinex order books on a price aggregated basis with customizable precision.
 Raw books can be retrieved by using precision R0.
 https://docs.bitfinex.com/reference#rest-public-book

### Stats (symbol, key, size, side, section, limit_, start_, end_, sort_)

## Bitfinex:Stats.
 The Bitfinex.Stats The Stats endpoint provides various statistics on a specified trading pair or funding currency. Use the available keys to specify which statistic you wish to retrieve. Please note that the 'Side' path param is only required for the pos.size key.
 It shows the current best bid and ask, the last traded price,
 as well as information on the daily volume and price movement over the last day.
 The endpoint can retrieve multiple tickers with a single query.

### Configs (query)

## Bitfinex:Configs.
 Fetch currency and symbol site configuration data.
 https://docs.bitfinex.com/reference#rest-public-conf
 A variety of types of config data can be fetched by constructing a path with an Action, Object, and conditionally a Detail value.

### DerivativesStatus (s_pairs)

## Bitfinex:DerivativesStatus.
 Endpoint used to receive different types of platform information - currently supports derivatives pair status only.

### DerivativesStatusHistory (symbol, limit_, start_, end_, sort_)

## Bitfinex:DerivativesStatusHistory.
 Endpoint used to receive different types of historical platform information - currently supports derivatives pair status only.

### Liquidations (limit_, start_, end_, sort_)

## Bitfinex:Liquidations.
 Endpoint to retrieve liquidations. By default it will retrieve the most recent liquidations, but time-specific data can be retrieved using timestamps.

### Leaderboards (key_, timeframe, symbol, section, limit_, start_, end_, sort_)

## Bitfinex:Leaderboards.
 The leaderboards endpoint allows you to retrieve leaderboard standings for unrealized profit (period delta), unrealized profit (inception), volume, and realized profit.

### PulseHistory (limit_, end_)

## Bitfinex:PulseHistory.
 View the latest pulse messages. You can specify an end timestamp to view older messages.

### PulseProfileDetails (nickname)

## Bitfinex:PulseProfileDetails.
 This endpoint shows details for a specific Pulse profile

### FundingStats (symbol, limit_, start_, end_)

## Bitfinex:FundingStats.
 Get a list of the most recent funding data for the given currency: FRR, average period, total amount provided, total amount used

### MarketAveragePrice (symbol, amount, period, rate_limit)

## Bitfinex:MarketAveragePrice.
 Calculate the average execution price for Trading or rate for Margin funding.

### ForeignExchangeRate (ccy1, ccy2)

## Bitfinex:ForeignExchangeRate.
 Calculate the exchange rate between two currencies

# WebSocket API

### wsTicker (symbol, callback)

## Bitfinex:wsTicker.
 The ticker endpoint provides a high level overview of the state of the market for a specified pair. It shows the current best bid and ask, the last traded price, as well as information on the daily volume and price movement over the last day.

### wsTrades (symbol, callback)

## Bitfinex:wsTrades.
 This channel sends a trade message whenever a trade occurs at Bitfinex. It includes all the pertinent details of the trade, such as price, size and the time of execution. The channel can send funding trade data as well.

### wsBooks (symbol, precision, frequency, length, subid, callback)

## Bitfinex:wsBooks.
 The Order Books channel allows you to keep track of the state of the Bitfinex order book. It is provided on a price aggregated basis with customizable precision. Upon connecting, you will receive a snapshot of the book

### wsCandles (symbol, timeframe, aaggr, pper_start, pend, callback)

## Bitfinex:wsCandles.
 The Candles endpoint provides OCHL (Open, Close, High, Low) and volume data for the specified trading pair.

### wsStatus (key, callback)

## Bitfinex:wsStatus
 Subscribe to and receive different types of platform information - currently supports derivatives pair status and liquidation feed.
 https://docs.bitfinex.com/reference#ws-public-status

### wsNewOrder (gid, cid, type_, symbol, amount, price, lev, price_trailing, price_aux_limit, price_oco_stop, flags, tif, meta)

## Bitfinex:wsNewOrder
 Creates a new order, can be used to create margin, exchange, and derivative orders.
 https://docs.bitfinex.com/reference#ws-auth-input-order-new

### wsUpdateOrder (id, gid, cid, cid_date, amount, price, lev, delta, price_trailing, price_aux_limit, flags, tif)

## Bitfinex:wsUpdateOrder
 Update an existing order, can be used to update margin, exchange, and derivative orders.
 https://docs.bitfinex.com/reference#ws-auth-input-order-update

### wsNewOrderMultiOp (op)

## Bitfinex:wsNewOrderMultiOp
 Send multiple order-related operations.  (Max 75 operations per request)
 https://docs.bitfinex.com/reference#ws-auth-input-order-multi-op

### wsCancelOrder (id, cid, cid_date)

## Bitfinex:wsCancelOrder
 To cancel an order, construct a payload using the 'oc' (order cancel) abbreviation.  The order details need to include either the Internal Order ID or the Client Order ID and Client Order ID Date (YYYY-MM-DD format).
 https://docs.bitfinex.com/reference#ws-auth-input-order-cancel

### wsCancelOrderMulti (payload)

## Bitfinex:wsCancelOrderMulti
 The Cancel Order Multi command allows sending of multiple order cancellation operations at the same time.  To do this, construct a payload with the 'oc_multi' (order cancel multi) abbreviation. The payload needs to include an object that specifies which orders to cancel based on Internal Order ID's, Client Order ID's (along with the cid_DATE in a YYYY-MM-DD format), Group ID, or the 'all' request field. Please look to the sidebar for examples of each method.
 https://docs.bitfinex.com/reference#ws-auth-input-order-cancel-multi

### wsNewOffer (type_, symbol, amount, rate, period, flags)

## Bitfinex:wsNewOffer
 Create a new funding offer.
 https://docs.bitfinex.com/reference#ws-auth-input-offer-new

### wsCancelOffer (id)

## Bitfinex:wsCancelOffer
 Cancel a funding offer.
 https://docs.bitfinex.com/reference#ws-auth-input-offer-cancel

### wsCalc (req)

## Bitfinex:wsCalc
 Send calc requests to trigger specific calculations.
 https://docs.bitfinex.com/reference#ws-auth-input-calc

#
## На свободное время ;)
## ERC-20 network 0xbc7d2c2fe40f9d8b78f79eaa7dedc3bae375d001
## NANO nano_3gus81iswuaefexdbximtm7xb89n4kdmdnga3yqne4xuw7igpnroobcw4x1c

