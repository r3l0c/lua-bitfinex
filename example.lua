local json = require 'json'
local bfx = require "lua-bitfinex"

local trades = bfx:wsTrades('tBTCUSD', function(channel_msg)
    print(json.encode(channel_msg))
end)

local rest_candles = bfx:Candles(bfx.TimeFrame.H1, 'tBTCUSD', 'hist')
print(json.encode(rest_candles))

local candles = bfx:wsCandles('tBTCUSD', bfx.TimeFrame.H1, nil, nil, nil, function(channel_msg)
    print(json.encode(channel_msg))
end)

bfx.ws_authenticated_cb = function(msg)
    print('AUTH CHANNELS: ' .. json.encode(msg))
end
bfx:wsCalc({{'margin_sym_tETHUSD'}})
