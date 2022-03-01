---------------------
-- Bitfinex API.
-- @module Bitfinex
local log = require 'log'
local websocket = require 'websocket'
local http_client = require('http.client').new()
local url = require 'net.url'
local json = require 'json'
local crypto = require 'crypto'
local fiber = require 'fiber'
local clock = require 'clock'

local Bitfinex = {
    utils = {
        gettime = function(addsecs)
            addsecs = addsecs or 0
            return (clock.time() + addsecs) * 10000
        end
    },
    API_URL = {
        REST = {
            PUBLIC = 'https://api-pub.bitfinex.com/v2/',
            AUTH = 'https://api.bitfinex.com/v2/'
        },
        WS = {
            PUBLIC = 'wss://api-pub.bitfinex.com/ws/2',
            AUTH = 'wss://api.bitfinex.com/ws/2'
        }
    },
    Auth = {},
    credentials = {
        apikey = '',
        apisecret = ''
    },
    TimeFrame = {
        M1 = '1m',
        M5 = '5m',
        M15 = '15m',
        M30 = '30m',
        H1 = '1h',
        H6 = '6h',
        H12 = '12h',
        D1 = '1D',
        W1 = '1W',
        D14 = '14D',
        Mo1 = '1M'
    },
    Precision = {
        P0 = 'P0',
        P1 = 'P1',
        P2 = 'P2',
        P3 = 'P3',
        P4 = 'P4',
        R0 = 'R0'
    },
    StatsKeys = {
        pos_size = 'pos.size',
        funding_size = 'funding.size',
        credits_size = 'credits.size',
        credits_size_sym = 'credits.size.sym',
        vol_1d = 'vol.1d',
        vol_7d = 'vol.7d',
        vol_30d = 'vol.30d',
        vwap = 'vwap'
    },
    ConfigsPub = {
        map = {
            currency = {
                sym = 'map:currency:sym',
                label = 'map:currency:label',
                unit = 'map:currency:unit',
                undl = 'map:currency:undl',
                pool = 'map:currency:pool',
                explorer = 'map:currency:explorer'
            },
            tx = {
                method = 'map:tx:method'
            }
        },
        list = {
            currency = 'list:currency',
            pair = {
                exchange = 'list:pair:exchange',
                margin = 'list:pair:margin'
            },
            competitions = 'list:competitions'
        },
        info = {
            pair = 'info:pair',
            tx = {
                status = 'info:tx:status'
            }
        },
        fees = 'fees',
        spec = {
            margin = 'spec:margin'
        }
    },
    Leaderboards = {
        key = {
            plu_diff = 'plu_diff',
            plu = 'plu',
            plr = 'plr',
            vol = 'vol'
        },
        timeframe = {
            H3 = '3h',
            W1 = '1w',
            M1 = '1M'
        }
    },
    AuthConn = {
        userId = 0,
        caps = {}
    },
    fibers = {},
    wspub = nil,
    wspub_ready = false,
    wsauth = nil,
    wsauth_ready = false,
    ch_wspub_recv = nil,
    ch_wspub_send = nil,
    ch_wsauth_recv = nil,
    ch_wsauth_send = nil,
    ws_authenticated_cb = nil
}

function Bitfinex:new(apikey, apisecret)
    local n = {}
    setmetatable(n, self)
    self.__index = self
    n.credentials.apikey = apikey
    n.credentials.apisecret = apisecret
    n.wspub = nil
    n.wsauth = nil
    n.wscbs = {}
    n.wschannels = {}
    n.wsfiber = nil
    n.ch_wspub_recv = nil
    n.ch_wspub_send = nil
    n.ch_wsauth_recv = nil
    n.ch_wsauth_send = nil
    n.wspub_ready = false
    n.wsauth_ready = false
    return n
end

function Bitfinex:makeSig(msg)
    local sig = crypto.hmac.sha384(self.credentials.apisecret, msg)
    return sig:gsub('.', function(c)
        return string.format('%02x', sig.byte(c))
    end)
end

function Bitfinex:error_log(msg)
    print(msg)
end

function Bitfinex:query_pub(qPath, qParams)
    local u = url.parse(self.API_URL.REST.PUBLIC .. qPath)
    if (qParams ~= nil) then
        u:setQuery(qParams)
    end
    local resp = http_client:request('GET', tostring(u))
    if resp.status ~= 200 then
        self:error_log('ERROR JSON DECODE: ' .. u .. ': body: ' .. (json.encode(resp) or 'nil'))
    end
    local j, e = json.decode(resp.body)
    if (e ~= nil) then
        self:error_log('ERROR JSON DECODE: ' .. u .. ': body: ' .. (json.encode(resp) or 'nil') .. ', error: ' .. e)
    end
    return j
end

function Bitfinex:query_post_json(qPath, qParams)
    local u = url.parse((string.find(qPath, 'auth/') and self.API_URL.REST.AUTH or self.API_URL.REST.PUBLIC) .. qPath)
    print(u)
    local opts = {
        headers = {}
    }
    opts.headers['Content-Type'] = 'application/json'
    if string.find(tostring(u), 'v2/auth/') then
        local nonce = tostring(self.utils.gettime())
        local signature = '/api/v2/' .. qPath .. nonce .. json.encode(qParams)
        print(signature)
        local sig = self:makeSig(signature)
        opts.headers['bfx-nonce'] = nonce
        opts.headers['bfx-apikey'] = self.credentials.apikey
        opts.headers['bfx-signature'] = sig
    end
    local resp = http_client:request('POST', tostring(u), json.encode(qParams), opts)
    if resp.status ~= 200 then
        self:error_log('ERROR JSON DECODE: ' .. u .. ': body: ' .. (json.encode(resp) or 'nil'))
    end
    local j, e = json.decode(resp.body)
    if (e ~= nil) then
        self:error_log('ERROR JSON DECODE: ' .. u .. ': body: ' .. (json.encode(resp) or 'nil') .. ', error: ' .. e)
    end
    return j
end

--- Bitfinex:Status
-- Get the current status of the platform, “Operative” or “Maintenance”. Maintenance periods generally last for a few minutes to a couple of hours and may be necessary from time to time during infrastructure upgrades.
-- @return table {OPERATIVE = 1} or {OPERATIVE = 0}. 1=operative, 0=maintenance
function Bitfinex:Status()
    local qPath = 'platform/status'
    return {
        OPERATIVE = self:query_pub(qPath)[1]
    }
end

--- Bitfinex:Candles.
-- The Candles endpoint provides OCHL (Open, Close, High, Low) and volume data for the specified funding currency or trading pair. The endpoint provides the last 100 candles by default, but a limit and a start and/or end timestamp can be specified.
-- @param timeframe string. Available values: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '1W', '14D', '1M'
-- @param symbol string. The symbol you want information about. (e.g. tBTCUSD, tETHUSD, fUSD, fBTC)
-- @param section string. Available values: 'last', 'hist'
-- @param period string. Funding period. Only required for funding candles. eg p30.
-- @param limit_ number. Number of candles requested (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table.  [[MTS, OPEN, CLOSE, HIGH, LOW, VOLUME], ...]
function Bitfinex:Candles(timeframe, symbol, section, period, limit_, start_, end_, sort_)

    local qPath = 'candles/trade:' .. timeframe .. ':' .. symbol .. (period ~= nil and ':' .. period or '') .. '/' ..
                      (section or 'last')
    local qParams = {}
    qParams['sort'] = sort_
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_
    return self:query_pub(qPath, qParams)
end

--- Bitfinex:Tickers.
-- The Bitfinex.Tickers provides a high level overview of the state of the market.
-- It shows the current best bid and ask, the last traded price,
-- as well as information on the daily volume and price movement over the last day.
-- The endpoint can retrieve multiple tickers with a single query.
-- @param symbols a table of symbols you want information about. nil, {'ALL'}, {'tBTCUSD', 'tLTCUSD'}...
-- @return table
function Bitfinex:Tickers(symbols)
    local qParams
    local qPath = 'tickers'
    if (type(symbols) == 'string') then
        symbols = {symbols}
    end
    qParams = {
        symbols = table.concat(symbols or {'ALL'}, ',')
    }
    local tickers_raw = self:query_pub(qPath, qParams)
    local tickers = {}
    for i, t in ipairs(tickers_raw) do
        local is_funding = string.sub(t[1], 1, 1) == 'f'
        local ticker
        if (is_funding) then
            ticker = {
                SYMBOL = t[1],
                FRR = tonumber(t[2]),
                BID = tonumber(t[3]),
                BID_PERIOD = tonumber(t[4]),
                BID_SIZE = tonumber(t[5]),
                ASK = tonumber(t[6]),
                ASK_PERIOD = tonumber(t[7]),
                ASK_SIZE = tonumber(t[8]),
                DAILY_CHANGE = tonumber(t[9]),
                DAILY_CHANGE_RELATIVE = tonumber(t[10]),
                LAST_PRICE = tonumber(t[11]),
                VOLUME = tonumber(t[12]),
                HIGH = tonumber(t[13]),
                LOW = tonumber(t[14]),
                FRR_AMOUNT_AVAILABLE = tonumber(t[17])
            }
        else
            ticker = {
                SYMBOL = t[1],
                BID = tonumber(t[2]),
                BID_SIZE = tonumber(t[3]),
                ASK = tonumber(t[4]),
                ASK_SIZE = tonumber(t[5]),
                DAILY_CHANGE = tonumber(t[6]),
                DAILY_CHANGE_RELATIVE = tonumber(t[7]),
                LAST_PRICE = tonumber(t[8]),
                VOLUME = tonumber(t[9]),
                HIGH = tonumber(t[10]),
                LOW = tonumber(t[11])
            }
        end
        table.insert(tickers, i, ticker)
    end
    return tickers
end

--- Bitfinex:Ticker.
-- The ticker endpoint provides a high level overview of the state of the market for a specified pair. It shows the current best bid and ask, the last traded price, as well as information on the daily volume and price movement over the last day.
-- @param symbol string. {'tBTCUSD'}, {'fUSD'}...
-- @return table
function Bitfinex:Ticker(symbol)
    return self:Tickers(symbol)[1]
end

--- Bitfinex:TickersHist.
-- History of recent trading tickers. Provides historic data of the best bid and ask at a 10-second interval.
-- Historic data goes back 1 year. The oldest results have a 30-minute interval.
-- @param symbols string. , {'tBTCUSD','tETHUSD'}...
-- @param limit_ number. Number of records (Max 250)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @return table
function Bitfinex:TickersHist(symbols, limit_, start_, end_)
    local qParams
    local qPath = 'tickers/hist'
    if (type(symbols) == 'string') then
        symbols = {symbols}
    end
    qParams = {
        symbols = table.concat(symbols or {'ALL'}, ',')
    }
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local tickers_raw = self:query_pub(qPath, qParams)
    local tickers = {}
    for i, t in ipairs(tickers_raw) do
        local is_funding = string.sub(t[1], 1, 1) == 'f'
        local ticker
        ticker = {
            SYMBOL = t[1],
            BID = tonumber(t[2]),
            ASK = tonumber(t[4]),
            MTS = tonumber(t[13])
        }
        table.insert(tickers, i, ticker)
    end
    return tickers
end

--- Bitfinex:Trades.
-- The trades endpoint allows the retrieval of past public trades and includes details such as price, size, and time.
-- Optional parameters can be used to limit the number of results; you can specify a start and end timestamp, a limit, and a sorting method.
-- @param symbol string. , {'tBTCUSD'} or tBTCUSD...
-- @param limit_ number. Number of records (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table
function Bitfinex:Trades(symbol, limit_, start_, end_, sort_)
    local qParams = {}
    if (type(symbol) == 'table') then
        symbol = symbol[1]
    end
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_
    qParams['sort'] = sort_

    local qPath = 'trades/' .. symbol .. '/hist'
    local trades_raw = self:query_pub(qPath, qParams)
    local trades = {}
    local is_funding = string.sub(symbol, 1, 1) == 'f'
    local trade
    for i, t in ipairs(trades_raw) do
        if (is_funding) then
            trade = {
                ID = t[1],
                MTS = tonumber(t[2]),
                AMOUNT = tonumber(t[3]),
                RATE = tonumber(t[4]),
                PERIOD = tonumber(t[5])
            }
        else
            trade = {
                ID = t[1],
                MTS = tonumber(t[2]),
                AMOUNT = tonumber(t[3]),
                PRICE = tonumber(t[4])
            }
        end
        table.insert(trades, i, trade)
    end
    return trades
end

--- Bitfinex:Book.
-- The Public Books endpoint allows you to keep track of the state of Bitfinex order books on a price aggregated basis with customizable precision.
-- Raw books can be retrieved by using precision R0.
-- https://docs.bitfinex.com/reference#rest-public-book
-- @param symbol a string of the symbol you want information about. (e.g. tBTCUSD, tETHUSD, fUSD, fBTC)
-- @param precision Level of price aggregation (P0, P1, P2, P3, P4, R0). Bitfinex.Precision.P0 ...
-- @param len number of price points ('1', '25', '100')
-- @return table
function Bitfinex:Book(symbol, precision, len)
    local qParams = {
        len = len
    }
    if (type(symbol) == 'table') then
        symbol = symbol[1]
    end

    local qPath = 'book/' .. symbol .. '/' .. precision
    local book_raw = self:query_pub(qPath, qParams)
    local book = {}
    local is_raw = precision == 'R0'
    local is_funding = string.sub(symbol, 1, 1) == 'f'
    local record
    for i, t in ipairs(book_raw) do
        if (is_funding) then
            if (is_raw) then
                record = {
                    ORDER_ID = tonumber(t[1]),
                    PERIOD = tonumber(t[2]),
                    RATE = tonumber(t[3]),
                    AMOUNT = tonumber(t[4])
                }
            else
                record = {
                    RATE = tonumber(t[1]),
                    PERIOD = tonumber(t[2]),
                    AMOUNT = tonumber(t[3]),
                    COUNT = tonumber(t[4])
                }
            end
        else
            if (is_raw) then
                record = {
                    ORDER_ID = tonumber(t[1]),
                    PRICE = tonumber(t[2]),
                    AMOUNT = tonumber(t[3])
                }
            else
                record = {
                    PRICE = tonumber(t[1]),
                    COUNT = tonumber(t[2]),
                    AMOUNT = tonumber(t[3])
                }
            end
        end
        table.insert(book, i, record)
    end
    return book
end

--- Bitfinex:Stats.
-- The Bitfinex.Stats The Stats endpoint provides various statistics on a specified trading pair or funding currency. Use the available keys to specify which statistic you wish to retrieve. Please note that the 'Side' path param is only required for the pos.size key.
-- It shows the current best bid and ask, the last traded price, 
-- as well as information on the daily volume and price movement over the last day. 
-- The endpoint can retrieve multiple tickers with a single query.
-- @param symbol a array of symbols you want information about. Trading/Funding only: {'tBTCUSD'} or {'fUSD'} or {'BFX'}(vol.1d / vol.7d / vol.30d keys) etc, Funding and trading: {'fUSD','tBTCUSD'}  ... 
-- @param key a string of Bitfinex.StatsKeys. Allowed values: 'funding.size', 'credits.size', 'credits.size.sym', 'pos.size', 'vol.1d', 'vol.7d', 'vol.30d', 'vwap'
-- @param size string. Available values: '1m' (for keys: 'pos.size', 'funding.size', 'credits.size', 'credits.size.sym'), '30m' (for keys: vol.1d, vol.7d, vol.30d), '1d' (for keys: vwap)
-- @param side string. Only used for 'pos.size' key. Available values: 'long', 'short'. Only for non-funding queries.
-- @param section, string. Available values: 'last', 'hist'
-- @param limit_ number. Number of records (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table of various statistics on a specified trading pair or funding currency. 
function Bitfinex:Stats(symbol, key, size, side, section, limit_, start_, end_, sort_)
    local qParams
    local qPath

    if (key == self.StatsKeys.pos_size) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. ':' .. side .. '/' .. section
    end
    if (key == self.StatsKeys.funding_size) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. '/' .. section
    end
    if (key == self.StatsKeys.credits_size) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. '/' .. section
    end
    if (key == self.StatsKeys.credits_size_sym) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. ':' .. symbol[1] .. '/' .. section
    end
    if (string.find(key, 'vol.')) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. '/' .. section
    end

    if (key == self.StatsKeys.vwap) then
        qPath = 'stats1/' .. key .. ':' .. size .. ':' .. symbol[1] .. '/' .. section
    end

    qParams = {}
    qParams['sort'] = sort_
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local stats_raw = self:query_pub(qPath, qParams)
    local stats = {}
    if (section == 'hist') then
        for i, t in ipairs(stats_raw) do
            table.insert(stats, i, {
                MTS = tonumber(t[1]),
                VALUE = tonumber(t[2])
            })
        end
    else
        return {
            MTS = tonumber(stats_raw[1]),
            VALUE = tonumber(stats_raw[2])
        }
    end
    return stats
end

--- Bitfinex:Configs.
-- Fetch currency and symbol site configuration data. 
-- https://docs.bitfinex.com/reference#rest-public-conf
-- A variety of types of config data can be fetched by constructing a path with an Action, Object, and conditionally a Detail value.
-- @param query string. Bitfinex.ConfigsPub
-- @return table
function Bitfinex:Configs(query)
    local qPath = 'conf/pub:' .. query
    return self:query_pub(qPath)
end

--- Bitfinex:DerivativesStatus.
-- Endpoint used to receive different types of platform information - currently supports derivatives pair status only. 
-- @param s_pairs string|table. The key or keys (Separate by commas) of the pairs to fetch status information. To fetch information for all pairs use the key value 'ALL' 
-- @return table
function Bitfinex:DerivativesStatus(s_pairs)
    local qParams
    if (type(s_pairs) == 'table') then
        qParams = {
            keys = table.concat(s_pairs, ',')
        }
    else
        qParams = {
            keys = s_pairs
        }
    end

    local qPath = 'status/deriv'
    local derivs_raw = self:query_pub(qPath, qParams)
    local derivs = {}
    for i, t in ipairs(derivs_raw) do
        table.insert(derivs, i, {
            KEY = tonumber(t[1]),
            MTS = tonumber(t[2]),
            DERIV_PRICE = tonumber(t[4]),
            SPOT_PRICE = tonumber(t[5]),
            INSURANCE_FUND_BALANCE = tonumber(t[7]),
            NEXT_FUNDING_EVT_TIMESTAMP_MS = tonumber(t[9]),
            NEXT_FUNDING_ACCRUED = tonumber(t[10]),
            NEXT_FUNDING_STEP = tonumber(t[11]),
            CURRENT_FUNDING = tonumber(t[13]),
            MARK_PRICE = tonumber(t[16]),
            OPEN_INTEREST = tonumber(t[19]),
            CLAMP_MIN = tonumber(t[23]),
            CLAMP_MAX = tonumber(t[24])
        })
    end
    return derivs
end

--- Bitfinex:DerivativesStatusHistory.
-- Endpoint used to receive different types of historical platform information - currently supports derivatives pair status only.
-- @param symbol string|table. The symbol you want information about. (e.g. tBTCF0:USTF0 tETHF0:USTF0, {'tBTCF0:USTF0'} etc.) 
-- @param limit_ number. Number of records (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table [[],[],...]
function Bitfinex:DerivativesStatusHistory(symbol, limit_, start_, end_, sort_)
    local qParams = {}
    qParams['sort'] = sort_
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local qPath = 'status/deriv/' .. (type(symbol) == 'table' and symbol[1] or symbol) .. '/hist'
    local derivs_raw = self:query_pub(qPath, qParams)
    local derivs = {}
    for i, t in ipairs(derivs_raw) do
        table.insert(derivs, i, {
            MTS = tonumber(t[1]),
            DERIV_PRICE = tonumber(t[3]),
            SPOT_PRICE = tonumber(t[4]),
            INSURANCE_FUND_BALANCE = tonumber(t[6]),
            NEXT_FUNDING_EVT_TIMESTAMP_MS = tonumber(t[8]),
            NEXT_FUNDING_ACCRUED = tonumber(t[9]),
            NEXT_FUNDING_STEP = tonumber(t[10]),
            CURRENT_FUNDING = tonumber(t[12]),
            MARK_PRICE = tonumber(t[15]),
            OPEN_INTEREST = tonumber(t[18]),
            CLAMP_MIN = tonumber(t[22]),
            CLAMP_MAX = tonumber(t[23])
        })
    end
    return derivs
end

--- Bitfinex:Liquidations.
-- Endpoint to retrieve liquidations. By default it will retrieve the most recent liquidations, but time-specific data can be retrieved using timestamps.
-- @param limit_ number. Number of records (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table [[],[],...]
function Bitfinex:Liquidations(limit_, start_, end_, sort_)
    local qParams = {}
    qParams['sort'] = sort_
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local qPath = 'liquidations/hist'
    local liquidations_raw = self:query_pub(qPath, qParams)
    local liquidations = {}
    for i, t in ipairs(liquidations_raw) do
        table.insert(liquidations, i, {
            POS_ID = tonumber(t[1][2]),
            MTS = tonumber(t[1][3]),
            SYMBOL = tonumber(t[1][5]),
            AMOUNT = tonumber(t[1][6]),
            BASE_PRICE = tonumber(t[1][7]),
            IS_MATCH = tonumber(t[1][9]),
            IS_MARKET_SOLD = tonumber(t[1][10]),
            PRICE_ACQUIRED = tonumber(t[1][12])
        })
    end
    return liquidations
end

--- Bitfinex:Leaderboards.
-- The leaderboards endpoint allows you to retrieve leaderboard standings for unrealized profit (period delta), unrealized profit (inception), volume, and realized profit.
-- @param key_ string. Bitfinex.Leaderboards.key. Allowed values: 'plu_diff' for unrealized profit (period delta); 'plu' for unrealized profit (inception); 'vol' for volume; 'plr' for realized profit
-- @param timeframe string. Bitfinex.Leaderboards.timeframe. Available values: '3h', '1w', '1M' - see table below for available time frames per key
-- @param symbol string. The symbol you want information about. (e.g. tBTCUSD, tETHUSD, tGLOBAL:USD) - see table below for available symbols per key
-- @param section string. Available values: 'hist'
-- @param limit_ number. Number of records (Max: 10000)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @param sort_ number. if = 1 it sorts results returned with old > new
-- @return table [[],[],...]
function Bitfinex:Leaderboards(key_, timeframe, symbol, section, limit_, start_, end_, sort_)
    local qParams = {}
    qParams['sort'] = sort_
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local qPath =
        'rankings/' .. (type(key_) == 'table' and key_[1] or key_) .. ':' .. timeframe .. ':' .. symbol .. '/' ..
            (section or 'hist')
    local leaders_raw = self:query_pub(qPath, qParams)
    local leaders = {}
    for i, t in ipairs(leaders_raw) do
        table.insert(leaders, i, {
            MTS = tonumber(t[1]),
            USERNAME = t[3],
            RANKING = tonumber(t[4]),
            VALUE = tonumber(t[7]),
            TWITTER_HANDLE = t[10]
        })
    end
    return leaders
end

--- Bitfinex:PulseHistory.
-- View the latest pulse messages. You can specify an end timestamp to view older messages.
-- @param limit_ number. Number of records (Max: 100)
-- @param end_ number. Millisecond end time
-- @return table [[],[],...]
function Bitfinex:PulseHistory(limit_, end_)
    local qParams = {}
    qParams['end'] = end_
    qParams['limit'] = limit_

    local qPath = 'pulse/hist'
    local pulse_raw = self:query_pub(qPath, qParams)
    local pulse = {}
    for i, t in ipairs(pulse_raw) do
        table.insert(pulse, i, {
            PID = t[1],
            MTS = tonumber(t[2]),
            PUID = t[4],
            TITLE = t[6],
            CONTENT = t[7],
            IS_PIN = tonumber(t[10]),
            IS_PUBLIC = tonumber(t[11]),
            COMMENTS_DISABLED = tonumber(t[12]),
            TAGS = t[13],
            ATTACHMENTS = t[14],
            META = t[15],
            LIKES = t[16],
            PULSE_PROFILE = {
                PUID = t[19][1][1],
                MTS = t[19][1][2],
                NICKNAME = t[19][1][4],
                PICTURE = t[19][1][5],
                TEXT = t[19][1][6],
                TWITTER_HANDLE = t[19][1][7],
                FOLLOWERS = t[19][1][9],
                FOLLOWING = t[19][1][10],
                TIPPING_STATUS = t[19][1][11]
            },
            COMMENTS = tonumber(t[20]),
            RANKING = tonumber(t[4]),
            VALUE = tonumber(t[7]),
            TWITTER_HANDLE = t[10]
        })
    end
    return pulse
end

--- Bitfinex:PulseProfileDetails.
-- This endpoint shows details for a specific Pulse profile
-- @param nickname string. Pulse user nickname (case sensitive)
-- @return table [[],[],...]
function Bitfinex:PulseProfileDetails(nickname)
    local qPath = 'pulse/profile/' .. nickname
    local profile_raw = self:query_pub(qPath)
    return {
        PUID = profile_raw[1],
        MTS = profile_raw[2],
        NICKNAME = profile_raw[4],
        PICTURE = profile_raw[6],
        TEXT = profile_raw[7],
        TWITTER_HANDLE = profile_raw[10],
        FOLLOWERS = profile_raw[12],
        FOLLOWING = profile_raw[3],
        TIPPING_STATUS = profile_raw[17]
    }
end

--- Bitfinex:FundingStats.
-- Get a list of the most recent funding data for the given currency: FRR, average period, total amount provided, total amount used
-- @param symbol string.The symbol you want information about. (e.g. fUSD, fBTC, fETH ...)
-- @param limit_ number. Number of records (Max: 100)
-- @param start_ number. Millisecond start time
-- @param end_ number. Millisecond end time
-- @return table [[],[],...]
function Bitfinex:FundingStats(symbol, limit_, start_, end_)
    local qParams = {}
    qParams['start'] = start_
    qParams['end'] = end_
    qParams['limit'] = limit_

    local qPath = 'funding/stats/' .. symbol .. '/hist'
    local fstats_raw = self:query_pub(qPath, qParams)
    local fstats = {}
    for i, t in ipairs(fstats_raw) do
        table.insert(fstats, i, {
            TIMESTAMP = tonumber(t[1]),
            FRR = tonumber(t[4]),
            AVG_PERIOD = tonumber(t[5]),
            FUNDING_AMOUNT = tonumber(t[8]),
            FUNDING_AMOUNT_USED = tonumber(t[9]),
            FUNDING_BELOW_THRESHOLD = tonumber(t[12])
        })
    end
    return fstats
end

--- Bitfinex:MarketAveragePrice.
-- Calculate the average execution price for Trading or rate for Margin funding.
-- @param symbol string. The symbol you want information about.
-- @param amount string. Amount. Positive for buy, negative for sell (ex. '1.123')
-- @param period number. (optional) Maximum period for Margin Funding
-- @param rate_limit string. Limit rate/price (ex. '1000.5')
-- @return table  [PRICE_AVG, AMOUNT], funding [RATE_AVG, AMOUNT]
function Bitfinex:MarketAveragePrice(symbol, amount, period, rate_limit)
    local qPath = 'calc/trade/avg'
    local calc_raw = self:query_pub(qPath, {
        symbol = symbol,
        amount = tostring(amount),
        period = period,
        rate_limit = rate_limit
    })
    local is_funding = string.sub(symbol, 1, 1) == 'f'
    if (is_funding) then
        return {
            RATE_AVG = calc_raw[1],
            AMOUNT = calc_raw[2]
        }
    else
        return {
            PRICE_AVG = calc_raw[1],
            AMOUNT = calc_raw[2]
        }
    end
end

--- Bitfinex:ForeignExchangeRate.
-- Calculate the exchange rate between two currencies
-- @param ccy1 string|table. First currency (base currency) or key-value table eg {ccy1 = 'BTC',ccy2 = 'USD'}, or {'BTC','USD'}
-- @param ccy2 string. Second currency (quote currency). nil, if ccy1 is k-v table.
-- @return table key-value [ CURRENT_RATE ]
function Bitfinex:ForeignExchangeRate(ccy1, ccy2)
    local qPath = 'calc/fx'
    local calc_raw = self:query_post_json(qPath, (type(ccy1) == 'string' and {
        ccy1 = ccy1,
        ccy2 = ccy2
    } or {
        ccy1 = ccy1[1],
        ccy2 = ccy1[2]
    }))
    print(json.encode(ccy1))
    return {
        CURRENT_RATE = calc_raw[1]
    }
end

Bitfinex.wspub = nil
Bitfinex.wsauth = nil
Bitfinex.wscbs = {}
Bitfinex.wschannels = {}
Bitfinex.wsfibers = {
    pubr = nil,
    pubw = nil,
    pubmsg = nil,
    authr = nil,
    authw = nil,
    authmsg = nil,
    wssubwatchdog = nil
}

function Bitfinex:restart()
    log.info('Bitfinex:restart')
    if (self.wspub ~= nil) then
        self.wspub_ready = false
        self.wsfibers.pubr:cancel()
        self.wsfibers.pubw:cancel()
        self.wsfibers.pubr = nil
        self.wsfibers.pubw = nil
        self.wspub:close()
        self:bootstrapws(true, false)
    end
    if (self.wsauth ~= nil) then
        self.wsauth_ready = false
        self.wsfibers.authr:cancel()
        self.wsfibers.authr = nil
        self.wsfibers.authw:cancel()
        self.wsfibers.authw = nil
        self.wsauth:close()
        self:bootstrapws(false, true)
    end
    self.wscbs = {}
    for k, ch in pairs(self.wschannels) do
        self.ch_wspub_send:put(json.encode(ch.req))
    end
end
Bitfinex.wsauth_config = {}
function Bitfinex:connectwsauth(filters, dms)
    Bitfinex.wsauth_config = {
        filters = filters or Bitfinex.wsauth_config.filters or nil,
        dms = filters or Bitfinex.wsauth_config.dms or nil
    }
    self:bootstrapws(false, true)
end

function Bitfinex:bootstrapws(pub, auth, max_try)
    max_try = max_try or 5
    self.wsfibers.wssubwatchdog = self.wsfibers.wssubwatchdog ~= nil and self.wsfibers.wssubwatchdog or
                                      fiber.create(self.fibers.wssubwatchdogfiber, self)
    if (pub == true) then
        for i = 1, max_try do
            log.info('Bitfinex pub ws.  N#' .. i)
            self.ch_wspub_recv = self.ch_wspub_recv ~= nil and self.ch_wspub_recv or fiber.channel()
            self.ch_wspub_send = (self.ch_wspub_send ~= nil and self.ch_wspub_send or fiber.channel())
            self.wsfibers.pubr = self.wsfibers.pubr ~= nil and self.wsfibers.pubr or
                                     fiber.create(self.fibers.wspubrfiber, self)
            self.wsfibers.pubw = self.wsfibers.pubw ~= nil and self.wsfibers.pubw or
                                     fiber.create(self.fibers.wspubwfiber, self)
            self.wsfibers.pubmsg = self.wsfibers.pubmsg ~= nil and self.wsfibers.pubmsg or
                                       fiber.create(self.fibers.on_pub_message_fiber, self)
            local wspub, err = websocket.connect(self.API_URL.WS.PUBLIC, nil, {
                timeout = 5
            })
            if (err ~= nil) then
                log.error(err)
            else
                self.wspub = wspub
                break
            end
        end
    end
    if (auth == true) then
        for i = 1, max_try do
            log.info('Bitfinex auth ws. N#' .. i)
            if (self.credentials.apikey ~= nil and self.credentials.apisecret ~= nil) then
                self.ch_wsauth_recv = self.ch_wsauth_recv ~= nil and self.ch_wsauth_recv or fiber.channel()
                self.ch_wsauth_send = self.ch_wsauth_send ~= nil and self.ch_wsauth_send or fiber.channel()
                self.wsfibers.authr = self.wsfibers.authr ~= nil and self.wsfibers.authr or
                                          fiber.create(self.fibers.wsauthrfiber, self)
                self.wsfibers.authw = self.wsfibers.authw ~= nil and self.wsfibers.authw or
                                          fiber.create(self.fibers.wsauthwfiber, self)
                self.wsfibers.authmsg = self.wsfibers.authmsg ~= nil and self.wsfibers.authmsg or
                                            fiber.create(self.fibers.on_auth_message_fiber, self)
                local wsauth, err = websocket.connect(self.API_URL.WS.AUTH, nil, {
                    timeout = 5
                })
                if (err ~= nil) then
                    print(err)
                else
                    self.wsauth = wsauth
                    local nonce = tostring(self.utils.gettime())
                    local authPayload = 'AUTH' .. nonce
                    local authSig = self.makeSig(authPayload)
                    self.wsauth:write(json.encode({
                        apiKey = self.credentials.apikey,
                        authSig = authSig,
                        authNonce = nonce,
                        authPayload = authPayload,
                        event = 'auth',
                        filters = Bitfinex.wsauth_config.filters or nil,
                        dms = Bitfinex.wsauth_config.dms or nil
                    }))
                    break
                end
            end
        end
    end
    return self.wspub ~= nil, self.wsauth ~= nil
end

function Bitfinex.fibers.wssubwatchdogfiber(bfx)
    local msg, error, new_msg
    log.info('Bitfinex.fibers.wssubwatchdogfiber(bfx)')
    while true do
        fiber.sleep(2)
        for k, ch in pairs(bfx.wschannels) do
            fiber.sleep(0.05)
            if (tonumber(ch.lasthb) < tonumber(bfx.utils.gettime(-20))) then
                log.warn('Channel ' .. k .. ' heartbeat outdated: ' ..
                             math.ceil((tonumber(bfx.utils.gettime()) - tonumber(ch.lasthb)) / 10000) .. ' secs')
                log.warn('Call bfx:restart()...')
                bfx:restart()
            end
        end
    end
end

function Bitfinex.fibers.wspubrfiber(bfx)
    local msg, error
    while true do
        if (bfx.wspub ~= nil) then
            msg, error = bfx.wspub:read(1) -- TODO: log error
            if (error ~= nil and error ~= 'Connection timed out') then
                log.error(' ERROR WS:read():' .. error)
            end
            if (msg ~= nil) then
                bfx.ch_wspub_recv:put(msg)
            end
            fiber.sleep(0.01)
        else
            fiber.sleep(0.01)
        end
    end
end

function Bitfinex.fibers.wspubwfiber(bfx)
    local new_msg
    while true do
        if (bfx.wspub ~= nil) then
            if (bfx.wspub_ready) then
                new_msg = bfx.ch_wspub_send:get(0.005)
                if (new_msg ~= nil) then
                    bfx.wspub:write(new_msg)
                end
            end
            fiber.sleep(0.01)
        else
            fiber.sleep(0.1)
        end
    end
end

function Bitfinex.fibers.wsauthrfiber(bfx)
    local msg
    while true do
        if (bfx.wsauth ~= nil) then
            msg, error = bfx.wsauth:read(0.5) -- TODO: log error
            if (msg ~= nil) then
                bfx.ch_wsauth_recv:put(msg)
            end
            fiber.sleep(0.01)
        else
            fiber.sleep(0.1)
        end
    end
end

function Bitfinex.fibers.wsauthwfiber(bfx)
    local new_msg
    while true do
        if (bfx.wsauth ~= nil) then
            if (bfx.wsauth_ready) then
                new_msg = bfx.ch_wsauth_send:get(0.005)
                if (new_msg ~= nil) then
                    bfx.wsauth:write(new_msg)
                end
            end
            fiber.sleep(0.01)
        else
            fiber.sleep(0.1)
        end
    end
end

function Bitfinex:_schtotbl(str)
    local SCH = {}
    str = str:gsub('[^%w_,]', '')
    print(str)
    str:gsub('([^,]+)', function(c)
        table.insert(SCH, c)
    end)
    return SCH
end

function Bitfinex._schPI(arr, sch)
    local res = {}
    for i, v in ipairs(arr) do
        res[(string.find(sch[i], 'PLACEHOLDER') and i or sch[i])] = v or nil
    end
    return res
end

function Bitfinex._schPA(arr, sch)
    local res = {}
    for i, t in ipairs(arr) do
        table.insert(res, i, Bitfinex._schPI(t, sch))
    end
    return res
end

function Bitfinex:_parsewsauth(raw_m)
    local m = json.decode(raw_m)
    if (m.event ~= nil and m.event == 'auth') then
        if (m.status == 'OK') then
            self.AuthConn.userId = m.userId
            self.AuthConn.caps = m.caps
            return m
        else
            log.warn('Auth status != OK: ' .. raw_m)
            return nil
        end
    end
    if (type(m[2]) == 'string') then
        if (string.match(m[2], '([os|on|ou|oc]+)$')) then -- orders
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'os') then
                sch = self:_schtotbl([[ID,
                GID,
                CID,
                SYMBOL,
                MTS_CREATE, 
                MTS_UPDATE, 
                AMOUNT, 
                AMOUNT_ORIG, 
                ORDER_TYPE,
                TYPE_PREV,
                MTS_TIF,
                _PLACEHOLDER,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                PRICE,
                PRICE_AVG,
                PRICE_TRAILING,
                PRICE_AUX_LIMIT,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                NOTIFY,
                HIDDEN, 
                PLACED_ID,
                _PLACEHOLDER,
                _PLACEHOLDER,
                ROUTING,
                _PLACEHOLDER,
                _PLACEHOLDER,
                META]])
            else
                sch = self:_schtotbl([[ID, 
                GID,
                CID,
                SYMBOL, 
                MTS_CREATE, 
                MTS_UPDATE, 
                AMOUNT, 
                AMOUNT_ORIG, 
                ORDER_TYPE,
                TYPE_PREV,
                MTS_TIF,
                _PLACEHOLDER,
                FLAGS,
                ORDER_STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                PRICE,
                PRICE_AVG,
                PRICE_TRAILING,
                PRICE_AUX_LIMIT,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                NOTIFY, 
                HIDDEN, 
                PLACED_ID,
                _PLACEHOLDER,
                _PLACEHOLDER,
                ROUTING,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER]])
            end
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'orders'
            return o
        end

        if (string.match(m[2], '([ps|pn|pu|pc]+)$')) then -- positions
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'ps') then
                sch = self:_schtotbl([[SYMBOL, 
                STATUS, 
                AMOUNT, 
                BASE_PRICE, 
                MARGIN_FUNDING, 
                MARGIN_FUNDING_TYPE,
                PL,
                PL_PERC,
                PRICE_LIQ,
                LEVERAGE,
                FLAG,
                POSITION_ID,
                MTS_CREATE,
                MTS_UPDATE,
                PLACEHOLDER,
                TYPE,
                PLACEHOLDER,
                COLLATERAL,
                COLLATERAL_MIN,
                META]])
            else
                sch = self:_schtotbl([[SYMBOL, 
                STATUS, 
                AMOUNT, 
                BASE_PRICE, 
                MARGIN_FUNDING, 
                MARGIN_FUNDING_TYPE,
                PL,
                PL_PERC,
                PRICE_LIQ,
                LEVERAGE,
                FLAG,
                POSITION_ID,
                MTS_CREATE,
                MTS_UPDATE,
                PLACEHOLDER,
                TYPE,
                PLACEHOLDER,
                COLLATERAL,
                COLLATERAL_MIN,
                META]])
            end
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'positions'
            return o
        end

        if (string.match(m[2], '([te|tu]+)$')) then -- trades
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'te') then
                sch = self:_schtotbl([[    ID, 
                SYMBOL, 
                MTS_CREATE,
                ORDER_ID, 
                EXEC_AMOUNT, 
                EXEC_PRICE, 
                ORDER_TYPE, 
                ORDER_PRICE, 
                MAKER,
                PLACEHOLDER,
                PLACEHOLDER,
                    CID]])
            else
                sch = self:_schtotbl([[ID, 
                SYMBOL, 
                MTS_CREATE, 
                ORDER_ID, 
                EXEC_AMOUNT, 
                EXEC_PRICE, 
                ORDER_TYPE, 
                ORDER_PRICE, 
                MAKER, 
                FEE, 
                FEE_CURRENCY,
                CID]])
            end
            table.insert(o, self._schPI(m[3], sch))
            o.CHANNEL = 'trades'
            return o
        end

        if (string.match(m[2], '([fos|fon|fou|foc]+)$')) then -- funding_offers
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'fos') then
                sch = self:_schtotbl([[ID,
                SYMBOL,
                MTS_CREATED,
                MTS_UPDATED,
                AMOUNT,
                AMOUNT_ORIG,
                OFFER_TYPE,
                _PLACEHOLDER,
                _PLACEHOLDER,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD,
                NOTIFY,
                HIDDEN,
                _PLACEHOLDER,
                RENEW,
                _PLACEHOLDER]])
            else
                sch = self:_schtotbl([[ID,
                SYMBOL,
                MTS_CREATED,
                MTS_UPDATED,
                AMOUNT,
                AMOUNT_ORIG,
                TYPE,
                _PLACEHOLDER,
                _PLACEHOLDER,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD,
                NOTIFY,
                HIDDEN,
                _PLACEHOLDER,
                RENEW,
                RATE_REAL]])
            end
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'funding_offers'
            return o
        end

        if (string.match(m[2], '([fcs|fcn|fcu|fcc]+)$')) then -- funding_credits
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'fcs') then
                sch = self:_schtotbl([[ID,
                SYMBOL,
                SIDE,
                MTS_CREATE,
                MTS_UPDATE,
                AMOUNT,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD,
                MTS_OPENING,
                MTS_LAST_PAYOUT,
                NOTIFY,
                HIDDEN,
                _PLACEHOLDER,
                RENEW,
                RATE_REAL,
                NO_CLOSE,
                POSITION_PAIR]])
            else
                sch = self:_schtotbl([[ID, 
                SYMBOL, 
                SIDE, 
                MTS_CREATE, 
                MTS_UPDATE, 
                AMOUNT, 
                FLAGS, 
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD, 
                MTS_OPENING, 
                MTS_LAST_PAYOUT, 
                NOTIFY, 
                HIDDEN, 
                _PLACEHOLDER,
                RENEW,
                RATE_REAL, 
                NO_CLOSE, 
                POSITION_PAIR]])
            end
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'funding_credits'
            return o
        end

        if (string.match(m[2], '([fls|fln|flu|flc]+)$')) then -- funding_loans
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[2] == 'fls') then
                sch = self:_schtotbl([[ID,
                SYMBOL,
                SIDE,
                MTS_CREATE,
                MTS_UPDATE,
                AMOUNT,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD,
                MTS_OPENING,
                MTS_LAST_PAYOUT,
                NOTIFY,
                HIDDEN,
                _PLACEHOLDER,
                RENEW,
                RATE_REAL,
                NO_CLOSE]])
            else
                sch = self:_schtotbl([[    ID,
                CURRENCY,
                SIDE,
                MTS_CREATE,
                MTS_UPDATE,
                AMOUNT,
                FLAGS,
                STATUS,
                _PLACEHOLDER,
                _PLACEHOLDER,
                _PLACEHOLDER,
                RATE,
                PERIOD,
                MTS_OPENING,
                MTS_LAST_PAYOUT,
                NOTIFY,
                HIDDEN,
                _PLACEHOLDER,
                RENEW,
                RATE_REAL,
                NO_CLOSE]])
            end
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'funding_loans'
            return o
        end

        if (string.match(m[2], '([ws|wu]+)$')) then -- wallets
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch = self:_schtotbl([[WALLET_TYPE, 
                CURRENCY, 
                BALANCE, 
                UNSETTLED_INTEREST,
                BALANCE_AVAILABLE,
                DESCRIPTION,
                META]])
            if (type(m[3][1]) == 'table') then
                table.insert(o, self._schPA(m[3], sch))
            else
                table.insert(o, self._schPI(m[3], sch))
            end
            o.CHANNEL = 'wallets'
            return o
        end

        if (string.match(m[2], '([bu]+)$')) then -- balance_info
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch = self:_schtotbl([[AUM,
            AUM_NET]])
            table.insert(o, self._schPI(m[3], sch))
            o.CHANNEL = 'balance_info'
            return o
        end

        if (string.match(m[2], '([miu]+)$')) then -- margin_info
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch
            if (m[3][1] == 'base') then
                o[3][1] = 'base'
                o.sch = self:_schtotbl([[USER_PL, 
                USER_SWAPS, 
                MARGIN_BALANCE, 
                MARGIN_NET,
                MARGIN_REQUIRED]])
            else
                o[3][1] = 'sym'
                o[3][2] = m[3][2]
                sch = self:_schtotbl([[TRADABLE_BALANCE,
                GROSS_BALANCE,
                BUY,
                SELL]])
            end

            table.insert(o, self._schPI(m[3][3], sch))
            o.CHANNEL = 'margin_info'
            return o
        end

        if (string.match(m[2], '([fiu]+)$')) then -- funding_info
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            o[3][1] = 'sym'
            o[3][2] = m[3][2]
            local sch = self:_schtotbl([[YIELD_LOAN,
            YIELD_LEND,
            DURATION_LOAN,
            DURATION_LEND]])
            table.insert(o, self._schPI(m[3][3], sch))
            o.CHANNEL = 'funding_info'
            return o
        end

        if (string.match(m[2], '([fte|ftu]+)$')) then -- funding_trades
            local o = {
                CHAN_ID = 0,
                TYPE = m[2]
            }
            local sch = self:_schtotbl([[ID,
            CURRENCY,
            MTS_CREATE,
            OFFER_ID,
            AMOUNT,
            RATE,
            PERIOD,
            MAKER]])
            table.insert(o, self._schPI(m[3], sch))
            o.CHANNEL = 'funding_trades'
            return o
        end

        if (string.match(m[2], '([n|on-rec|oc-req|uca|price|fon-req|foc-req]+)$')) then -- notifications TODO: BFX API:'This section (Notifications) is currently a work in progress, but it will be a way to be alerted as to different changes in status, price alerts, etc'
            -- https://docs.bitfinex.com/reference#ws-auth-notifications
            m.channel = 'notifications'
            return m
        end
        return m

    end
end

function Bitfinex.fibers.on_auth_message_fiber(bfx)
    while true do
        local msg = bfx.ch_wsauth_recv:get(0.001)
        if (msg ~= nil) then
            log.info(msg.data)
            if (type(bfx.ws_authenticated_cb) == 'function') then
                bfx.ws_authenticated_cb(bfx:_parsewsauth(msg))
            end
        end
        fiber.sleep(0.001)
    end
end

function Bitfinex.fibers.on_pub_message_fiber(bfx)
    while true do
        local msg = bfx.ch_wspub_recv:get(0.001)
        if (msg ~= nil) then
            bfx:onwspubmsg(msg)
        end
        fiber.sleep(0.001)
    end
end

function Bitfinex._parsewspub(r, ch)
    if (type(r) == 'string') then

    end
    local o = {}
    if (ch.ci.channel == 'ticker') then
        local isf = string.sub(ch.ci.symbol, 1, 1) == 'f'
        if (isf) then
            o.CHANNEL_ID = r[1]
            o.FRR = r[2][1]
            o.BID = r[2][2]
            o.BID_PERIOD = r[2][3]
            o.BID_SIZE = r[2][4]
            o.ASK = r[2][5]
            o.ASK_PERIOD = r[2][6]
            o.ASK_SIZE = r[2][7]
            o.DAILY_CHANGE = r[2][8]
            o.DAILY_CHANGE_RELATIVE = r[2][9]
            o.LAST_PRICE = r[2][10]
            o.VOLUME = r[2][11]
            o.HIGH = r[2][12]
            o.LOW = r[2][13]
            o[15] = r[2][14]
            o[16] = r[2][15]
            o.FRR_AMOUNT_AVAILABLE = r[2][16]
            return o
        else
            o.CHANNEL_ID = r[1]
            o.BID = r[2][1]
            o.BID_SIZE = r[2][2]
            o.ASK = r[2][3]
            o.ASK_SIZE = r[2][4]
            o.DAILY_CHANGE = r[2][5]
            o.DAILY_CHANGE_RELATIVE = r[2][6]
            o.LAST_PRICE = r[2][7]
            o.VOLUME = r[2][8]
            o.HIGH = r[2][9]
            o.LOW = r[2][10]
            return o
        end
    end

    if (ch.ci.channel == 'trades') then
        local isf = string.sub(ch.ci.symbol, 1, 1) == 'f'
        local ai = type(r[2]) == 'string' and 3 or 2
        o[2] = (type(r[2]) == 'string' and r[2] or nil)
        o[ai] = {}
        o.CHANNEL_ID = r[1]
        if (isf) then
            if (type(r[2]) == 'string') then
                table.insert(o, ai, {
                    ID = r[ai][1],
                    MTS = r[ai][2],
                    AMOUNT = r[ai][3],
                    PRICE = r[ai][4],
                    PERIOD = r[ai][5]
                })
            else
                for p, rr in ipairs(r[ai]) do
                    table.insert(o[ai], {
                        ID = rr[1],
                        MTS = rr[2],
                        AMOUNT = rr[3],
                        RATE = rr[4],
                        PERIOD = rr[5]
                    })
                end
            end
            return o
        else
            if (type(r[2]) == 'string') then
                table.insert(o, ai, {
                    ID = r[ai][1],
                    MTS = r[ai][2],
                    AMOUNT = r[ai][3],
                    PRICE = r[ai][4]
                })
            else
                print(ai)
                for p, rr in ipairs(r[ai]) do
                    table.insert(o[ai], {
                        ID = rr[1],
                        MTS = rr[2],
                        AMOUNT = rr[3],
                        PRICE = rr[4]
                    })
                end
            end
            return o
        end
    end

    if (ch.ci.channel == 'book' and ch.ci['prec'] ~= 'R0') then
        local isf = string.sub(ch.ci.symbol, 1, 1) == 'f'
        if (isf) then
            o.CHANNEL_ID = r[1]
            o[2] = {}
            if (type(r[2][1]) == 'table') then
                for p, rr in ipairs(r[2]) do
                    table.insert(o[2], {
                        RATE = rr[1],
                        PERIOD = rr[2],
                        COUNT = rr[3],
                        AMOUNT = rr[4]
                    })
                end
            else
                o[2].RATE = r[2][1]
                o[2].PERIOD = r[2][2]
                o[2].COUNT = r[2][3]
                o[2].AMOUNT = r[2][4]
            end
            return o
        else
            o.CHANNEL_ID = r[1]
            o[2] = {}
            if (type(r[2][1]) == 'table') then
                for p, rr in ipairs(r[2]) do
                    table.insert(o[2], {
                        PRICE = rr[1],
                        COUNT = rr[2],
                        AMOUNT = rr[3]
                    })
                end
            else
                o[2].PRICE = r[2][1]
                o[2].COUNT = r[2][2]
                o[2].AMOUNT = r[2][3]
            end
            return o
        end
    end

    if (ch.ci.channel == 'book' and ch.ci['prec'] == 'R0') then
        local isf = string.sub(ch.ci.symbol, 1, 1) == 'f'
        if (isf) then
            o.CHANNEL_ID = r[1]
            o[2] = {}
            if (type(r[2][1]) == 'table') then
                for p, rr in ipairs(r[2]) do
                    table.insert(o[2], {
                        OFFER_ID = rr[1],
                        PERIOD = rr[2],
                        RATE = rr[3],
                        AMOUNT = rr[4]
                    })
                end
            else
                o[2].OFFER_ID = r[2][1]
                o[2].PERIOD = r[2][2]
                o[2].RATE = r[2][3]
                o[2].AMOUNT = r[2][4]
            end
            return o
        else
            o.CHANNEL_ID = r[1]
            o[2] = {}
            if (type(r[2][1]) == 'table') then
                for p, rr in ipairs(r[2]) do
                    table.insert(o[2], {
                        ORDER_ID = rr[1],
                        PRICE = rr[2],
                        AMOUNT = rr[3]
                    })
                end
            else
                o[2].ORDER_ID = r[2][1]
                o[2].PRICE = r[2][2]
                o[2].AMOUNT = r[2][3]
            end
            return o
        end
    end

    if (ch.ci.channel == 'candles') then
        o[2] = {}
        o.CHANNEL_ID = r[1]
        if (type(r[2][1]) == 'table') then
            for p, rr in ipairs(r[2]) do
                table.insert(o[2], {
                    MTS = rr[1],
                    OPEN = rr[2],
                    CLOSE = rr[3],
                    HIGH = rr[4],
                    LOW = rr[5],
                    VOLUME = rr[6]
                })
            end
        else
            o[2] = {
                MTS = r[2][1],
                OPEN = r[2][2],
                CLOSE = r[2][3],
                HIGH = r[2][4],
                LOW = r[2][5],
                VOLUME = r[2][6]
            }
        end
        return o
    end

    if (ch.ci.channel == 'status') then
        local d = string.sub(ch.ci.key, 1, 1) == 'd'
        o.CHANNEL_ID = r[1]
        if (d) then
            o.TIME_MS = r[2][1]
            o[2] = r[2][2]
            o.DERIV_PRICE = r[2][3]
            o.SPOT_PRICE = r[2][4]
            o[5] = r[2][5]
            o.INSURANCE_FUND_BALANCE = r[2][6]
            o[7] = r[2][7]
            o.NEXT_FUNDING_EVT_TIMESTAMP_MS = r[2][8]
            o.NEXT_FUNDING_ACCRUED = r[2][9]
            o.NEXT_FUNDING_STEP = r[2][10]
            o[11] = r[2][11]
            o.CURRENT_FUNDING = r[2][12]
            o[13] = r[2][13]
            o[14] = r[2][14]
            o.MARK_PRICE = r[2][15]
            o[16] = r[2][16]
            o[17] = r[2][17]
            o.OPEN_INTEREST = r[2][18]
            o[19] = r[2][19]
            o[20] = r[2][20]
            o[21] = r[2][21]
            o.CLAMP_MIN = r[2][22]
            o.CLAMP_MAX = r[2][23]
            return o
        else
            o['pos'] = r[2][1][1]
            o.POS_ID = r[2][1][2]
            o.TIME_MS = r[2][1][3]
            o[4] = r[2][1][4]
            o.SYMBOL = r[2][1][5]
            o.AMOUNT = r[2][1][6]
            o.BASE_PRICE = r[2][1][7]
            o[8] = r[2][1][8]
            o.IS_MATCH = r[2][1][9]
            o.IS_MARKET_SOLD = r[2][1][10]
            o[11] = r[2][1][11]
            o.LIQUIDATION_PRICE = r[2][1][12]
            return o
        end
    end
end

function Bitfinex:onwspubmsg(msg)
    local m = json.decode(msg.data)
    -- print(msg.data)
    if (m.event ~= nil) then
        if (m.event == 'info') then
            self.wspub_ready = tonumber(m.platform.status) ~= 0
        end
        if (m.event == 'auth') then
            self.wsauth_ready = m.status == 'OK'
        end
        if (m.event == 'subscribed') then
            table.insert(self.wschannels, m.chanId, {
                ci = m,
                cb = self.wscbs[(m.channel .. (m['symbol'] or m['key']))].callback,
                req = self.wscbs[(m.channel .. (m['symbol'] or m['key']))].req,
                lasthb = self.utils.gettime()
            })
            -- end

        end
    elseif (type(m[1]) == 'number') then
        if (tonumber(m[1]) == 0) then -- authenticated channel

        end
        if (m[2] == 'hb') then
            if (self.wschannels[m[1]] ~= nil) then
                self.wschannels[m[1]].lasthb = tostring(self.utils.gettime())
                return
            end
        end
        -- log.error('if (type(m[1]) == 'number') then')
        if (self.wschannels[m[1]] ~= nil) then
            self.wschannels[m[1]].cb(self._parsewspub(m, self.wschannels[m[1]])) -- TODO
        end
    end
end

function Bitfinex:wssend_auth(payload)
    if (self.wsauth == nil) then
        self:connectwsauth()
    end
    payload.event = 'subscribe'
    self.ch_wsauth_send:put(json.encode(payload))
end

function Bitfinex:subscribe_pub(payload)
    if (self.wspub == nil) then
        self:bootstrapws(true, false)
    end
    payload.event = 'subscribe'
    local payload_json = json.encode(payload)
    log.warn('Bitfinex:subscribe_pub: ' .. payload_json)
    self.ch_wspub_send:put(payload_json)
end

--- Bitfinex:wsTicker.
-- The ticker endpoint provides a high level overview of the state of the market for a specified pair. It shows the current best bid and ask, the last traded price, as well as information on the daily volume and price movement over the last day.
-- @param symbol string. Trading pair or funding currency
-- @param callback function. Callback function for trades channel messages
function Bitfinex:wsTicker(symbol, callback)
    local req = {
        symbol = symbol,
        channel = 'ticker'
    }
    self.wscbs[('ticker' .. symbol)] = {
        callback = callback,
        req = req
    }
    self:subscribe_pub(req)
end

--- Bitfinex:wsTrades.
-- This channel sends a trade message whenever a trade occurs at Bitfinex. It includes all the pertinent details of the trade, such as price, size and the time of execution. The channel can send funding trade data as well.
-- @param symbol string. Trading pair or funding currency
-- @param callback function. Callback function for trades channel messages
function Bitfinex:wsTrades(symbol, callback)
    local req = {
        symbol = symbol,
        channel = 'trades'
    }
    self.wscbs[('trades' .. symbol)] = {
        callback = callback,
        req = req
    }
    self:subscribe_pub(req)
end

--- Bitfinex:wsBooks.
-- The Order Books channel allows you to keep track of the state of the Bitfinex order book. It is provided on a price aggregated basis with customizable precision. Upon connecting, you will receive a snapshot of the book
-- @param symbol string. Trading pair or funding currency
-- @param precision string. Level of price aggregation (P0, P1, P2, P3, P4). The default is P0
-- @param frequency string. Frequency of updates (F0, F1). F0=realtime / F1=2sec. The default is F0.
-- @param length string. Number of price points ("1", "25", "100", "250") [default="25"]
-- @param subid string. Optional user-defined ID for the subscription
-- @param callback function. Callback function for trades channel messages
function Bitfinex:wsBooks(symbol, precision, frequency, length, subid, callback)
    local req = {
        symbol = symbol,
        channel = 'book',
        prec = precision,
        freq = frequency,
        len = length,
        subId = subid
    }
    self.wscbs[('book' .. symbol)] = {
        callback = callback,
        req = req
    }
    self:subscribe_pub(req)
end

--- Bitfinex:wsCandles.
-- The Candles endpoint provides OCHL (Open, Close, High, Low) and volume data for the specified trading pair.
-- @param symbol string. Trading pair or funding currency
-- @param timeframe string.
-- @param aaggr string.
-- @param pper_start string.
-- @param pend string.
-- @param callback function. Callback function for trades channel messages
function Bitfinex:wsCandles(symbol, timeframe, aaggr, pper_start, pend, callback)
    local key = 'trade:' .. timeframe .. ':' .. symbol
    key = key .. (aaggr ~= nil and ':' .. aaggr or '')
    key = key .. (pper_start ~= nil and ':' .. pper_start or '')
    key = key .. (pend ~= nil and ':' .. pend or '')
    local req = {
        key = key,
        channel = 'candles'
    }
    self.wscbs[('candles' .. key)] = {
        callback = callback,
        req = req
    }
    self:subscribe_pub(req)
end

--- Bitfinex:wsStatus
-- Subscribe to and receive different types of platform information - currently supports derivatives pair status and liquidation feed.
-- https://docs.bitfinex.com/reference#ws-public-status
-- @param key string. The value of the key, contained in the subscription payload, determines what status data will be returned. key: "deriv:SYMBOL" // e.g. "deriv:tBTCF0:USTF0" or key: "liq:global" etc
-- @param callback function. Callback function for trades channel messages
function Bitfinex:wsStatus(key, callback)
    local req = {
        key = key,
        channel = 'status'
    }
    self.wscbs[('candles' .. key)] = {
        callback = callback,
        req = req
    }
    self:subscribe_pub(req)
end

--- Bitfinex:wsNewOrder
-- Creates a new order, can be used to create margin, exchange, and derivative orders.
-- https://docs.bitfinex.com/reference#ws-auth-input-order-new
-- @param gid number|table. (optional) Group id for the order or function args in one table, like {gid=GID, cid=CID, ...}
-- @param cid number. Should be unique in the day (UTC) (not enforced)
-- @param type_ string. The type of the order: LIMIT, EXCHANGE LIMIT, MARKET, EXCHANGE MARKET, STOP, EXCHANGE STOP, STOP LIMIT, EXCHANGE STOP LIMIT, TRAILING STOP, EXCHANGE TRAILING STOP, FOK, EXCHANGE FOK, IOC, EXCHANGE IOC.
-- @param symbol string. symbol (tBTCUSD, tETHUSD, ...)
-- @param amount int string. Positive for buy, Negative for sell
-- @param price int string. Price (Not required for market orders)
-- @param lev number. Set the leverage for a derivative order, supported by derivative symbol orders only. The value should be between 1 and 100 inclusive. The field is optional, if omitted the default leverage value of 10 will be used.
-- @param price_trailing int string. The trailing price
-- @param price_aux_limit int string. Auxiliary Limit price (for STOP LIMIT)
-- @param price_oco_stop int string. OCO stop price
-- @param flags number int16. See https://docs.bitfinex.com/v2/docs/flag-values.
-- @param tif datetime string. Time-In-Force: datetime for automatic order cancellation (ie. 2020-01-01 10:45:23) )
-- @param meta table. The meta object allows you to pass along an affiliate code inside the object - example: meta: {aff_code: 'AFF_CODE_HERE'}
function Bitfinex:wsNewOrder(gid, cid, type_, symbol, amount, price, lev, price_trailing, price_aux_limit,
    price_oco_stop, flags, tif, meta)
    local payload = {}
    if (type(gid) ~= 'table') then
        payload.gid = gid
        payload.cid = cid
        payload['type'] = type_
        payload.symbol = symbol
        payload.amount = amount
        payload.price = price
        payload.lev = lev
        payload.price_trailing = price_trailing
        payload.price_aux_limit = price_aux_limit
        payload.price_oco_stop = price_oco_stop
        payload.flags = flags
        payload.tif = tif
        payload.meta = meta
    else
        payload = gid
        payload['type'] = payload.type_ or payload['type'] or nil
    end
    self:wssend_auth({0, 'on', nil, payload})
end

--- Bitfinex:wsUpdateOrder
-- Update an existing order, can be used to update margin, exchange, and derivative orders.
-- https://docs.bitfinex.com/reference#ws-auth-input-order-update
-- @param id number|table. Order ID or function args in one table, like {id=ORDER_ID,delta=DELTA, ...}
-- @param gid number|table. Group Order ID
-- @param cid number. Client Order ID
-- @param cid_date string. Client Order ID Date
-- @param amount int string. Positive for buy, Negative for sell
-- @param price int string. Price (Not required for market orders)
-- @param lev number. Set the leverage for a derivative order, supported by derivative symbol orders only. The value should be between 1 and 100 inclusive. The field is optional, if omitted the default leverage value of 10 will be used.
-- @param delta int string. Change of amount
-- @param price_trailing int string. The trailing price
-- @param price_aux_limit int string. Auxiliary Limit price (for STOP LIMIT)
-- @param flags number int16. See https://docs.bitfinex.com/v2/docs/flag-values.
-- @param tif datetime string. Time-In-Force: datetime for automatic order cancellation (ie. 2020-01-01 10:45:23) )
function Bitfinex:wsUpdateOrder(id, gid, cid, cid_date, amount, price, lev, delta, price_trailing, price_aux_limit,
    flags, tif)
    local payload = {}
    if (type(gid) ~= 'table') then
        payload.id = id
        payload.gid = gid
        payload.cid = cid
        payload.cid_date = cid_date
        payload.amount = amount
        payload.price = price
        payload.lev = lev
        payload.delta = delta
        payload.price_trailing = price_trailing
        payload.price_aux_limit = price_aux_limit
        payload.flags = flags
        payload.tif = tif
    else
        payload = id
    end
    self:wssend_auth({0, 'ou', nil, payload})
end

--- Bitfinex:wsNewOrderMultiOp
-- Send multiple order-related operations. (Max 75 operations per request)
-- https://docs.bitfinex.com/reference#ws-auth-input-order-multi-op
-- @param op table. the inner array to specify the different operations you would like to perform, starting each separate operation with the appropriate abbreviation followed by an object holding the details.
function Bitfinex:wsNewOrderMultiOp(op)
    self:wssend_auth({0, 'ox_multi', nil, op})
end

--- Bitfinex:wsCancelOrder
-- To cancel an order, construct a payload using the 'oc' (order cancel) abbreviation. The order details need to include either the Internal Order ID or the Client Order ID and Client Order ID Date (YYYY-MM-DD format).
-- https://docs.bitfinex.com/reference#ws-auth-input-order-cancel
-- @param id number. Internal Order ID
-- @param cid number. Client Order ID
-- @param cid_date string. Client Order ID Date
function Bitfinex:wsCancelOrder(id, cid, cid_date)
    self:wssend_auth({0, 'oc', nil, {
        id = id,
        cid = cid,
        cid_date = cid_date
    }})
end

--- Bitfinex:wsCancelOrderMulti
-- The Cancel Order Multi command allows sending of multiple order cancellation operations at the same time. To do this, construct a payload with the 'oc_multi' (order cancel multi) abbreviation. The payload needs to include an object that specifies which orders to cancel based on Internal Order ID's, Client Order ID's (along with the cid_DATE in a YYYY-MM-DD format), Group ID, or the 'all' request field. Please look to the sidebar for examples of each method.
-- https://docs.bitfinex.com/reference#ws-auth-input-order-cancel-multi
-- @param payload table.   {'id'={ID, 1234, ...}} or {'cid'= {{ID, CID_DATE},{234, '2016-12-05'},}} or {'gid'= {GID, 11, ...}}
function Bitfinex:wsCancelOrderMulti(payload)
    self:wssend_auth({0, 'oc_multi', nil, payload})
end

--- Bitfinex:wsNewOffer
-- Create a new funding offer.
-- https://docs.bitfinex.com/reference#ws-auth-input-offer-new
-- @param type_ string|table. LIMIT, FRRDELTAVAR, FRRDELTAFIX or key-value table with function args like {type_='LIMIT', symbol='fUSD, amount='-1.234', rate='0.0002', period=7,flags=0} 
-- @param symbol string. Symbol (fUSD, fBTC, ...)
-- @param amount int string. Amount (> 0 for offer, < 0 for bid)
-- @param rate int string. Rate (or offset for FRRDELTA offers)
-- @param period int. Time period of offer. Minimum 2 days. Maximum 120 days.
-- @param flags number. See https://docs.bitfinex.com/v2/docs/flag-values.
function Bitfinex:wsNewOffer(type_, symbol, amount, rate, period, flags)
    local payload = {}
    if (type(type_) ~= 'table') then
        payload['type'] = payload.type_ or payload['type'] or nil
        payload.symbol = symbol
        payload.amount = amount
        payload.rate = rate
        payload.period = period
        payload.flags = flags
    else
        payload = type_
        payload['type'] = payload.type_ or payload['type'] or nil
    end
    self:wssend_auth({0, 'fon', nil, payload})
end

--- Bitfinex:wsCancelOffer
-- Cancel a funding offer.
-- https://docs.bitfinex.com/reference#ws-auth-input-offer-cancel
-- @param id number. Offer ID
function Bitfinex:wsCancelOffer(id)
    self:wssend_auth({0, 'foc', nil, {
        id = id
    }})
end

--- Bitfinex:wsCalc
-- Send calc requests to trigger specific calculations.
-- https://docs.bitfinex.com/reference#ws-auth-input-calc
-- @param req table. Available Calc Requests: [['margin_base'],['margin_sym_tBTCUSD'],['funding_sym_fUSD'],['position_tBTCUSD'],['wallet_margin_BTC'],['balance']]
function Bitfinex:wsCalc(req)
    self:wssend_auth({0, 'calc', nil, req})
end

return Bitfinex
