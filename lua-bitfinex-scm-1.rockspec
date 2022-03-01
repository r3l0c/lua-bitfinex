package = "lua-bitfinex"
version = "scm-1"
source = {
   url = "git+https://github.com/r3l0c/lua-bitfinex.git",
   branch = 'master',
}
description = {
   summary = "Bitfinex API client implementation for tarantool.",
   detailed = [[
Public REST: full support, WebSocket full support.
   ]],
   homepage = "https://github.com/r3l0c/lua-bitfinex",
   license = "MIT"
}
dependencies = {
   "lua >= 5.1",
   "websocket",
}
build = {
    type = "builtin",
    modules = {
        ["lua-bitfinex"] = "lua-bitfinex.lua",
    },
}