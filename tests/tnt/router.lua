#!/usr/bin/env tarantool

require('strict').on()

local math = require('math')
local os = require('os')
local vshard = require('vshard')

local function getenvint(envname)
	local v = tonumber(os.getenv(envname) or 0)
	v = math.floor(v or 0)
	assert(v > 0)
	return v
end

local nreplicasets = getenvint('NREPLICASETS')
local start_port = getenvint('START_PORT')

local cfgmaker = dofile('cfgmaker.lua')

local clustercfg = cfgmaker.clustercfg(start_port, nreplicasets)

clustercfg['bucket_count'] = 100

clustercfg.listen = "0.0.0.0:12000"
clustercfg.background = true
clustercfg.log="router"..".log"
clustercfg.pid_file="router"..".pid"

local router = vshard.router.new('router', clustercfg)

box.once('access:v1', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

router:bootstrap({timeout = 4, if_not_bootstrapped = true})

local api = {}

function api.add_product(product)
    local bucket_id = router:bucket_id_strcrc32(product.id)
    product.bucket_id = bucket_id

    return router:call(bucket_id, 'write', 'product_add', {product})
end

function api.get_product(req)
    local bucket_id = router:bucket_id_strcrc32(req.id)

    return router:call(bucket_id, 'read', 'product_get', {req})
end

rawset(_G, 'api', api)