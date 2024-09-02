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

local router = vshard.router.new('router', clustercfg)

router:bootstrap({timeout = 4, if_not_bootstrapped = true})

os.exit(0)
