#!/usr/bin/env tarantool

require('strict').on()

local box = require('box')
local debug = require('debug')
local fiber = require('fiber')
local math = require('math')
local os = require('os')
local uuid = require('uuid')

-- vshard must be global, not local. Otherwise it does not work.
local vshard = require 'vshard'
rawset(_G, 'vshard', vshard)

local function getenvint(envname)
	local v = tonumber(os.getenv(envname) or 0)
	v = math.floor(v or 0)
	assert(v > 0)
	return v
end

local nreplicasets = getenvint('NREPLICASETS')
local start_port = getenvint('START_PORT')

local source = debug.getinfo(1,"S").source

local is_master = false
local filename = string.match(source, "/storage_%d+_master.lua$")
if filename then
	is_master = true
else
	filename = string.match(source, "/storage_%d+_follower.lua$")
end
assert(filename ~= nil)

local rs_id = tonumber(string.match(filename, "%d+") or 0)
assert(rs_id > 0)

local cfgmaker = dofile('cfgmaker.lua')

local clustercfg = cfgmaker.clustercfg(start_port, nreplicasets)
local instance_uuid
if is_master then
	instance_uuid = cfgmaker.master_uuid(rs_id)
else
	instance_uuid = cfgmaker.follower_uuid(rs_id)
end


vshard.storage.cfg(clustercfg, instance_uuid)

-- everything below is copypasted from storage.lua in vshard example:
-- https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/example/storage.lua
box.once("testapp:schema:1", function()
	local customer = box.schema.space.create('customer')
    customer:format({
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'name', 'string'},
    })
    customer:create_index('customer_id', {parts = {'customer_id'}})
    customer:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})

    -- create products for easy bench
	local products = box.schema.space.create('products')
    products:format({
        {'id', 'uuid'},
        {'bucket_id', 'unsigned'},
        {'name', 'string'},
        {'count', 'unsigned'},
    })
    products:create_index('id', {parts = {'id'}})


    local account = box.schema.space.create('account')
    account:format({
        {'account_id', 'unsigned'},
        {'customer_id', 'unsigned'},
        {'bucket_id', 'unsigned'},
        {'balance', 'unsigned'},
        {'name', 'string'},
    })
    account:create_index('account_id', {parts = {'account_id'}})
    account:create_index('customer_id', {parts = {'customer_id'}, unique = false})
    account:create_index('bucket_id', {parts = {'bucket_id'}, unique = false})
	box.snapshot()

	box.schema.func.create('customer_lookup')
    box.schema.role.grant('public', 'execute', 'function', 'customer_lookup')
    box.schema.func.create('customer_add')
    box.schema.role.grant('public', 'execute', 'function', 'customer_add')
    box.schema.func.create('echo')
    box.schema.role.grant('public', 'execute', 'function', 'echo')
    box.schema.func.create('sleep')
    box.schema.role.grant('public', 'execute', 'function', 'sleep')
    box.schema.func.create('raise_luajit_error')
    box.schema.role.grant('public', 'execute', 'function', 'raise_luajit_error')
    box.schema.func.create('raise_client_error')
    box.schema.role.grant('public', 'execute', 'function', 'raise_client_error')

    box.schema.user.grant('storage', 'super')
    box.schema.user.create('tarantool')
    box.schema.user.grant('tarantool', 'super')
end)


-- product_add - simple add some product to storage
function product_add(product)
    local id = uuid.fromstr(product.id)

    box.space.products:insert({ id, product.bucket_id, product.name, product.count})

    return true
end

function customer_add(customer)
    box.begin()
    box.space.customer:insert({customer.customer_id, customer.bucket_id,
                               customer.name})
    for _, account in ipairs(customer.accounts) do
        box.space.account:insert({
            account.account_id,
            customer.customer_id,
            customer.bucket_id,
            0,
            account.name
        })
    end
    box.commit()
    return true
end

function customer_lookup(customer_id)
    if type(customer_id) ~= 'number' then
        error('Usage: customer_lookup(customer_id)')
    end

    local customer = box.space.customer:get(customer_id)
    if customer == nil then
        return nil
    end
    customer = {
        customer_id = customer.customer_id;
        name = customer.name;
    }
    local accounts = {}
    for _, account in box.space.account.index.customer_id:pairs(customer_id) do
        table.insert(accounts, {
            account_id = account.account_id;
            name = account.name;
            balance = account.balance;
        })
    end
    customer.accounts = accounts;
    return customer
end

function echo(...)
    return ...
end

function sleep(time)
    fiber.sleep(time)
    return true
end

function raise_luajit_error()
    assert(1 == 2)
end

function raise_client_error()
    box.error(box.error.UNKNOWN)
end
