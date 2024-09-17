-- configure path so that you can run application
-- from outside the root directory
if package.setsearchroot ~= nil then
    package.setsearchroot()
end

local vshard = require('vshard')

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.schema.user.grant(
        'guest',
        'read,write,execute',
        'universe'
)

local s = box.schema.space.create('test', {
    id = 617,
    if_not_exists = true,
    format = {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned', is_nullable = true},
        {name = 'name', type = 'string'},
    }
})
s:create_index('primary_index', {
    parts = {
        {field = 1, type = 'unsigned'},
    },
})
s:create_index('bucket_id', {
    parts = {
        {field = 2, type = 'unsigned'},
    },
    unique = false,
})

for i=1,100 do
    s:insert({i, 100, 'Petr'})
end

local function is_ready_false()
    return false
end

local function is_ready_true()
    return true, nil
end

local function sum(a, b)
    return a + b, nil
end

local function up_error()
    return nil, error("raise error")
end

rawset(_G, 'is_ready', is_ready_false)
rawset(_G, 'sum', sum)
rawset(_G, 'up_error', up_error)

-- Setup vshard.
_G.vshard = vshard
box.once('guest', function()
    box.schema.user.grant('guest', 'super')
end)
local uri = 'guest@127.0.0.1:3013'
local box_info = box.info()

local replicaset_uuid
if box_info.replicaset then
    -- Since Tarantool 3.0.
    replicaset_uuid = box_info.replicaset.uuid
else
    replicaset_uuid = box_info.cluster.uuid
end

local cfg = {
    bucket_count = 300,
    sharding = {
        [replicaset_uuid] = {
            replicas = {
                [box_info.uuid] = {
                    uri = uri,
                    name = 'storage',
                    master = true,
                },
            },
        },
    },
}
vshard.storage.cfg(cfg, box_info.uuid)
vshard.router.cfg(cfg)
vshard.router.bootstrap()

box.schema.user.create('test', { password = 'test' , if_not_exists = true })
box.schema.user.grant('test', 'execute', 'universe', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create,read,write,drop,alter', 'space', nil, { if_not_exists = true })
box.schema.user.grant('test', 'create', 'sequence', nil, { if_not_exists = true })

-- Set is_ready = is_ready_true only when every other thing is configured.
rawset(_G, 'is_ready', is_ready_true)
