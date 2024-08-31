#!/usr/bin/env tarantool

require('strict').on()

local config_example = {
    sharding = {
        ['cbf06940-0790-498b-948d-042b62cf3d29'] = { -- replicaset #1
            replicas = {
                ['8a274925-a26d-47fc-9e1b-af88ce939412'] = {
                    uri = 'storage:storage@127.0.0.1:3301',
                    name = 'storage_1_a',
                    master = true
                },
                ['3de2e3e1-9ebe-4d0d-abb1-26d301b84633'] = {
                    uri = 'storage:storage@127.0.0.1:3302',
                    name = 'storage_1_b'
                }
            },
        }, -- replicaset #1
        ['ac522f65-aa94-4134-9f64-51ee384f1a54'] = { -- replicaset #2
            replicas = {
                ['1e02ae8a-afc0-4e91-ba34-843a356b8ed7'] = {
                    uri = 'storage:storage@127.0.0.1:3303',
                    name = 'storage_2_a',
                    master = true
                },
                ['001688c3-66f8-4a31-8e19-036c17d489c2'] = {
                    uri = 'storage:storage@127.0.0.1:3304',
                    name = 'storage_2_b'
                }
            },
        }, -- replicaset #2
    }, -- sharding
    replication_connect_quorum = 0,
}

local function get_uid(rs_id, instance_id)
    local uuid_template = "00000000-0000-%04d-%04d-000000000000"

    return string.format(uuid_template, rs_id, instance_id)
end

local function replicaset_uuid(rs_id)
    return get_uid(rs_id, 0)
end

local function master_replica_uuid(rs_id)
    return get_uid(rs_id, 1)
end

local function follower_replica_uuid(rs_id)
    return get_uid(rs_id, 2)
end

local function master_replica_name(rs_id)
    return string.format("storage_%d_master", rs_id)
end

local function follower_replica_name(rs_id)
    return string.format("storage_%d_follower", rs_id)
end

local function replica_cfg(start_port, rs_id, is_master)
    local uri_template = 'storage:storage@127.0.0.1:%d'
    local port, name
    if is_master then
        port = start_port + 2 * (rs_id - 1) -- multiple to 2 because there are 2 instances per replicaset
        name = master_replica_name(rs_id)
    else
        port = start_port + 2 * (rs_id - 1) + 1
        name = follower_replica_name(rs_id)
    end

    return {
        uri = string.format(uri_template, port),
        name = name,
        master = is_master,
    }
end

local function master_replica_cfg(start_port, rs_id)
    return replica_cfg(start_port, rs_id, true)
end

local function follower_replica_cfg(start_port, rs_id)
    return replica_cfg(start_port, rs_id, false)
end

local function clustercfg(start_port, nreplicasets)
    local cfg = {
        sharding = {},
        replication_connect_quorum = 0,
    }

    for rs_id = 1, nreplicasets do
        local master_uuid = master_replica_uuid(rs_id)
        local follower_uuid = follower_replica_uuid(rs_id)

        local replicas = {
            [master_uuid] = master_replica_cfg(start_port, rs_id),
            [follower_uuid] = follower_replica_cfg(start_port, rs_id),
        }

        local rs_uuid = replicaset_uuid(rs_id)

        cfg.sharding[rs_uuid] = {
            replicas = replicas,
        }
    end

    return cfg
end

return {
    clustercfg = clustercfg,
    master_uuid = master_replica_uuid,
    follower_uuid = follower_replica_uuid,
}
