std = "luajit"
codes = true

read_globals = {
	-- Tarantool vars:
	"box",
	"tonumber64",
    "package",
	"spacer",
	"F",
	"T",

	package = {
		fields = {
			reload = {
				fields = {
					"count",
					"register",
				}
			}
		}
	},

	-- Exported by package 'config'
	"config",
}

max_line_length = 200

ignore = {
	"212",
	"213",
	"411", -- ignore was previously defined
	"422", -- ignore shadowing
	"111", -- ignore non standart functions; for tarantool there is global functions
}

local conf_rules = {
	read_globals = {
		"instance_name",
	},
	globals = {
		"box", "etcd",
	}
}

exclude_files = {
	"tests/tnt/.rocks/*",
    "examples/customer/tarantool/.rocks/*",
	"tests/tnt/tmp/*", -- ignore tmp tarantool files
	"examples/customer/tarantool/*",                    -- TODO: now we ignore examples from original vshard example
	".rocks/*",                                         -- ignore rocks after tests prepare
}


files["etc/*.lua"] = conf_rules
