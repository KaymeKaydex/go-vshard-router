# A framework to test go-vshard-router module using real tarantools

## Requirements

Your system must have:
- an installed tarantool that supports vshard (1.9+)
- an installed tnt vshard library (`local vshard = require('vshard')` must work)

The next will run all go-tests in this directory
```bash
make run
```
