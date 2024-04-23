```
unix/:./data/router_1.control> vshard.router.call(1, 'read', 'customer_lookup', {1})
---
- null
  ...

unix/:./data/router_1.control> vshard.router.call(1, 'write', 'customer_add', {{customer_id=1, name = "Ilya", bucket_id=1, accounts={{account_id=2, name="First Customer"}}  }})
---
- true
  ...

unix/:./data/router_1.control> vshard.router.call(1, 'read', 'customer_lookup', {1})
---
- {'accounts': [{'account_id': 2, 'balance': 0, 'name': 'First Customer'}], 'customer_id': 1,
  'name': 'Ilya'}
  ...
```