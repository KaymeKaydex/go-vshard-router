# Go VShard Router

> :warning: This project is depreacted and archived as the functionality moved to [tarantool/go-vshard-router](https://github.com/tarantool/go-vshard-router) repo. You can pull it using `go get github.com/tarantool/go-vshard-router/v2`. The API is simplified and morernized, yet functionality is similar. All questions and issues you can submit [here](https://github.com/tarantool/go-vshard-router/issues).

<img align="right" width="159px" src="docs/static/logo.png" alt="go vshard router logo">

[![Go Reference](https://pkg.go.dev/badge/github.com/KaymeKaydex/go-vshard-router.svg)](https://pkg.go.dev/github.com/KaymeKaydex/go-vshard-router)
[![Actions Status][actions-badge]][actions-url]
[![Go Report Card](https://goreportcard.com/badge/github.com/KaymeKaydex/go-vshard-router)](https://goreportcard.com/report/github.com/KaymeKaydex/go-vshard-router)
[![codecov](https://codecov.io/gh/KaymeKaydex/go-vshard-router/graph/badge.svg?token=WLRWE97IT1)](https://codecov.io/gh/KaymeKaydex/go-vshard-router)

Translations:
- [English](https://github.com/KaymeKaydex/go-vshard-router/blob/main/README.md)

go-vshard-router — библиотека для отправки запросов напрямую в стораджа в шардированный кластер tarantool,
без использования tarantool-router.  Эта библиотека написана на основе [модуля библиотеки tarantool vhsard router](https://github.com/tarantool/vshard/blob/master/vshard/router/init.lua). go-vshard-router применяет новый подход к созданию кластера

Схема кластера с tarantool-proxy
```mermaid
graph TD
    subgraph Tarantool Database Cluster
        subgraph Replicaset 1
            Master_001_1
            Replica_001_2
        end

    end

ROUTER1["Tarantool vshard-router 1_1"] --> Master_001_1
ROUTER2["Tarantool vshard-router 1_2"] --> Master_001_1
ROUTER3["Tarantool vshard-router 1_3"] --> Master_001_1
ROUTER1["Tarantool vshard-router 1_1"] --> Replica_001_2
ROUTER2["Tarantool vshard-router 1_2"] --> Replica_001_2
ROUTER3["Tarantool vshard-router 1_3"] --> Replica_001_2

GO["Golang service"]
GO --> ROUTER1
GO --> ROUTER2
GO --> ROUTER3
```

Новая схема использования
```mermaid
graph TD
    subgraph Application Host
        Golang-Service
    end

    Golang-Service --> |iproto| MASTER1
    Golang-Service --> |iproto| REPLICA1
    
    MASTER1["Master 001_1"]
    REPLICA1["Replica 001_2"]
    
    subgraph Tarantool Database Cluster
        subgraph Replicaset 1
            MASTER1
            REPLICA1
        end
    end

    ROUTER1["Tarantool vshard-router(As contorol plane)"]
    ROUTER1 --> MASTER1
    ROUTER1 --> REPLICA1
```
## Как начать использовать?
### Предварительные условия

- **[Go](https://go.dev/)**: любая из **двух последних мажорных версий** [releases](https://go.dev/doc/devel/release).

### Установка Go-Vshard-Router
С помощью [Go module](https://github.com/golang/go/wiki/Modules) можно добавить следующий импорт

```
import "github.com/KaymeKaydex/go-vshard-router"
```
в ваш код, а затем `go [build|run|test]` автоматически получит необходимые зависимости.


В противном случае выполните следующую команду Go, чтобы установить пакет go-vshard-router:
```sh
$ go get -u github.com/KaymeKaydex/go-vshard-router
```

### Использование Go-Vshard-Router

Сначала вам необходимо импортировать пакет go-vshard-router для его использования.

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

func main() {
	ctx := context.Background()

	directRouter, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		DiscoveryTimeout: time.Minute,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TopologyProvider: static.NewProvider(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
			vshardrouter.ReplicasetInfo{
				Name: "replcaset_1",
				UUID: uuid.New(),
			}: {
				{
					Addr: "127.0.0.1:1001",
					UUID: uuid.New(),
				},
				{
					Addr: "127.0.0.1:1002",
					UUID: uuid.New(),
				},
			},
			vshardrouter.ReplicasetInfo{
				Name: "replcaset_2",
				UUID: uuid.New(),
			}: {
				{
					Addr: "127.0.0.1:2001",
					UUID: uuid.New(),
				},
				{
					Addr: "127.0.0.1:2002",
					UUID: uuid.New(),
				},
			},
		}),
		TotalBucketCount: 128000,
		PoolOpts: tarantool.Opts{
			Timeout: time.Second,
		},
	})
	if err != nil {
		panic(err)
	}

	user := struct {
		ID uint64
	}{
		ID: 123,
	}

	bucketID := vshardrouter.BucketIDStrCRC32(strconv.FormatUint(user.ID, 10), directRouter.RouterBucketCount())

	interfaceResult, getTyped, err := directRouter.RouterCallImpl(
		ctx,
		bucketID,
		vshardrouter.CallOpts{VshardMode: vshardrouter.ReadMode, PoolMode: pool.PreferRO, Timeout: time.Second * 2},
		"storage.api.get_user_info",
		[]interface{}{&struct {
			BucketID uint64                 `msgpack:"bucket_id" json:"bucket_id,omitempty"`
			Body     map[string]interface{} `msgpack:"body"`
		}{
			BucketID: bucketID,
			Body: map[string]interface{}{
				"user_id": "123456",
			},
		}},
	)

	info := &struct {
		BirthDay int
	}{}

	err = getTyped(&[]interface{}{info})
	if err != nil {
		panic(err)
	}

	fmt.Printf("interface result: %v", interfaceResult)
	fmt.Printf("get typed result: %v", info)
}

```
### Ознакомьтесь с другими примерами
#### Быстрое начало
Познакомьтесь с  [Полной документацией](docs/doc_ru.md), которая включает в себя примеры и теорию.
#### [Customer service](examples/customer/README.ru.md)
Сервис с go-vshard-router поверх примера тарантула из оригинальной библиотеки vshard с использованием raft.

## Бенчмарки
### Go Bench

| Бенчмарк                         | Число запусков | Время (ns/op) | Память (B/op) | Аллокации (allocs/op) |
|----------------------------------|----------------|---------------|---------------|-----------------------|
| BenchmarkCallSimpleInsert_GO-12       | 14216  | 81118         | 1419           | 29                      |
| BenchmarkCallSimpleInsert_Lua-12      | 9580   | 123307        | 1131           | 19                      |
| BenchmarkCallSimpleSelect_GO-12       | 18832  | 65190         | 1879           | 38                      |
| BenchmarkCallSimpleSelect_Lua-12      | 9963   | 104781        | 1617           | 28                      |


### [K6](https://github.com/grafana/k6)

Топология:
- 4 репликасета (x2 инстанса на репликасет)
- 4 тарантул прокси
- 1 инстанс гошного сервиса

сценарий constant VUes:
в нагрузке близкой к продовой

```select```
- go-vshard-router: uncritically worse latency, but 3 times more rps
  ![Image alt](docs/static/direct.png)
- tarantool-router: (80% cpu, heavy rps kills proxy at 100% cpu)
  ![Image alt](docs/static/not-direct.png)


[actions-badge]: https://github.com/KaymeKaydex/go-vshard-router/actions/workflows/main.yml/badge.svg
[actions-url]: https://github.com/KaymeKaydex/go-vshard-router/actions/workflows/main.yml