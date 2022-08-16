## go-consistent-hashing

Golang implementation of consistent-hashing.



### Links

- [Consistent_hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [Consistent Hashing with Bounded Loads](http://ai.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)



### Start

> First install [goreman](https://github.com/mattn/goreman), which manages Procfile-based applications.

1. Start proxy server

```shell
$ cd cmd/proxy-server
$ go build -o proxy-server
$ goreman start
```

2. Start kv-store servers

```shell
$ cd cmd/kv-server
$ go build -o kv-server
$ goreman start
```



### Usage

Try to use different keys and observe the proxy server's console log.

```shell
$ curl http://127.0.0.1:3333/key?key=${key}
$ curl http://127.0.0.1:3333/key_least?key=${key}
```

