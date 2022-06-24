## go-consistent-hashing

Golang implementation of consistent-hashing.



### Links

- [Consistent_hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
- [Consistent Hashing with Bounded Loads](http://ai.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)



### Start

1. Start proxy server

```shell
$ go run cmd/proxy-server/main.go
```

2. Start kv-store servers

```shell
$ go run cmd/kv-server/main.go -p 10001
$ go run cmd/kv-server/main.go -p 10002
$ go run cmd/kv-server/main.go -p 10003
```



### Usage

Try to use different keys and obserse the proxy server's console log.

```shell
$ curl http://curl 127.0.0.1:3333/key?key=${key}
$ curl http://curl 127.0.0.1:3333/key_least?key=${key}
```

