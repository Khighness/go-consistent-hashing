package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// @Author KHighness
// @Update 2022-06-24

var (
	port = flag.String("p", "10000", "port")
)

func main() {
	flag.Parse()
	address := fmt.Sprintf("127.0.0.1:%s", *port)
	server := KVStoreServer{
		Address:      address,
		Cache:        sync.Map{},
		RegistryHost: "http://127.0.0.1:3333",
		ExpireTime:   10,
	}
	server.Start(context.Background())
}

type KVStoreServer struct {
	Address      string
	RegistryHost string
	Cache        sync.Map
	ExpireTime   int64
}

func (s *KVStoreServer) Start(ctx context.Context) {
	log.Printf("Start kv-server at %s", s.Address)

	var err error
	if err = s.register(); err != nil {
		panic(err)
	}

	http.HandleFunc("/", s.kvHandler)
	if err = http.ListenAndServe(s.Address, nil); err != nil {
		err = s.unregister()
		if err != nil {
			panic(err)
		}
		panic(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		_ = s.unregister()
		log.Printf("Context done, server stopped")
	case <-interrupt:
		_ = s.unregister()
		log.Printf("Stop signal interrupted server")
	}
}

func (s *KVStoreServer) kvHandler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form["key"][0]

	if _, ok := s.Cache.Load(key); !ok {
		val := fmt.Sprintf("k-%s", key)
		s.Cache.Store(key, val)
		log.Printf("Cached <%s, %s>", key, val)
		time.AfterFunc(time.Duration(s.ExpireTime) * time.Second, func() {
			s.Cache.Delete(key)
			log.Printf("Removed <%s, %s>", key, val)
		})
	}

	val, _ := s.Cache.Load(key)
	_, _ = fmt.Fprintf(w, val.(string))
}

func (s *KVStoreServer) register() error {
	resp, err := http.Get(fmt.Sprintf("%s/register?host=%s", s.RegistryHost, s.Address))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Printf("Register to %s", s.RegistryHost)
	return nil
}

func (s *KVStoreServer) unregister() error {
	resp, err := http.Get(fmt.Sprintf("%s/unregister?host=%s", s.RegistryHost, s.Address))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	log.Printf("Unregister to %s", s.RegistryHost)
	return nil
}

