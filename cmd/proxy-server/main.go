package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/khighness/go-consistent-hashing/core"
	"github.com/khighness/go-consistent-hashing/proxy"
)

// @Author KHighness
// @Update 2022-06-24

func main() {
	server := ProxyServer{
		Address: "127.0.0.1:3333",
		Proxy:   proxy.NewProxy(core.NewConsistent(10, nil)),
	}
	server.Start()
}

type ProxyServer struct {
	Address string
	Proxy *proxy.Proxy
}

func (s *ProxyServer) Start() {
	log.Printf("Start proxy serer: %s", s.Address)

	http.HandleFunc("/register", s.registerHost)
	http.HandleFunc("/unregister", s.unregisterHost)
	http.HandleFunc("/key", s.getKey)
	http.HandleFunc("/key_least", s.getKey)
	if err := http.ListenAndServe(s.Address, nil); err != nil {
		panic(err)
	}
}

func (s *ProxyServer) getKey(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form["key"][0]

	val, err := s.Proxy.GetKey(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}
	_, _ = fmt.Fprintf(w, val)
}

func (s *ProxyServer) getKeyLeast(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	key := r.Form["key"][0]

	val, err := s.Proxy.GetKeyLeast(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}
	_, _ = fmt.Fprintf(w, val)
}

func (s *ProxyServer) registerHost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	address := r.Form["host"][0]

	err := s.Proxy.RegisterHost(address)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}
}

func (s *ProxyServer) unregisterHost(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	address := r.Form["host"][0]

	err := s.Proxy.UnregisterHost(address)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, err.Error())
		return
	}
}