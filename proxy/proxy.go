package proxy

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/khighness/go-consistent-hashing/core"
)

// @Author KHighness
// @Update 2022-06-24

type Proxy struct {
	ch *core.ConsistentHash
}

func NewProxy(consistentHash *core.ConsistentHash) *Proxy {
	proxy := &Proxy{
		ch: consistentHash,
	}
	return proxy
}

func (p *Proxy) RegisterHost(address string) error {
	err := p.ch.RegisterHost(address)
	if err != nil {
		return err
	}
	return nil
}

func (p *Proxy) UnregisterHost(address string) error {
	err := p.ch.UnregisterHost(address)
	if err != nil {
		return err
	}
	return nil
}

func (p *Proxy) GetKey(key string) (string, error) {
	log.Printf("Request key: %s", key)
	host, err := p.ch.GetHostByKey(key)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s?key=%s", host, key))
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Response from host %s: %s", host, string(body))

	return string(body), nil
}