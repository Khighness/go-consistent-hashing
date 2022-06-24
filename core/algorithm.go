package core

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"sync"
)

// @Author KHighness
// @Update 2022-06-24

const (
	// hostReplicaFormat 虚拟节点名称格式
	hostReplicaFormat = "%s%d"
)

var (
	// defaultReplicaNum 虚拟节点数量
	defaultReplicaNum = 10

	// loadBoundFactor 负载边界因子
	// ref: https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
	loadBoundFactor = 0.25

	// defaultHashFunc 默认哈希函数
	defaultHashFunc = func(key string) uint64 {
		out := sha512.Sum512([]byte(key))
		return binary.LittleEndian.Uint64(out[:])
	}
)

// ConsistentHash is an implementation of consistent-hashing-algorithm
type ConsistentHash struct {
	// replicaNum 缓存服务器在哈希环中对应的虚拟节点数
	replicaNum int

	// totalLoad 缓存服务器对应的总请求数
	totalLoad int64

	// hashFunc 哈希函数
	hashFunc func(key string) uint64

	// hostMap 映射表：address -> Host
	hostMap map[string]*Host

	// replicaHostMap 映射表：虚拟节点index -> 缓存服务器address
	replicaHostMap map[uint64]string

	// sortedHostHashSet 哈希环
	sortedHostHashSet []uint64

	// 读写锁
	mu sync.RWMutex
}

// NewConsistent creates a consistent-hashing-algorithm
func NewConsistent(replicaNum int, hashFunc func(key string) uint64) *ConsistentHash {
	ch := &ConsistentHash{
		replicaNum:        replicaNum,
		totalLoad:         0,
		hashFunc:          hashFunc,
		hostMap:           make(map[string]*Host),
		replicaHostMap:    make(map[uint64]string),
		sortedHostHashSet: make([]uint64, 0),
		mu:                sync.RWMutex{},
	}

	if ch.replicaNum <= 0 {
		ch.replicaNum = defaultReplicaNum
	}
	if ch.hashFunc == nil {
		ch.hashFunc = defaultHashFunc
	}

	return ch
}

// RegisterHost 注册缓存服务器
func (ch *ConsistentHash) RegisterHost(address string) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 判断服务器是否已经注册
	if _, ok := ch.hostMap[address]; ok {
		return ErrHostAlreadyExists
	}
	log.Printf("Register host: %s", address)
	ch.hostMap[address] = &Host{Address: address, LoadBound: 0}

	// 循环创建虚拟节点
	for i := 0; i < ch.replicaNum; i++ {
		hashedIdx := ch.hashFunc(fmt.Sprintf(hostReplicaFormat, address, i))
		log.Printf("Add virtual node %v for host %s", hashedIdx, address)
		ch.replicaHostMap[hashedIdx] = address
		ch.sortedHostHashSet = append(ch.sortedHostHashSet, hashedIdx)
	}

	// 对哈希环排序
	sort.Slice(ch.sortedHostHashSet, func(i, j int) bool {
		return ch.sortedHostHashSet[i] < ch.sortedHostHashSet[j]
	})
	return nil
}

// UnregisterHost 注销缓存服务器
func (ch *ConsistentHash) UnregisterHost(address string) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 怕暖服务器是否存在
	if _, ok := ch.hostMap[address]; !ok {
		return ErrHostNotFound
	}
	log.Printf("Unregister host: %s", address)
	delete(ch.hostMap, address)

	// 循环删除虚拟节点
	for i := 0; i < ch.replicaNum; i++ {
		hashedIdx := ch.hashFunc(fmt.Sprintf(hostReplicaFormat, address, i))
		log.Printf("Remove virtual node %v for host %s", hashedIdx, address)
		delete(ch.replicaHostMap, hashedIdx)
		ch.delHashIndex(hashedIdx)
	}
	return nil
}

// delHashIndex 从哈希环中移除虚拟节点
func (ch *ConsistentHash) delHashIndex(val uint64) {
	idx := -1
	l := 0
	r := len(ch.sortedHostHashSet) - 1
	for l <= r {
		m := (l + r) / 2
		if ch.sortedHostHashSet[m] == val {
			idx = m
			break
		} else if ch.sortedHostHashSet[m] < val {
			l = m + 1
		} else {
			r = m - 1
		}
	}
	if idx != -1 {
		ch.sortedHostHashSet = append(ch.sortedHostHashSet[:idx], ch.sortedHostHashSet[idx+1:]...)
	}
}

// GetKey 根据Key查询Host
func (ch *ConsistentHash) GetHostByKey(key string) (string, error) {
	hashedIdx := ch.hashFunc(key)
	idx := ch.searchKey(hashedIdx)
	return ch.replicaHostMap[ch.sortedHostHashSet[idx]], nil
}

// searchKey 根据key在哈希环上顺指针寻找第一台缓存服务器的索引
func (ch *ConsistentHash) searchKey(key uint64) int {
	idx := sort.Search(len(ch.sortedHostHashSet), func(i int) bool {
		return ch.sortedHostHashSet[i] >= key
	})

	if idx >= len(ch.sortedHostHashSet) {
		idx = 0
	}
	return idx
}

