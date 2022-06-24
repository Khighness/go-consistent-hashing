package core

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
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

	// totalLoad 代理服务器承受的总负载
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
	idx := ch.searchIndex(hashedIdx)
	return ch.replicaHostMap[ch.sortedHostHashSet[idx]], nil
}

// searchIndex 根据key在哈希环上顺指针寻找第一台缓存服务器的索引
func (ch *ConsistentHash) searchIndex(key uint64) int {
	idx := sort.Search(len(ch.sortedHostHashSet), func(i int) bool {
		return ch.sortedHostHashSet[i] >= key
	})

	if idx >= len(ch.sortedHostHashSet) {
		idx = 0
	}
	return idx
}

// GetHostByKeyLeast 有界负载的一致性哈希
func (ch *ConsistentHash) GetHostByKeyLeast(key string) (string, error) {
	ch.mu.RLock()
	defer ch.mu.RLock()

	if len(ch.replicaHostMap) == 0 {
		return "", ErrHostNotFound
	}

	hashedIdx := ch.hashFunc(key)
	idx := ch.searchIndex(hashedIdx)

	i := idx
	for {
		address := ch.replicaHostMap[ch.sortedHostHashSet[i]]
		loadChecked, err := ch.checkLoadCapacity(address)
		if err != nil {
			return "", err
		}
		if loadChecked {
			return address, nil
		}
		i++

		if i >= len(ch.replicaHostMap) {
			i = 0
		}
	}
}

// MaxLoad 获取单节点的最大负载
// (total_load / number_of_hosts) * (1 + load_bound_factor)
func (ch *ConsistentHash) MaxLoad() int64 {
	if ch.totalLoad == 0 {
		ch.totalLoad = 1
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64(ch.totalLoad / int64(len(ch.hostMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * (1 + loadBoundFactor))
	return int64(avgLoadPerNode)
}

// IncLoad 递增缓存服务器的负载
func (ch *ConsistentHash) IncLoad(address string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	atomic.AddInt64(&ch.hostMap[address].LoadBound, 1)
	atomic.AddInt64(&ch.totalLoad, 1)
}

// DecLoad 递减缓存服务器的负载
func (ch *ConsistentHash) DecLoad(address string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	atomic.AddInt64(&ch.hostMap[address].LoadBound, -1)
	atomic.AddInt64(&ch.totalLoad, -1)
}

// GetLoads 获取所有缓存服务器的负载数据
func (ch *ConsistentHash) GetLoads() map[string]int64 {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	loads := make(map[string]int64)
	for address, host := range ch.hostMap {
		loads[address] = atomic.LoadInt64(&host.LoadBound)
	}
	return loads
}

// checkLoadCapacity 检验一个缓存服务器是否能在有界负载之内提供服务
func (ch *ConsistentHash) checkLoadCapacity(address string) (bool, error) {
	if ch.totalLoad < 0 {
		ch.totalLoad = 0
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64((ch.totalLoad + 1) / int64(len(ch.hostMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * (1 + loadBoundFactor))

	candidateHost, ok := ch.hostMap[address]
	if !ok {
		return false, ErrHostNotFound
	}

	if float64(candidateHost.LoadBound)+1 <= avgLoadPerNode {
		return true, nil
	}
	return false, nil
}
