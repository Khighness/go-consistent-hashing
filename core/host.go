package core

// @Author KHighness
// @Update 2022-06-24

type Host struct {
	// Address host:port
	Address string
	// LoadBound 缓存服务器的负载
	LoadBound int64
}