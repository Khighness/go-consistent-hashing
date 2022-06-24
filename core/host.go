package core

// @Author KHighness
// @Update 2022-06-24

type Host struct {
	// Address host:port
	Address string
	// LoadBound 缓存服务器当前处理的请求缓存数
	LoadBound int64
}