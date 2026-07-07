package executor

import (
	"context"
	"time"
)

// Gate 是全局并发上限的 in-process 实现（不引 Redis）。
// 用带缓冲 chan 作信号量；一个 token = 一个"在系统中"的 task（queued+running）。
// submit 路径 Acquire（满则排队等待，超时返回 false → 503），
// executor 在 task 终态时 Release。
type Gate struct {
	sem chan struct{}
}

// NewGate 创建容量为 max 的并发闸门。
func NewGate(max int) *Gate {
	if max <= 0 {
		max = 1
	}
	return &Gate{sem: make(chan struct{}, max)}
}

// Acquire 尝试占用一个槽位，最多等待 wait。成功返回 true。
func (g *Gate) Acquire(ctx context.Context, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case g.sem <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case g.sem <- struct{}{}:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// Release 释放一个槽位。多释放是安全 no-op（非阻塞）。
func (g *Gate) Release() {
	select {
	case <-g.sem:
	default:
	}
}

// InFlight 当前占用的槽位数。
func (g *Gate) InFlight() int {
	return len(g.sem)
}
