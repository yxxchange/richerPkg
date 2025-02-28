package rate_limiter

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type TestCaseProcessor[T, V any] struct {
}

func (tp *TestCaseProcessor[T, V]) Process(_ T) (V, error) {
	fmt.Println("start to do")
	time.Sleep(500 * time.Millisecond)
	fmt.Println("ok")
	var res V
	return res, nil
}

func TestRateLimiter_BatchSubmit(t *testing.T) {
	rateLimiter := NewRateLimiter[int, int](DefaultOptions)
	taskCtx := make([]int, 10000)
	rateLimiter.RegisterProcessor(&TestCaseProcessor[int, int]{})
	controlCtx, _ := context.WithTimeout(context.Background(), time.Second*3)
	rateLimiter.Start(controlCtx)
	rateLimiter.ProcessTasks(taskCtx)
	var resList []Output[int, int]
	var customFn ProcessResultFn[int, int] = func(o Output[int, int]) {
		resList = append(resList, o)
	}
	rateLimiter.ProcessResultSerially(customFn)
	fmt.Printf("len: %d", len(resList))
}

func TestRateLimiter_SingleSubmit(t *testing.T) {
	rateLimiter := NewRateLimiter[int, int](DefaultOptions)
	taskCtx := make([]int, 10000)
	rateLimiter.RegisterProcessor(&TestCaseProcessor[int, int]{})
	controlCtx, _ := context.WithTimeout(context.Background(), time.Second*30)
	rateLimiter.Start(controlCtx)
	var resList []Output[int, int]
	var customFn ProcessResultFn[int, int] = func(o Output[int, int]) {
		resList = append(resList, o)
	}
	go func() {
		for _, ta := range taskCtx {
			_ = rateLimiter.ProcessTask(ta)
		}
		rateLimiter.WaitAndClose()
	}()
	rateLimiter.ProcessResultSerially(customFn)
	fmt.Printf("len: %d", len(resList))
}
