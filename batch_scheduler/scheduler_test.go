package batch_scheduler

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

func TestBatchScheduler_Scheduler(t *testing.T) {
	batchScheduler := NewBatchScheduler[int, int](DefaultOptions)
	taskCtx := make([]int, 10000)
	batchScheduler.RegisterProcessor(&TestCaseProcessor[int, int]{})
	controlCtx, _ := context.WithTimeout(context.Background(), time.Second*3)
	batchScheduler.Start(controlCtx)
	batchScheduler.ProcessTasks(taskCtx)
	var resList []Output[int, int]
	var customFn ProcessResultFn[int, int] = func(o Output[int, int]) {
		resList = append(resList, o)
	}
	batchScheduler.ProcessResultSerially(customFn)
	fmt.Printf("len: %d", len(resList))
}
