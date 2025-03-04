package rate_limiter

import (
	"batch_scheduler/common"
	"context"
	"golang.org/x/time/rate"
	"sync"
)

var DefaultOptions = LimiterOptions{
	MaxConcurrency:   100,
	BatchSize:        200,
	RequestPerSecond: 1000,

	TaskChanSize:   100,
	ResultChanSize: 100,
}

type ProcessResultFn[T, V any] func(output Output[T, V])

type LimiterOptions struct {
	MaxConcurrency   int
	BatchSize        int
	RequestPerSecond int

	TaskChanSize   int
	ResultChanSize int
}

type Output[T, V any] struct {
	Arg    T
	Result V
	Err    error
}

type RateLimiter[T, V any] struct {
	Processor common.Processor[T, V]
	Logger    common.Logger

	TaskCtxQueue *SafeChan[T]
	ResultQueue  *SafeChan[Output[T, V]]

	RateLimiter *rate.Limiter

	maxConcurrency int
	batchSize      int
	rqs            int

	wg  sync.WaitGroup
	ctx context.Context
}

func NewRateLimiter[T, V any](options LimiterOptions) *RateLimiter[T, V] {
	return &RateLimiter[T, V]{
		maxConcurrency: options.MaxConcurrency,
		batchSize:      options.BatchSize,
		rqs:            options.RequestPerSecond,
		TaskCtxQueue:   NewSafeChan[T](options.TaskChanSize),
		ResultQueue:    NewSafeChan[Output[T, V]](options.ResultChanSize),
		Logger:         &common.DefaultLogger{},
		RateLimiter:    rate.NewLimiter(rate.Limit(options.RequestPerSecond), options.BatchSize),
	}
}

func (bs *RateLimiter[T, V]) RegisterProcessor(processor common.Processor[T, V]) {
	bs.Processor = processor
}

func (bs *RateLimiter[T, V]) RegisterLogger(logger common.Logger) {
	bs.Logger = logger
}

func (bs *RateLimiter[T, V]) Start(ctx context.Context) {
	if ctx == nil {
		panic("ctx can't be nil")
	}
	bs.wg.Add(bs.maxConcurrency)
	bs.ctx = ctx
	for i := 0; i < bs.maxConcurrency; i++ {
		go bs.work(ctx)
	}
}

func (bs *RateLimiter[T, V]) ResultFlow() chan Output[T, V] {
	return bs.ResultQueue.Chan()
}

func (bs *RateLimiter[T, V]) submit(task T) {
	select {
	case <-bs.ctx.Done():
		bs.Logger.Infof("can't submit task, because ctx done, task info: %v", task)
	default:
		bs.TaskCtxQueue.Chan() <- task
	}
}

func (bs *RateLimiter[T, V]) singleSubmit(task T) error {
	err := bs.RateLimiter.Wait(bs.ctx)
	if err != nil {
		bs.Logger.Errorf("batch submit err: %v", err)
		return err
	}
	bs.submit(task)
	return nil
}

func (bs *RateLimiter[T, V]) batchSubmit(tasks []T) {
	for i := 0; i <= len(tasks); i += bs.batchSize {
		end := min(i+bs.batchSize, len(tasks))
		subTasks := tasks[i:end]
		err := bs.RateLimiter.WaitN(bs.ctx, bs.batchSize)
		if err != nil {
			bs.Logger.Errorf("batch submit err: %v", err)
			return
		}
		for _, t := range subTasks {
			bs.submit(t)
		}
	}
}

func (bs *RateLimiter[T, V]) work(ctx context.Context) {
	defer bs.wg.Done()
	for {
		select {
		case task, ok := <-bs.TaskCtxQueue.Chan():
			if !ok {
				return
			}
			res, err := bs.Processor.Process(task)
			if err != nil {
				bs.Logger.Errorf(err.Error())
				continue
			}
			bs.ResultQueue.Chan() <- Output[T, V]{
				Arg:    task,
				Result: res,
				Err:    err,
			}
		case <-ctx.Done():
			bs.Logger.Infof("exit because ctx done")
			return
		}
	}
}

func (bs *RateLimiter[T, V]) waitClose() {
	bs.TaskCtxQueue.Close()
	bs.wg.Wait()
	bs.ResultQueue.Close()
}

func (bs *RateLimiter[T, V]) mustStart() {
	if bs.ctx == nil {
		panic("must execute Start() method")
	}
}

func (bs *RateLimiter[T, V]) ProcessResultSerially(fn ProcessResultFn[T, V]) {
	bs.mustStart()
	for res := range bs.ResultFlow() {
		fn(res)
	}
}

func (bs *RateLimiter[T, V]) ProcessResultConcurrently(fn ProcessResultFn[T, V]) {
	bs.mustStart()
	for res := range bs.ResultFlow() {
		go func(input Output[T, V]) {
			fn(input)
		}(res)
	}
}

func (bs *RateLimiter[T, V]) ProcessTasks(taskCtxList []T) {
	bs.mustStart()
	go func() {
		bs.batchSubmit(taskCtxList)
		bs.waitClose()
	}()
}

func (bs *RateLimiter[T, V]) ProcessTask(task T) error {
	bs.mustStart()
	return bs.singleSubmit(task)
}

func (bs *RateLimiter[T, V]) WaitAndClose() {
	bs.waitClose()
}
