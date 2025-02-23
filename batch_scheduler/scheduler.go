package batch_scheduler

import (
	"context"
	"golang.org/x/time/rate"
	"sync"
)

var DefaultOptions = SchedulerOptions{
	MaxConcurrency:   100,
	BatchSize:        200,
	RequestPerSecond: 1000,

	TaskChanSize:   100,
	ResultChanSize: 100,
}

type ProcessResultFn[T, V any] func(output Output[T, V])

type SchedulerOptions struct {
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

type BatchScheduler[T, V any] struct {
	Processor Processor[T, V]
	Logger    Logger

	TaskCtxQueue *SafeChan[T]
	ResultQueue  *SafeChan[Output[T, V]]

	RateLimiter *rate.Limiter

	maxConcurrency int
	batchSize      int
	rqs            int

	wg  sync.WaitGroup
	ctx context.Context
}

func NewBatchScheduler[T, V any](options SchedulerOptions) *BatchScheduler[T, V] {
	return &BatchScheduler[T, V]{
		maxConcurrency: options.MaxConcurrency,
		batchSize:      options.BatchSize,
		rqs:            options.RequestPerSecond,
		TaskCtxQueue:   NewSafeChan[T](options.TaskChanSize),
		ResultQueue:    NewSafeChan[Output[T, V]](options.ResultChanSize),
		Logger:         &DefaultLogger{},
		RateLimiter:    rate.NewLimiter(rate.Limit(options.RequestPerSecond), options.BatchSize),
	}
}

func (bs *BatchScheduler[T, V]) RegisterProcessor(processor Processor[T, V]) {
	bs.Processor = processor
}

func (bs *BatchScheduler[T, V]) RegisterLogger(logger Logger) {
	bs.Logger = logger
}

func (bs *BatchScheduler[T, V]) Start(ctx context.Context) {
	if ctx == nil {
		panic("ctx can't be nil")
	}
	bs.wg.Add(bs.maxConcurrency)
	bs.ctx = ctx
	for i := 0; i < bs.maxConcurrency; i++ {
		go bs.work(ctx)
	}
}

func (bs *BatchScheduler[T, V]) ResultFlow() chan Output[T, V] {
	return bs.ResultQueue.Chan()
}

func (bs *BatchScheduler[T, V]) submit(task T) {
	select {
	case <-bs.ctx.Done():
		bs.Logger.Infof("can't submit task, because ctx done, task info: %v", task)
	default:
		bs.TaskCtxQueue.Chan() <- task
	}
}

func (bs *BatchScheduler[T, V]) batchSubmit(tasks []T) {
	for i := 0; i <= len(tasks); i += bs.batchSize {
		end := min(i+bs.batchSize, len(tasks))
		subTasks := tasks[i:end]
		err := bs.RateLimiter.WaitN(context.TODO(), bs.batchSize)
		if err != nil {
			i -= bs.batchSize
			continue
		}
		for _, t := range subTasks {
			bs.submit(t)
		}
	}
}

func (bs *BatchScheduler[T, V]) work(ctx context.Context) {
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

func (bs *BatchScheduler[T, V]) waitClose() {
	bs.TaskCtxQueue.Close()
	bs.wg.Wait()
	bs.ResultQueue.Close()
}

func (bs *BatchScheduler[T, V]) mustStart() {
	if bs.ctx == nil {
		panic("must execute Start() method")
	}
}

func (bs *BatchScheduler[T, V]) ProcessResultSerially(fn ProcessResultFn[T, V]) {
	bs.mustStart()
	for res := range bs.ResultFlow() {
		fn(res)
	}
}

func (bs *BatchScheduler[T, V]) ProcessResultConcurrently(fn ProcessResultFn[T, V]) {
	bs.mustStart()
	for res := range bs.ResultFlow() {
		go func(input Output[T, V]) {
			fn(input)
		}(res)
	}
}

func (bs *BatchScheduler[T, V]) ProcessTasks(taskCtxList []T) {
	bs.mustStart()
	go func() {
		bs.batchSubmit(taskCtxList)
		bs.waitClose()
	}()
}
