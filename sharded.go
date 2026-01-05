package timer

import (
	"context"
	"sync"
	"time"

	"github.com/siakiera-solutions/logger"
)

type sharded struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	tick func(ctx context.Context, shards []int) error

	log logger.Logger

	shardsFn     func(workerID int, workersCount int, shardsCount int) []int
	workersCount int
	shardsCount  int
	interval     time.Duration
}

func NewSharded(
	tick func(ctx context.Context, shards []int) error,
	log logger.Logger,
	workersCount int,
	shardsCount int,
	shardsFn func(workerID int, workersCount int, shardsCount int) []int,
	interval time.Duration,
) Timer {
	ctx, cancel := context.WithCancel(context.Background())

	return &sharded{
		ctx:    ctx,
		cancel: cancel,

		tick: tick,

		log: log.With(
			"layer", "pkg",
			"component", "timer.sharded",
		),

		shardsFn:     shardsFn,
		workersCount: workersCount,
		shardsCount:  shardsCount,
		interval:     interval,
	}
}

func (s *sharded) Start() {
	s.wg.Add(s.workersCount)
	for i := 0; i < s.workersCount; i++ {
		shards := s.shardsFn(i, s.workersCount, s.shardsCount)

		go func(workerID int, shards []int) {
			defer s.wg.Done()

			timer := time.NewTimer(s.interval)
			defer timer.Stop()

			for {
				select {
				case <-s.ctx.Done():
					return

				case <-timer.C:
					err := s.tick(s.ctx, shards)
					if err != nil {
						s.log.Error("tick", "worker", workerID, "err", err)
					}

					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(s.interval)
				}
			}

		}(i, shards)
	}
	s.log.Debug("started")
}

func (s *sharded) Stop() {
	s.cancel()
	s.wg.Wait()
	s.log.Debug("stopped")
}
