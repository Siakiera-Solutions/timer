package timer

import (
	"context"
	"sync"
	"time"

	"github.com/siakiera-solutions/logger"
)

type fixed struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	tick func(ctx context.Context) error

	log logger.Logger

	interval time.Duration
	base     time.Time
}

func Fixed(
	tick func(ctx context.Context) error,
	log logger.Logger,
	interval time.Duration,
	base time.Time,
) Timer {
	ctx, cancel := context.WithCancel(context.Background())

	return &fixed{
		ctx:    ctx,
		cancel: cancel,
		tick:   tick,
		log: log.With(
			"layer", "pkg",
			"component", "timer.fixed",
		),
		interval: interval,
		base:     base,
	}
}

func (f *fixed) Start() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()

		nextRun := f.nextTick()
		f.log.Debug("next", "date", nextRun.String())
		timer := time.NewTimer(time.Until(nextRun))
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				if err := f.tick(f.ctx); err != nil {
					f.log.Error("tick", "err", err)
				}
				nextRun = f.nextTick()
				timer.Reset(time.Until(nextRun))
				f.log.Debug("next", "date", nextRun.String())
			case <-f.ctx.Done():
				return
			}
		}
	}()
	f.log.Debug("started")
}

func (f *fixed) Stop() {
	f.cancel()
	f.wg.Wait()
	f.log.Debug("stopped")
}

func (f *fixed) nextTick() time.Time {
	now := time.Now().UTC()
	delta := now.Sub(f.base)
	n := int64(delta / f.interval)

	return f.base.Add(time.Duration(n+1) * f.interval)
}
