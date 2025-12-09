package timer

import (
	"context"
	"sync"
	"time"

	"github.com/siakiera-solutions/logger"
)

type defaultT struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	tick func(ctx context.Context) error

	log logger.Logger

	interval time.Duration
}

func Default(
	tick func(ctx context.Context) error,
	op string,
	log logger.Logger,
	interval time.Duration,
) Timer {
	ctx, cancel := context.WithCancel(context.Background())

	return &defaultT{
		ctx:    ctx,
		cancel: cancel,
		tick:   tick,
		log: log.With(
			"layer", "pkg",
			"component", "timer.default",
		),
		interval: interval,
	}
}

func (d *defaultT) Start() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		timer := time.NewTimer(d.interval)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				if err := d.tick(d.ctx); err != nil {
					d.log.Error("tick", "err", err)
				}
				timer.Reset(d.interval)
			case <-d.ctx.Done():
				return
			}
		}
	}()
	d.log.Debug("started")
}

func (d *defaultT) Stop() {
	d.cancel()
	d.wg.Wait()
	d.log.Debug("stopped")
}
