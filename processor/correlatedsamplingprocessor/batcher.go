package correlatedsamplingprocessor

import (
	"sync"
	"time"

	"errors"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/support/bundler"
)

var _ batcher = (*batcherImp)(nil)

type batcher interface {
	Add(signal) error
	Flush(pdata.TraceID)
	FlushAll()
}

type batcherImp struct {
	logger      *zap.Logger
	handler     func(xs []*signal)
	bundlers    map[pdata.TraceID]*bundler.Bundler
	bundlerLock *sync.RWMutex
	bundlerPool *sync.Pool
	closed      bool
}

func newBatcher(logger *zap.Logger, decisionWait time.Duration, handler func(xs []*signal)) *batcherImp {
	b := &batcherImp{
		logger:      logger,
		handler:     handler,
		bundlers:    make(map[pdata.TraceID]*bundler.Bundler),
		bundlerLock: &sync.RWMutex{},
		closed:      false,
	}
	b.bundlerPool = b.getBundlerPool(decisionWait, handler)

	return b
}

func (b *batcherImp) getBundlerPool(decisionWait time.Duration, handler func(xs []*signal)) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			var e signal
			b := bundler.NewBundler(&e, b.handle)

			b.DelayThreshold = decisionWait
			b.BundleCountThreshold = 10000
			b.BundleByteThreshold = 1e6 * 10 // 10M
			b.BundleByteLimit = 0            // unlimited
			b.BufferedByteLimit = 1e9        // 1G
			b.HandlerLimit = 1

			return b
		},
	}
}

func (b *batcherImp) handle(p interface{}) {
	batch := p.([]*signal)
	if len(batch) < 1 {
		// should never happen
		return
	}

	traceID := batch[0].TraceID

	// call the handler hook
	b.handler(batch)

	// remove the bundler
	{
		b.bundlerLock.RLock()
		bd, ok := b.bundlers[traceID]
		b.bundlerLock.RUnlock()

		if ok {
			b.bundlerLock.Lock()
			// remove from map so no additional data is written
			delete(b.bundlers, traceID)
			b.bundlerLock.Unlock()

			// return bundler to the pool
			b.bundlerPool.Put(bd)
		}
	}
}

func (b *batcherImp) Add(s signal) error {
	bd := b.getBundler(s.TraceID)
	if bd == nil {
		return errors.New("closed")
	}

	// ignore the size
	err := bd.Add(&s, 1)
	if err != nil {
		return err
	}

	return nil
}

func (b *batcherImp) getBundler(traceID pdata.TraceID) *bundler.Bundler {
	b.bundlerLock.RLock()
	bd, ok := b.bundlers[traceID]
	closed := b.closed
	b.bundlerLock.RUnlock()

	if closed {
		return nil
	}

	if !ok {
		bd = b.bundlerPool.Get().(*bundler.Bundler)

		b.bundlerLock.Lock()
		b.bundlers[traceID] = bd
		b.bundlerLock.Unlock()
	}

	return bd
}

func (b *batcherImp) Flush(traceID pdata.TraceID) {
	b.bundlerLock.Lock()
	bd, ok := b.bundlers[traceID]
	b.bundlerLock.Unlock()

	if ok {
		bd.Flush()
	}
}

func (b *batcherImp) FlushAll() {
	b.bundlerLock.Lock()
	b.closed = true
	count := len(b.bundlers)
	bundlers := make([]*bundler.Bundler,count)
	i:=0
	for _, bd := range b.bundlers {
		bundlers[i] = bd
		i++
	}
	b.bundlerLock.Unlock()

	for _, bd := range bundlers {
		bd.Flush()
	}
}
