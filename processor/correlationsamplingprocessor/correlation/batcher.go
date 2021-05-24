package correlation

import (
	"sync"
	"time"

	"errors"

	"sync/atomic"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/support/bundler"
)

var _ batcher = (*batcherImp)(nil)

type batcher interface {
	Add(Signal) error
	Flush(pdata.TraceID)
	Close()
}

type batcherImp struct {
	logger      *zap.Logger
	handler     func(xs []*Signal)
	bundlerMap  *BundlerSyncMap
	bundlerPool *sync.Pool
	closed      uint64
}

func newBatcher(logger *zap.Logger, decisionWait time.Duration, handler func(xs []*Signal)) *batcherImp {
	b := &batcherImp{
		logger:     logger,
		handler:    handler,
		bundlerMap: NewBundlerSyncMap(),
		closed:     0,
	}
	b.bundlerPool = b.getBundlerPool(decisionWait)

	return b
}

func (b *batcherImp) getBundlerPool(decisionWait time.Duration) *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			var e Signal
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
	batch := p.([]*Signal)
	if len(batch) < 1 {
		// should never happen
		return
	}

	traceID := batch[0].TraceID

	// call the handler hook
	b.handler(batch)

	// remove the bundler
	{
		if bd, ok := b.bundlerMap.Load(traceID); ok {
			// remove from map so no additional data is written
			b.bundlerMap.Delete(traceID)
			// return bundler to the pool
			b.bundlerPool.Put(bd)
		}
	}
}

func (b *batcherImp) Add(s Signal) error {
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
	closed := atomic.LoadUint64(&b.closed)
	if closed == 1 {
		return nil
	}

	bd := b.bundlerPool.Get().(*bundler.Bundler)
	actual, loaded := b.bundlerMap.LoadOrStore(traceID, bd)
	if loaded {
		bd.Flush()
		// return bundler to the pool
		b.bundlerPool.Put(bd)
	}

	return actual
}

func (b *batcherImp) Flush(traceID pdata.TraceID) {
	bd, ok := b.bundlerMap.Load(traceID)

	if ok {
		bd.Flush()
	}
}

func (b *batcherImp) Close() {
	atomic.CompareAndSwapUint64(&b.closed, 0, 1)
	b.bundlerMap.Range(func(key pdata.TraceID, value *bundler.Bundler) bool {
		value.Flush()
		return true
	})
}
