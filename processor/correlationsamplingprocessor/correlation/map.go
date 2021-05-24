package correlation

import (
	"sync"

	"go.opentelemetry.io/collector/consumer/pdata"
	"google.golang.org/api/support/bundler"
)

type BundlerMap struct {
	sync.RWMutex
	internal map[pdata.TraceID]*bundler.Bundler
}

func NewBundlerMap() *BundlerMap {
	return &BundlerMap{
		internal: make(map[pdata.TraceID]*bundler.Bundler),
	}
}

func (m *BundlerMap) Load(key pdata.TraceID) (value *bundler.Bundler, ok bool) {
	m.RLock()
	result, ok := m.internal[key]
	m.RUnlock()
	return result, ok
}

func (m *BundlerMap) Delete(key pdata.TraceID) {
	m.Lock()
	delete(m.internal, key)
	m.Unlock()
}

func (m *BundlerMap) Store(key pdata.TraceID, value *bundler.Bundler) {
	m.Lock()
	m.internal[key] = value
	m.Unlock()
}

func (m *BundlerMap) Range(f func(key pdata.TraceID, value *bundler.Bundler) bool) {
	m.Lock()
	keys := make([]pdata.TraceID, len(m.internal))
	i := 0
	for k := range m.internal {
		keys[i] = k
		i++
	}
	m.Unlock()

	for _, k := range keys {
		if v, ok := m.Load(k); ok {
			f(k, v)
		}
	}
}

type BundlerSyncMap struct {
	internal sync.Map
}

func NewBundlerSyncMap() *BundlerSyncMap {
	return &BundlerSyncMap{
		internal: sync.Map{},
	}
}

func (m *BundlerSyncMap) Load(key pdata.TraceID) (value *bundler.Bundler, ok bool) {
	result, ok := m.internal.Load(key)
	if !ok {
		return nil, ok
	}

	return result.(*bundler.Bundler), ok
}

func (m *BundlerSyncMap) LoadOrStore(key pdata.TraceID, value *bundler.Bundler) (actual *bundler.Bundler, loaded bool) {
	result, loaded := m.internal.LoadOrStore(key, value)

	return result.(*bundler.Bundler), loaded
}

func (m *BundlerSyncMap) Delete(key pdata.TraceID) {
	m.internal.Delete(key)
}

func (m *BundlerSyncMap) Store(key pdata.TraceID, value *bundler.Bundler) {
	m.internal.Store(key, value)
}

func (m *BundlerSyncMap) Range(f func(key pdata.TraceID, value *bundler.Bundler) bool) {
	m.internal.Range(func(key, value interface{}) bool {
		k := key.(pdata.TraceID)
		v := value.(*bundler.Bundler)
		return f(k, v)
	})
}
