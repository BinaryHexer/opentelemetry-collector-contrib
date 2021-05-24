package sampling

import (
	"sync"

	"go.opentelemetry.io/collector/consumer/pdata"
)

type DecisionMap struct {
	sync.RWMutex
	internal map[pdata.TraceID]Decision
}

func NewDecisionMap() *DecisionMap {
	return &DecisionMap{
		internal: make(map[pdata.TraceID]Decision),
	}
}

func (m *DecisionMap) Load(key pdata.TraceID) (value Decision, ok bool) {
	m.RLock()
	result, ok := m.internal[key]
	m.RUnlock()
	return result, ok
}

func (m *DecisionMap) Delete(key pdata.TraceID) {
	m.Lock()
	delete(m.internal, key)
	m.Unlock()
}

func (m *DecisionMap) Store(key pdata.TraceID, value Decision) {
	m.Lock()
	m.internal[key] = value
	m.Unlock()
}

type DecisionSyncMap struct {
	internal sync.Map
}

func NewDecisionSyncMap() *DecisionSyncMap {
	return &DecisionSyncMap{
		internal: sync.Map{},
	}
}

func (m *DecisionSyncMap) Load(key pdata.TraceID) (value Decision, ok bool) {
	result, ok := m.internal.Load(key)
	if !ok {
		return Unspecified, ok
	}

	return result.(Decision), ok
}

func (m *DecisionSyncMap) Delete(key pdata.TraceID) {
	m.internal.Delete(key)
}

func (m *DecisionSyncMap) Store(key pdata.TraceID, value Decision) {
	m.internal.Store(key, value)
}
