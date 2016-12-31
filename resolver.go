package operator

import (
	"fmt"
	"sync"
)

type OperatorResolver interface {
	ResolveOperator(receiverID string) (string, error)
	SetOperator(receiverID string, host string) error
}

var DefaultOperatorResolver OperatorResolver = nil

func init() {
	om := &operatorManager{map[string]string{}, sync.Mutex{}}
	DefaultOperatorResolver = om
}

type operatorManager struct {
	operators map[string]string
	lock      sync.Mutex
}

func (o *operatorManager) ResolveOperator(receiverID string) (string, error) {
	o.lock.Lock()
	defer o.lock.Unlock()
	host, found := o.operators[receiverID]
	if !found {
		return "", fmt.Errorf("Operator not found")
	}
	return host, nil
}

func (o *operatorManager) SetOperator(receiverID string, address string) error {
	if address == "" {
		return fmt.Errorf("SetOperator error: address cannot be empty. Perhaps forgot to set the operator's address before serving.")
	}
	o.lock.Lock()
	defer o.lock.Unlock()
	o.operators[receiverID] = address
	return nil
}

type ServiceResolver interface {
	SetService(serviceName string, host string) error
	GetService(serviceName string) (string, bool, error)
}

var DefaultServiceResolver ServiceResolver = nil

func init() {
	sm := &MemoryServiceResolver{map[string]string{}, sync.Mutex{}}
	DefaultServiceResolver = sm
}

type MemoryServiceResolver struct {
	Services map[string]string // map from service key to service address
	lock     sync.Mutex
}

func (r *MemoryServiceResolver) SetService(serviceName string, host string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Services[serviceName] = host
	return nil
}

func (r *MemoryServiceResolver) GetService(serviceName string) (string, bool, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	serviceHost, found := r.Services[serviceName]
	return serviceHost, found, nil
}
