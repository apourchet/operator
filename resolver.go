package operator

import "fmt"

type OperatorResolver interface {
	ResolveOperator(receiverID string) (string, error)
	SetOperator(receiverID string, host string) error
}

var DefaultOperatorResolver OperatorResolver = nil

func init() {
	om := &operatorManager{map[string]string{}}
	DefaultOperatorResolver = om
}

type operatorManager struct {
	operators map[string]string
}

func (o *operatorManager) ResolveOperator(receiverID string) (string, error) {
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
	o.operators[receiverID] = address
	return nil
}

type ServiceResolver interface {
	SetService(serviceName string, host string) error
	GetService(serviceName string) (string, bool, error)
}

var DefaultServiceResolver ServiceResolver = nil

func init() {
	sm := &MemoryServiceResolver{map[string]string{}}
	DefaultServiceResolver = sm
}

type MemoryServiceResolver struct {
	Services map[string]string // map from service key to service address
}

func (r *MemoryServiceResolver) SetService(serviceName string, host string) error {
	r.Services[serviceName] = host
	return nil
}

func (r *MemoryServiceResolver) GetService(serviceName string) (string, bool, error) {
	serviceHost, found := r.Services[serviceName]
	return serviceHost, found, nil
}
