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

func (o *operatorManager) SetOperator(receiverID string, host string) error {
	o.operators[receiverID] = host
	return nil
}
