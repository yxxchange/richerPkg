package common

import "fmt"

type Logger interface {
	Infof(format string, i ...interface{})
	Errorf(format string, i ...interface{})
}

type DefaultLogger struct{}

func (d *DefaultLogger) Infof(format string, i ...interface{}) {
	fmt.Printf(format+"\n", i...)
}

func (d *DefaultLogger) Errorf(format string, i ...interface{}) {
	fmt.Printf("[Err] "+format+"\n", i...)
}
