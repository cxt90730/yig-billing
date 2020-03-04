package helper

import (
	"errors"
	"sync"
	"time"
)

func WithLock(lock sync.Mutex, f func() error) func() error {
	return func() error {
		lock.Lock()
		defer lock.Unlock()
		return f()
	}
}

func WithRetry(retryTimes int, f func() error) func() error {
	return func() error {
		n := 0
		var err error
		for n < retryTimes {
			n += 1
			err = f()
			if err == nil {
				return nil
			}
			time.Sleep(time.Second)
		}
		return errors.New("max retry times exceeded: " + err.Error())
	}
}
