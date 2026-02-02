package main

import (
	"context"
	"errors"
	"sync"
)

type Getter interface {
	Get(ctx context.Context, address, key string) (string, error)
}

func Get(ctx context.Context, getter Getter, addresses []string, key string) (string, error) {
	if len(addresses) == 0 {
		return "", nil
	}

	resultCh := make(chan string)
	seen := make(map[string]bool)

	var wg sync.WaitGroup
	for _, address := range addresses {
		if seen[address] {
			continue
		}
		seen[address] = true

		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			value, err := getter.Get(ctx, addr, key)
			if err != nil {
				return
			}

			resultCh <- value
		}(address)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	select {
	case value, ok := <-resultCh:
		if !ok {
			return "", errors.New("all requests failed")
		}
		return value, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
