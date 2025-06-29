// Copyright (c) 2025 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package limits

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type resourceExhausted struct {
	used int64
}

func (re *resourceExhausted) Error() string {
	return fmt.Sprintf("resouce exhausted (used %d)", re.used)
}

func IsResourceExhausted(err error) bool {
	var re *resourceExhausted
	return errors.As(err, &re)
}

type Semaphore struct {
	n int
	c chan struct{}
}

func NewSempahore(n int) *Semaphore {
	return &Semaphore{
		n: n,
		c: make(chan struct{}, n),
	}
}

func UnlimitedSemaphore() *Semaphore {
	return NewSempahore(0)
}

func (s *Semaphore) Reserve(ctx context.Context) error {
	if s.n == 0 {
		return nil
	}
	select {
	case s.c <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Semaphore) Release() {
	if s.n == 0 {
		return
	}
	select {
	case <-s.c:
	default:
		panic("semaphore would block on release?")
	}
}

type Quota struct {
	mu sync.Mutex
	q  int64
	u  int64
}

func NewQuota(n int64) *Quota {
	return &Quota{q: n, u: n}
}

func UnlimitedQuota() *Quota {
	return NewQuota(0)
}

func (q *Quota) Reserve(n int64) error {
	if q.q == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.u-n < 0 {
		return &resourceExhausted{used: q.q}
	}
	q.u -= n
	return nil
}
