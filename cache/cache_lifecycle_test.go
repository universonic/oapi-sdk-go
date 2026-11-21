package larkcache

import (
	"sync"
	"testing"
	"time"
)

func TestCacheCloseStopsAndJoinsJanitor(t *testing.T) {
	cache := New(time.Hour)
	cache.Set("value", "retained", time.Hour)
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			cache.Close()
			select {
			case <-cache.cron.done:
			default:
				t.Error("Close returned before the janitor exited")
			}
		}()
	}
	callers.Wait()
	cache.Close()
	if got := cache.Get("value"); got != "retained" {
		t.Fatalf("Close changed stored data: %v", got)
	}
}
