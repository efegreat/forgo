package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	"github.com/efegreat/forgo/ratelimit"
)

func ExampleMemory() {
	// http.RateLimit and grpc.RateLimit count in a Memory of their own. RateLimitWith counts in the
	// bootstrap's ILimiter instead, such as forgo-adapter's Redis limiter, which every instance shares.
	limiter := ratelimit.NewMemory(0)
	rate := ratelimit.Rate{Limit: 2, Period: time.Minute}

	for range 3 {
		res, err := limiter.Allow(context.Background(), "client:203.0.113.7", rate)
		if err != nil {
			panic(err)
		}
		fmt.Println(res.Allowed, res.Remaining)
	}
	// Output:
	// true 1
	// true 0
	// false 0
}
