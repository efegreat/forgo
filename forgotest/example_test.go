package forgotest_test

import (
	"fmt"
	"io"
	nethttp "net/http"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/server/http"
)

// newApp builds the app, as main does with the same function.
func newApp() *forgo.App {
	return forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithServers(http.Config{
			Addr: ":8080", // the env serves on a loopback port of its own
			Routes: []http.RouteConfig{
				{Name: "health", Method: "GET", Path: "/health", Handler: http.HealthHandler()},
			},
		}),
	)
}

func ExampleNew() {
	// In a test package, this runs in TestMain, once for all of its tests.
	env, err := forgotest.New(newApp(), forgotest.WithLogger(logger.Config{Output: io.Discard}))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer env.Close()

	addr, err := env.Addr(http.ServerNameHTTP)
	if err != nil {
		fmt.Println(err)
		return
	}
	resp, err := nethttp.Get("http://" + addr + "/health")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 200
}
