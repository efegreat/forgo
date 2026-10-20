// Package forgotest runs the app that main runs, for feature and E2E tests, so that tests need no app of
// their own.
//
// Build one Env per test package, in TestMain, from the function that also builds the app of main, and
// close it once the tests ran:
//
//	var env *forgotest.Env
//
//	func TestMain(m *testing.M) {
//		var err error
//		if env, err = forgotest.New(app.New(config.HTTP)); err != nil {
//			log.Fatal(err)
//		}
//		defer env.Close()
//		m.Run()
//	}
//
// New does what forgo.App.Run does before serving, so a configuration that fails the app's start fails New
// the same way. It then serves every server that implements IListenerServer, such as the HTTP and gRPC
// servers, on a loopback listener of its own, which tests reach through Env.Addr. The other servers, such
// as cron and the worker, are created and initialized, but never served: nothing runs on a schedule or
// consumes from a broker during tests.
//
// Tests of the HTTP server choose, request by request, between that live server and its router alone:
// Env.Handler returns the router, with the chain of every route, to serve requests in process, such as with
// httptest.NewRecorder, without the network and net/http's server.
//
// The operations of the servers an env never serves run one at a time instead: Env.RunJob runs a cron job
// once, Env.Deliver hands a message to a subscriber of the consumer, as the broker would, and Env.Outbox
// returns the messages operations saved to the outbox, as the producer's relay would publish them. Outbox and
// Deliver together test both ends of a message without a broker.
//
// The tests of a package share the Env and what it is connected to, such as a database, so they reset the
// state they depend on themselves, and do not run in parallel.
package forgotest
