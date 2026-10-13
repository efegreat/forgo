// Package http serves routes with net/http and its pattern-based ServeMux. Each request is an operation of
// type OperationTypeHTTPHandler, so the middlewares of every other server work on it too.
//
// Declare the routes in a Config and pass it to forgo.WithServers. A route's handler receives its request
// bound and validated from the path, query, headers and JSON body, and returns a result or an error. The
// server answers both through its serializer, by default a JSON envelope, and maps errors to an
// operation.Error with its ErrorMapper. A handler can also write the response itself through State.Writer,
// such as a redirect or a file.
//
// The package also ships opt-in middlewares (CORS, RateLimit and JWTAuth) and the admin handlers of live
// configs (LiveConfigsHandler and SetLiveConfigHandler).
package http
