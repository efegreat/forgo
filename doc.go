// Package forgo runs backend applications from configuration: resources, providers and servers.
//
// An App builds a Bootstrap from resources, such as database connections, and providers, the
// constructors of repositories and services, in declaration order. Each server's ServerFactory then
// resolves its handlers and middlewares through a Resolver, and the Bootstrap is sealed before anything
// serves, so no dependency is looked up while the application runs:
//
//	app := forgo.NewApp(
//		forgo.WithName("users-api"),
//		forgo.WithResources(sqlDBConf),
//		forgo.WithProviders(postgresrepo.NewUserRepository, user.NewService),
//		forgo.WithServers(httpConf),
//	)
//	if err := app.Run(); err != nil {
//		os.Exit(1)
//	}
//
// Run serves until SIGINT or SIGTERM, or until a server fails, then terminates every server gracefully
// and closes the resources.
//
// The package also holds LiveConfigs, which load and save configurations that change while the
// application runs, and Env, which reads the environment.
package forgo
