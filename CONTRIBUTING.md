# Contributing to forgo

- **Bugs:** open an issue with the smallest program or test that shows the problem. For a security issue, follow [SECURITY.md](SECURITY.md) instead.
- **Features and changes of behavior:** open an issue to discuss them before you send a pull request. forgo keeps a small, deliberate API, and every change must serve the design principles in [AGENTS.md](AGENTS.md).
- **Adapters** for a database, broker or cache belong to [forgo-adapter](https://github.com/efegreat/forgo-adapter), never to forgo.

## Making a change

[AGENTS.md](AGENTS.md) is the contributor guide: the repository map, the architecture invariants, the coding conventions and the checklists for new servers, middlewares and commands.

Before you open a pull request, run:

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
```

CI also runs staticcheck, govulncheck, the fuzz targets, and the migration integration tests against PostgreSQL and MySQL.

- Update [REFERENCE.md](REFERENCE.md) in the same pull request whenever public API, defaults, config fields, middleware order, log keys, error codes or CLI flags change.
- A change to a contract that forgo-adapter implements needs a matching pull request there.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org): `fix(worker): …`, `feat(http): …`, `docs: …`.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
