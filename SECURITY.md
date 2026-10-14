# Security policy

## Supported versions

Security fixes are released for the latest minor version of forgo.

## Reporting a vulnerability

Do not open a public issue. Report the vulnerability privately through [GitHub's vulnerability reporting](https://github.com/efegreat/forgo/security/advisories/new), with the affected version, what an attacker can do, and a reproduction if you have one.

Areas where a flaw is likely to be a vulnerability: JWT verification (`JWTAuth`, `JWKS`), rate limiting and client IP detection, CORS, request binding, and the masking of `SensitiveFields` and credential headers in logs.

A vulnerability in a dependency belongs to that dependency's maintainers, unless the way forgo uses it makes it exploitable. Vulnerabilities of the adapters belong to [forgo-adapter](https://github.com/efegreat/forgo-adapter/security/advisories/new).
