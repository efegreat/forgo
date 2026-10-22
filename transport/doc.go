// Package transport holds what transports share beneath the operation layer: ISerializer, which writes the
// response of an operation, and the connection observers a server's Listener reports to.
//
// It depends on the standard library only, so servers and adapters can import it freely.
//
// See the [transport section] of REFERENCE.md.
//
// [transport section]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#transport
package transport
