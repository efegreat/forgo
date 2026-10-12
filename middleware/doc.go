// Package middleware holds the transport-agnostic middlewares that the built-in servers compose into
// their base chains. Applications can attach them to any server: ID, Logger, Recorder, Recovery,
// Locker, Tolerance and Guard. A middleware that needs a transport's State belongs in that transport's
// package instead.
//
// Base chains use them in this order, outermost first, and leave out what a transport does not need:
// ID → Logger → Locker → Recorder → Tolerance → Recovery → Guard. Each position matters. Logger logs
// the ID. Locker sits outside Recorder, so a run skipped for a held lock logs no operation, and outside
// Tolerance, so the lock spans every attempt. Recorder sits outside Recovery, so a panic is logged as a
// failure. REFERENCE.md lists the base chain of every server.
package middleware
