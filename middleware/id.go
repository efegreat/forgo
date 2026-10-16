package middleware

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"time"

	"github.com/efegreat/forgo/operation"
)

// maxIDLen bounds the incoming IDs that ID keeps. It fits every common format, such as UUIDs, ULIDs
// and W3C trace IDs.
const maxIDLen = 128

// ID gives every operation an ID that correlates its logs and its response.
//
// It keeps an ID that the transport took from its input, such as the HTTP X-Request-ID header, when it
// is at most 128 bytes of visible ASCII. Otherwise, and when the ID is empty, it sets a new UUIDv7. An
// incoming ID comes from the client: it appears on every log line of the operation and is echoed in the
// response, so an oversized or malformed one is replaced, never kept.
func ID() operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if !validID(ctx.ID) {
				ctx.ID = newID()
			}
			return next(ctx)
		}
	}
}

// validID reports whether id is non-empty, at most maxIDLen bytes, and only visible ASCII: no spaces,
// control characters or bytes above 0x7e.
func validID(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for i := range len(id) {
		if id[i] < '!' || id[i] > '~' {
			return false
		}
	}
	return true
}

// newID returns a new UUIDv7 (RFC 9562): the Unix time in milliseconds, then the fraction of the millisecond
// in 12 bits, so that IDs sort by time down to about 250ns, then 62 random bits. It takes no lock and
// allocates only the string. An ID correlates logs, so it must be unique, not secret: math/rand/v2 draws from
// a per-thread ChaCha8 source seeded by the system, without the lock of crypto/rand on some systems.
func newID() string {
	now := time.Now()
	fraction := (uint64(now.Nanosecond()%int(time.Millisecond)) << 12) / uint64(time.Millisecond)
	var u [16]byte
	binary.BigEndian.PutUint64(u[:8], uint64(now.UnixMilli())<<16|0x7000|fraction) // version 7
	binary.BigEndian.PutUint64(u[8:], rand.Uint64()&(1<<62-1)|1<<63)               // variant 10

	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])
	return string(buf[:])
}
