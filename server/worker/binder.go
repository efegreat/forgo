package worker

import (
	"bytes"
	"encoding/json"

	"github.com/go-playground/validator/v10"
)

// IBinder is an interface that defines a method for binding a message to a Go struct. It is used to extract data from the message and populate the fields of the struct.
type IBinder interface {
	Bind(Message, any) error
}

type defaultBinder struct {
	validator *validator.Validate
}

func (db *defaultBinder) Bind(m Message, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(m.Payload))
	decoder.UseNumber()

	if err := decoder.Decode(dst); err != nil {
		return err
	}

	return db.validator.Struct(dst)
}

func newDefaultBinder() IBinder {
	return &defaultBinder{validator: validator.New(validator.WithRequiredStructEnabled())}
}
