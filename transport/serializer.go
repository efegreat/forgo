package transport

// ISerializer is used to format the response for the client. It has two methods: Success and Error.
// Both methods report failures that happen while serializing or writing the
// response; they never handle those failures themselves, so the caller decides
// how to log or otherwise react to them.
type ISerializer interface {
	// Success is used to send a successful response with the result data.
	// It returns an error if the response could not be serialized or written.
	Success(result any) error
	// Error is used to send an error response with the error message by mapping the error.
	// The mapped error is written to the client; the returned error only
	// describes a failure to serialize or write that response.
	Error(err error) error
}
