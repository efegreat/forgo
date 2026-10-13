package transport

// ISerializer writes the response of one operation. A transport creates one per response, bound to where it
// goes (the HTTP server's SerializerFactory binds it to the request's http.ResponseWriter), and calls Success
// or Error once. A serializer called again must not write again: the default HTTP one returns
// http.ErrAlreadyWritten.
//
// Serializers never log: they return their failures and the transport decides what to do with them.
type ISerializer interface {
	// Success writes result as the successful response. A nil result has no content, as EmptyHandler
	// answers. It returns nil once the response is written, or the failure to serialize or write it.
	Success(result any) error
	// Error writes err as the error response. Errors are mapped before the call, so in the HTTP server err
	// is the operation.Error its ErrorMapper returned, whose Status, Code and Message make the response.
	//
	// It returns nil once the response is written. The operation then fails with the error the transport
	// answered, and Recorder logs it whole, with everything wrapped around err. When serializing or writing
	// fails, it returns err wrapped with the failure (operation.Error.Wrap), so the log keeps err's code and
	// severity. Returning err itself once the response is written also works, but the log then loses what was
	// wrapped around it.
	Error(err error) error
}
