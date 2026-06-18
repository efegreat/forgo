package operation

type ErrorSeverity int

const (
	// SeverityLight indicates a minor incident that needs attention but does not impact operations.
	SeverityLight ErrorSeverity = iota
	// SeverityModerate indicates a significant issue that may affect performance or user experience but is not critical.
	SeverityModerate
	// SeverityCritical indicates a critical issue that impacts the system's functionality. It needs to be handled as soon as possible.
	SeverityCritical
)

type Error struct {
	Severity ErrorSeverity
	Err      error
}

func (e Error) Error() string {
	return e.Err.Error()
}

func (e Error) Unwrap() error {
	return e.Err
}
