package operation

import "fmt"

// ErrorSeverity defines the severity level of an error. It helps categorize errors based on their impact on the system or operation.
type ErrorSeverity int

const (
	// SeverityLight indicates a minor incident that needs attention but does not impact operations.
	SeverityLight ErrorSeverity = iota
	// SeverityModerate indicates a significant issue that may affect performance or user experience but is not critical.
	SeverityModerate
	// SeverityCritical indicates a critical issue that impacts the system's functionality. It needs to be handled as soon as possible.
	SeverityCritical
)

// Error is a struct that encapsulates an error along with its severity level. It implements the error interface, allowing it to be used as a standard error in Go.
type Error struct {
	Severity ErrorSeverity
	Err      error
}

func (e Error) Error() string {
	if e.Err != nil {
		return fmt.Errorf("severity: %d, error: %w", e.Severity, e.Err).Error()
	}
	return ""
}

func (e Error) Unwrap() error {
	return e.Err
}
