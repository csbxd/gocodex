package bridge

import (
	"io"
	"net"
)

// Error carries an SDK or ABI failure. Both public packages expose this type.
type Error struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	message := e.Message
	if e.Kind == "unexpected_eof" {
		message = io.ErrUnexpectedEOF.Error() + ": " + message
	}
	return "gocodex: " + e.Kind + ": " + message
}

// Is preserves a native premature EOF while retaining the backend details.
// It must not match io.EOF: truncated responses are failures, not clean endings.
func (e *Error) Is(target error) bool {
	return e.Kind == "unexpected_eof" && target == io.ErrUnexpectedEOF
}

func (e *Error) Timeout() bool { return e.Kind == "timeout" }

// Temporary completes net.Error so callers can recognize native timeouts.
// Other failures retain their cause without being declared retryable here.
func (e *Error) Temporary() bool { return e.Timeout() }

var _ net.Error = (*Error)(nil)
