package bridge

import "net"

// Error carries an SDK or ABI failure. Both public packages expose this type.
type Error struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return "gocodex: " + e.Kind + ": " + e.Message }
func (e *Error) Timeout() bool { return e.Kind == "timeout" }

// Temporary completes net.Error so callers can recognize native timeouts.
// Other failures retain their cause without being declared retryable here.
func (e *Error) Temporary() bool { return e.Timeout() }

var _ net.Error = (*Error)(nil)
