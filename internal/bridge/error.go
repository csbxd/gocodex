package bridge

// Error carries an SDK or ABI failure. Both public packages expose this type.
type Error struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return "gocodex: " + e.Kind + ": " + e.Message }
func (e *Error) Timeout() bool { return e.Kind == "timeout" }
