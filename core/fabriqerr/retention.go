package fabriqerr

import "errors"

// ErrAggregateErased rejects writes to a permanently retired aggregate identity.
var ErrAggregateErased = errors.New("fabriq: aggregate erased")
