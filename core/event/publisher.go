package event

import "context"

// Publisher fans one committed envelope out to the event stream and its
// derived change channels. Implemented by the Redis adapter; consumed by
// the outbox relay. Returns the event-stream entry ID (the relay stores it
// back onto the outbox row).
type Publisher interface {
	Publish(ctx context.Context, env Envelope, channels []string) (streamID string, err error)
}
