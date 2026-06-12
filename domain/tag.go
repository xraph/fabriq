package domain

import "github.com/xraph/grove"

// ReadingsSeries is the Timescale hypertable telemetry readings land in.
// Readings are written through Timeseries().BulkWrite — the event-bypass
// path: no per-row outbox events; the worker publishes conflated deltas.
const ReadingsSeries = "tag_readings"

// Tag is telemetry metadata: one measured point on an asset. The tag row
// itself is an ordinary aggregate (created/updated/deleted events); its
// readings are not.
type Tag struct {
	grove.BaseModel `grove:"table:tags"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Name     string `grove:"name,notnull" json:"name"`
	Unit     string `grove:"unit" json:"unit"`         // °C, bar, rpm
	Datatype string `grove:"datatype" json:"datatype"` // float, bool, int
	AssetID  string `grove:"asset_id" json:"asset_id"`
}
