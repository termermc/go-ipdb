package ipdb

// Checkpoint is checkpoint information for a database.
type Checkpoint struct {
	// When the database was last updated from source.
	// Unix epoch second timestamp.
	LastUpdatedUnix int64 `json:"last_updated_unix"`
}

// AllCheckpoints is information for all database checkpoints.
type AllCheckpoints struct {
	// All checkpoints.
	// Key is the database name, value is the checkpoint.
	Checkpoints map[string]Checkpoint `json:"checkpoints"`
}
