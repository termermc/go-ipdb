package ipdb

// Checkpoint is checkpoint information for a database.
type Checkpoint struct {
	// When the database was last updated from source.
	// Unix epoch second timestamp.
	LastUpdatedUnix int64 `json:"last_updated_unix"`
}

// AllCheckpoints is information for all database checkpoints.
type AllCheckpoints struct {
	// Checkpoint information for the Tor exit nodes database.
	TorExitDb Checkpoint `json:"tor_exit_db"`

	// Checkpoint information for the datacenter IPs database.
	DatacenterDb Checkpoint `json:"datacenter_db"`

	// Checkpoint information for the IPv4 to country code database.
	GeoIpv4Db Checkpoint `json:"geo_ipv4_db"`

	// Checkpoint information for the IPv6 to country code database.
	GeoIpv6Db Checkpoint `json:"geo_ipv6_db"`
}

// AllCheckpointsDefault is all default database checkpoint values.
var AllCheckpointsDefault = AllCheckpoints{
	TorExitDb: Checkpoint{
		LastUpdatedUnix: 0,
	},
	DatacenterDb: Checkpoint{
		LastUpdatedUnix: 0,
	},
	GeoIpv4Db: Checkpoint{
		LastUpdatedUnix: 0,
	},
	GeoIpv6Db: Checkpoint{
		LastUpdatedUnix: 0,
	},
}
