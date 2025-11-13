package ipdb

// DbNameMaxSize is the max size of the database name, in bytes.
const DbNameMaxSize = 64

// DbNameGeoIpv4 is the database name used for the GeoIP database for IPv4 addresses.
// Do not name any database with this name.
const DbNameGeoIpv4 = "__ipdb_geo_ipv4"

// DbNameGeoIpv6 is the database name used for the GeoIP database for IPv6 addresses.
// Do not name any database with this name.
const DbNameGeoIpv6 = "__ipdb_geo_ipv6"
