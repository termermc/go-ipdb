# go-ipdb

Self-contained IP database library for Go.

Automatically downloads and caches various types of IP databases for local in-process querying without reaching out to external IPs.

Supports the following operations:
 - Get ISO 3166 country code for IP address (geoIP)
 - Check if IP address is a Tor exit node
 - Check if an IP address is in a datacenter range (such as a VPN or hosting provider)

## Download

Add to your project by running:

```bash
go get github.com/termermc/go-ipdb
```

## IP to Country

The IP to country system uses the MaxMindDB (`.mmdb`) format. By specifying a URL to a mirror of the MaxMind GeoLite2 database,
your application can resolve the ISO 3166 country code of visitor IP addresses. A default can be provided if the IP address's country is not known.

```go
ip := netip.MustParseAddr("13.107.246.40")

cc, err := ipdb.ResolveIpIsoCountry(ip)
if err != nil {
	panic(err)
}

println(cc) // US
```

## Datacenter Check

The datacenter IP check system uses newline-separated list(s) of CIDR notation IP ranges format.
All lists will be loaded into the in-memory database and IPs can be checked against them.
Multiple lists can be combined for better detection.

```go
ip := netip.MustParseAddr("13.107.246.40")

isDc, err := ipdb.IsIpDatacenter(ip)
if err != nil {
     panic(err)
}

if isDc {
	println("Datacenter IP detected")
}
```

## Tor Exit Node Check

The Tor IP check system uses newline-separated list(s) of CIDR notation IP ranges format, or bare IP addresses without CIDR notation.
Any bare IP addresses will be assumed to be `/32` for IPv4 and `/128` for IPv6.
All lists will be loaded into the in-memory database and IPs can be checked against them.
Multiple lists can be combined for better detection.

```go
ip := netip.MustParseAddr("103.251.167.10")

isTor, err := ipdb.IsIpTorExitNode(ip)
if err != nil {
	panic(err)
}

if isTor {
	println("Tor exit node detected")
}
```
