# go-ipdb

Self-contained IP database library for Go.

**See also: [go-domaindb](https://github.com/termermc/go-domaindb)**

Automatically downloads and caches various types of IP databases for local in-process querying without reaching out to external APIs.

Supports the following operations:
 - Get ISO 3166 country code for IP address (geoIP)
 - Check if IP address is a Tor exit node
 - Check if an IP address is in a datacenter range (such as a VPN or hosting provider)
 - Optional bring-your-own download logic
 - Optional bring-your-own caching logic

Requests for IP data do not leave your process, and you can aggregate multiple data sources into a single database for better accuracy.

Even if remote lists go down, the library can still function by using cached data.
If you specify multiple data sources, the failing sources will be skipped and the remaining sources will be used.

## Use Cases

This library is useful if:
 - You need to block non-human requests to parts of your application
 - You don't want to rely on an external service for IP data
 - You have strict privacy requirements that prevent using an external service for IP data
 - You need to aggregate multiple IP data sources into a single database

## Download

Add to your project by running:

```bash
go get github.com/termermc/go-ipdb
```

## Examples

See [examples](examples) for more usage examples.

Below are some simpler examples demonstrating the basic usage of the library.

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

## Obtaining Database Files

You can find many different IP lists online in `.txt` format for datacenter and VPN ranges.
A few list URLs are included in the examples directory.
Googling will yield more results. You should avoid any lists that are not updated frequently.

For Tor exit nodes, you can use [Dan's exit node list](https://www.dan.me.uk/torlist/?exit).
Keep in mind that this list rate limits you to 1 request per 30 minutes, so ensure you configure the Ipdb not to download faster than that.
In general, you should be updating your databases every hour, or even less frequently.

MaxMind provides the free GeoLite2 database on their website, but you must agree to their terms of use and sign up before downloading it.
There are public mirrors without this limitation online. You can Google "geolite2 country mmdb" to find them.
A mirror URL from an NPM package is included in the examples directory.

Please keep in mind that the more lists you use, the more memory your process will consume.
The in-memory representation of a database is larger than the database file itself.
