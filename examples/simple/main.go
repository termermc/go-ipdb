package main

import (
	"fmt"
	"github.com/termermc/go-ipdb"
	"net/netip"
	"net/url"
	"os"
	"time"
)

func mustParseUrl(str string) *url.URL {
	res, err := url.Parse(str)
	if err != nil {
		panic(err)
	}
	return res
}

func main() {
	const ipDbDir = "./ipdb"
	err := os.MkdirAll(ipDbDir, 0755)
	if err != nil {
		panic(err)
	}

	// Use the built-in FsStorageDriver.
	// This stores cached databases and checkpoints in the specified directory.
	// If you want to store them somewhere else, you can implement your own StorageDriver.
	storageDriver, err := ipdb.NewFsStorageDriver(ipDbDir)
	if err != nil {
		panic(err)
	}

	// Use the built-in logger.
	// It logs to the console.
	// Specifying nil for logLevels because we want to print all log messages of every log level.
	// In production, you may want to exclude LogLevelDebug because it is rather verbose.
	// If you use a specific logging system, you can implement your own Logger.
	logger := ipdb.NewDefaultLogger(nil)

	// Create the actual Ipdb instance.
	// You may omit any of the data sources if you do not need them.
	// Read the documentation on the Ipdb struct for more details.
	ipDb, err := ipdb.NewIpdb(ipdb.Options{
		StorageDriver: storageDriver,
		Logger:        logger,

		// You can optionally provide your own HTTP client to use for downloading databases.
		// By default, it uses a client with a 10-second timeout.
		HttpClient: nil,

		// You can set this to true to disable downloading databases.
		// If true, it will only use cached databases.
		// Keep in mind that if there is no cached database available, it will return an error.
		DisableDownload: false,

		// You can set this to true to do most of the initialization in the background.
		// This is useful if you're developing and don't want database loading to block startup.
		// It is NOT recommended for production.
		// See documentation on this field for more details.
		LoadDatabasesInBackground: false,

		// For Tor exit nodes, we're using Dan's exit node list.
		// We're also using another list from X4BNet, hosted on GitHub.
		// If Dan's list is rate limiting us, we'll still have access to X4BNet's list.
		// If both work, they will be merged into a single list.
		TorExitIpsSource: &ipdb.DataSource{
			RefreshInterval: 1 * time.Hour,
			Urls: []*url.URL{
				mustParseUrl("https://www.dan.me.uk/torlist/?exit"),
				mustParseUrl("https://raw.githubusercontent.com/X4BNet/lists_torexit/refs/heads/main/ipv4.txt"),
			},
		},

		// We're using X4BNet's datacenter and VPN lists.
		DatacenterIpsSource: &ipdb.DataSource{
			RefreshInterval: 1 * time.Hour,
			Urls: []*url.URL{
				mustParseUrl("https://raw.githubusercontent.com/X4BNet/lists_vpn/refs/heads/main/output/vpn/ipv4.txt"),
				mustParseUrl("https://raw.githubusercontent.com/X4BNet/lists_vpn/refs/heads/main/output/datacenter/ipv4.txt"),
			},
		},

		// We're using mirrors of MaxMind's GeoLite2 country databases hosted on jsDelivr.
		// These are bundled as part of the @ip-location-db/geolite2-country-mmdb NPM package.
		GeoIpv4Source: &ipdb.DataSource{
			RefreshInterval: 1 * time.Hour,
			Urls: []*url.URL{
				mustParseUrl("https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-country-mmdb/geolite2-country-ipv4.mmdb"),
			},
		},
		GeoIpv6Source: &ipdb.DataSource{
			RefreshInterval: 1 * time.Hour,
			Urls: []*url.URL{
				mustParseUrl("https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-country-mmdb/geolite2-country-ipv6.mmdb"),
			},
		},
	})
	if err != nil {
		panic(err)
	}

	// List of IPs to check.
	ips := []string{
		// Google DNS (obviously datacenter)
		"8.8.8.8",

		// Random residential IPs
		"59.183.80.47",
		"174.118.170.245",

		// Some datacenter IPs
		"13.107.246.40",   // Microsoft
		"184.28.133.90",   // Akamai
		"199.195.254.219", // BuyVM

		// Tor exit nodes
		"2a0d:bbc7::f816:3eff:fee6:ca14",
		"45.84.107.101",
		"192.42.116.194",
	}

	for _, ipStr := range ips {
		ip := netip.MustParseAddr(ipStr)

		isTor, err := ipDb.IsIpTorExitNode(ip)
		if err != nil {
			panic(err)
		}

		isDc, err := ipDb.IsIpDatacenter(ip)
		if err != nil {
			panic(err)
		}

		cc, err := ipDb.ResolveIpIsoCountry(ip, "AQ")
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s is: tor exit node: %v, datacenter: %v, country: %s\n", ip.String(), isTor, isDc, cc)
	}

	// Closing the database frees all databases.
	// The Ipdb instance is no longer usable after closure.
	err = ipDb.Close()
	if err != nil {
		panic(err)
	}

	// In this example, the program terminates.
	// In real-world usage, the program will continue to run and databases will be updated in the background.
	// Add `select {}` to the end of the program to leave it open for a few hours to see log messages caused by the automatic updates.

	//select {}
}
