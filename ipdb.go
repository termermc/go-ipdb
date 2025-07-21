package ipdb

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/oschwald/maxminddb-golang/v2"
	errors2 "github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/yl2chen/cidranger"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const defaultHttpClientTimeout = 10 * time.Second

// DbType is a type of IP database.
type DbType string

func (t DbType) String() string {
	return string(t)
}

const (
	// DbTypeTorExit is the type for the Tor exit IPs database.
	DbTypeTorExit DbType = "Tor exit nodes"

	// DbTypeDatacenter is the type for the datacenter IPs database.
	DbTypeDatacenter DbType = "datacenter IP ranges"

	// DbTypeGeoIpv4 is the type for the IPv4 to country code database.
	DbTypeGeoIpv4 DbType = "IPv4 geolocation"

	// DbTypeGeoIpv6 is the type for the IPv6 to country code database.
	DbTypeGeoIpv6 DbType = "IPv6 geolocation"
)

type ipdbUpdate struct {
	Ts   time.Time
	Type DbType
}

// Ipdb stores and updates IP databases.
//
// Includes functionality to:
//   - Resolve ISO 3166 country codes for an IP address
//   - Check if an IP address is a Tor exit node
//   - Check if an IP address is used by a datacenter (as opposed to a residential IP)
//
// Databases are cached on disk and updated periodically from data sources.
// At runtime, databases are stored in-memory.
//
// Caches are not aware of which data sources were used to create them, so adding, removing or changing data source URLs or Get method implementations should be followed by clearing the cache.
//
// Create an instance with NewIpdb; do not create an empty Ipdb struct and attempt to use it.
//
// There should only be one instance of Ipdb per storage driver or storage location, and ideally only one per process.
// It is safe to use a single instance of Ipdb across multiple goroutines.
type Ipdb struct {
	storage    StorageDriver
	disableDl  bool
	httpClient *http.Client
	logger     Logger
	updates    chan ipdbUpdate

	torExitSrc    *DataSource
	datacenterSrc *DataSource
	geoIpv4Src    *DataSource
	geoIpv6Src    *DataSource

	torExitRangerMutex    *xsync.RBMutex
	datacenterRangerMutex *xsync.RBMutex
	geoIpv4MmdbMutex      *xsync.RBMutex
	geoIpv6MmdbMutex      *xsync.RBMutex

	torExitRanger    cidranger.Ranger
	datacenterRanger cidranger.Ranger
	geoIpv4Mmdb      *maxminddb.Reader
	geoIpv6Mmdb      *maxminddb.Reader

	hasTorExit    bool
	hasDatacenter bool
	hasGeoIpv4    bool
	hasGeoIpv6    bool

	isRunning bool
}

// DataSource stores source information for IP data.
// It includes the URL the data can be fetched from and the time to wait between updating the data from the URL.
type DataSource struct {
	// Urls are the URLs where the IP data is located.
	// Either Get or Urls must be provided; Get takes precedence over Urls.
	// Any URLs that cannot be fetched will result in an error log and be skipped.
	Urls []*url.URL

	// Get is a function to get the IP data.
	// Either Get or Url must be provided; Get takes precedence over Url.1
	Get func() (io.ReadCloser, error)

	// RefreshInterval is the interval between updating the data from the source.
	RefreshInterval time.Duration
}

// Options are options for creating an Ipdb instance.
// Any omitted DataSource fields will be disabled and unavailable, even if cached files for them exist.
type Options struct {
	// The storage driver used to store cached databases and checkpoint information.
	// Unless you have a custom driver you want to use, you should most likely use FsStorageDriver.
	// Required.
	StorageDriver StorageDriver

	// By default, Ipdb prints log messages to stdout and stderr.
	// If Logger is specified, it will call this function instead.
	// Note that err may be nil.
	Logger Logger

	// Overrides the default HTTP client if not nil.
	// If nil, uses a default HTTP client with a 10-second timeout.
	HttpClient *http.Client

	// If true, disables downloading from sources and only uses cached database files.
	//
	// Important: You must still provide sources for the databases you want to use, regardless of whether download is disabled.
	DisableDownload bool

	// If true, the Ipdb instance will be created without waiting for databases to be loaded.
	// The databases will be loaded asynchronously in the background.
	// This can be useful if you're developing and don't want database loading to block startup.
	// It is NOT recommended for production.
	//
	// Important: Any methods on Ipdb that require databases to be initialized will fail until the databases have loaded.
	// If this is true, you should also provide a function for the OnError field to handle errors that might occur while loading the databases in the background.
	LoadDatabasesInBackground bool

	// The source for the Tor exit nodes list.
	// The URL must point to a file containing a newline-separated list of IP addresses (plain IP addresses like `127.0.0.1`, not CIDR with network like `127.0.0.1/8`).
	// Empty lines are ignored.
	//
	// Tor exit IP checking will be disabled if this field is nil.
	TorExitIpsSource *DataSource

	// The source for the datacenter IPs list.
	// The URL must point to a file containing a newline-separated list of CIDR notation addresses (like `192.0.2.0/24` or `2001:db8::/32`)
	// Empty lines are ignored.
	// Datacenter IP checking will be disabled if this field is nil.
	DatacenterIpsSource *DataSource

	// The source for the IPv4 country database.
	// The URL must point to a MaxMindDB (.mmdb) file, containing at least the fields provided by MaxMind's GeoLite2 database.
	// See: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/
	//
	// Resolving IPv4 country codes will be disabled if this field is nil.
	GeoIpv4Source *DataSource

	// The source for the IPv6 country database.
	// The URL must point to a MaxMindDB (.mmdb) file, containing at least the fields provided by MaxMind's GeoLite2 database.
	// See: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/
	//
	// Resolving IPv6 country codes will be disabled if this field is nil.
	GeoIpv6Source *DataSource
}

// NewIpdb creates a new Ipdb instance.
// Blocks until the databases are loaded, unless Options.LoadDatabasesInBackground is true.
// There should only be one instance of Ipdb per storage driver or storage location, and ideally only one per process.
// If error is nil, the returned Ipdb instance will never be nil.
func NewIpdb(options Options) (*Ipdb, error) {
	var httpClient *http.Client
	if options.HttpClient == nil {
		httpClient = &http.Client{
			Timeout: defaultHttpClientTimeout,
		}
	} else {
		httpClient = options.HttpClient
	}

	var logger Logger
	if options.Logger == nil {
		logger = NewDefaultLogger(nil)
	} else {
		logger = options.Logger
	}

	s := &Ipdb{
		storage:    options.StorageDriver,
		disableDl:  options.DisableDownload,
		httpClient: httpClient,
		logger:     logger,
		updates:    make(chan ipdbUpdate, 8),

		torExitSrc:    options.TorExitIpsSource,
		datacenterSrc: options.DatacenterIpsSource,
		geoIpv4Src:    options.GeoIpv4Source,
		geoIpv6Src:    options.GeoIpv6Source,

		torExitRangerMutex:    xsync.NewRBMutex(),
		datacenterRangerMutex: xsync.NewRBMutex(),
		geoIpv4MmdbMutex:      xsync.NewRBMutex(),
		geoIpv6MmdbMutex:      xsync.NewRBMutex(),

		isRunning: true,
	}

	s.logger.Log(LogLevelInfo, "initializing Ipdb", nil)

	var downloadedTorExitUnix int64
	var downloadedDatacenterUnix int64
	var downloadedGeoIpv4Unix int64
	var downloadedGeoIpv6Unix int64

	alreadyHadCheckpoints := false
	checkpoints, err := s.storage.ReadCheckpoints()
	if err == nil {
		alreadyHadCheckpoints = true
	} else {
		if errors.Is(err, syscall.ENOENT) {
			checkpoints = &AllCheckpointsDefault
		} else {
			return nil, errors2.Wrap(err, "failed to load checkpoints during initialization")
		}
	}

	setup := func() error {
		var err error

		// Read databases.
		if !s.isRunning {
			return nil
		}
		if s.torExitSrc == nil {
			s.logger.Log(LogLevelInfo, "no data source for Tor exit nodes database; corresponding database will not be loaded", nil)
		} else {
			var torExitReader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Log(LogLevelDebug, "reading Tor exit nodes database from cache", nil)

				torExitReader, err = s.storage.ReadDatabase(DbTypeTorExit)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return errors2.Wrap(err, "failed to read Tor exit nodes database during initialization")
				}
				defer func() {
					if torExitReader != nil {
						_ = torExitReader.Close()
					}
				}()
			}
			if torExitReader == nil {
				// No cached database.
				if s.disableDl {
					return errors2.Wrap(ErrNoCacheAndNoDownload, "cannot download tor exit nodes database")
				}

				// Try downloading it.
				err = s.DownloadAndLoadTorExitNodeDatabase()
				if err != nil {
					return errors2.Wrap(err, "failed to download Tor exit nodes database during initialization")
				}

				downloadedTorExitUnix = time.Now().Unix()
			} else {
				err = s.loadTorExitNodeDatabaseFromReader(torExitReader)
				if err != nil {
					return errors2.Wrap(err, "failed to load Tor exit nodes database during initialization")
				}
			}
		}

		if !s.isRunning {
			return nil
		}
		if s.datacenterSrc == nil {
			s.logger.Log(LogLevelInfo, "no data source for datacenter ranges database; corresponding database will not be loaded", nil)
		} else {
			var dcRangesReader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Log(LogLevelDebug, "reading datacenter ranges database from cache", nil)

				dcRangesReader, err = s.storage.ReadDatabase(DbTypeDatacenter)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return errors2.Wrap(err, "failed to read datacenter ranges database during initialization")
				}
				defer func() {
					if dcRangesReader != nil {
						_ = dcRangesReader.Close()
					}
				}()
			}
			if dcRangesReader == nil {
				// No cached database.
				if s.disableDl {
					return errors2.Wrap(ErrNoCacheAndNoDownload, "cannot download datacenter ranges database")
				}

				// Try downloading it.
				err = s.DownloadAndLoadDatacenterRangesDatabase()
				if err != nil {
					return errors2.Wrap(err, "failed to download datacenter ranges database during initialization")
				}

				downloadedDatacenterUnix = time.Now().Unix()
			} else {
				err = s.loadDatacenterRangesFromReader(dcRangesReader)
				if err != nil {
					return errors2.Wrap(err, "failed to load datacenter ranges database during initialization")
				}
			}
		}

		if !s.isRunning {
			return nil
		}
		if s.geoIpv4Src == nil {
			s.logger.Log(LogLevelInfo, "no data source for geo IPv4 database; corresponding database will not be loaded", nil)
		} else {
			var geoIpv4Reader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Log(LogLevelDebug, "reading geo IPv4 database from cache", nil)

				geoIpv4Reader, err = s.storage.ReadDatabase(DbTypeGeoIpv4)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return errors2.Wrap(err, "failed to read geo IPv4 database during initialization")
				}
				defer func() {
					if geoIpv4Reader != nil {
						_ = geoIpv4Reader.Close()
					}
				}()
			}
			if geoIpv4Reader == nil {
				// No cached database.
				if s.disableDl {
					return errors2.Wrap(ErrNoCacheAndNoDownload, "cannot download geo IPv4 database")
				}

				// Try downloading it.
				err = s.DownloadAndLoadGeoIpv4Mmdb()
				if err != nil {
					return errors2.Wrap(err, "failed to download geo IPv4 database during initialization")
				}

				downloadedGeoIpv4Unix = time.Now().Unix()
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv4Reader, DbTypeGeoIpv4)
				if err != nil {
					return errors2.Wrap(err, "failed to load geo IPv4 database during initialization")
				}
			}
		}

		if !s.isRunning {
			return nil
		}
		if s.geoIpv6Src == nil {
			s.logger.Log(LogLevelInfo, "no data source for geo IPv6 database; corresponding database will not be loaded", nil)
		} else {
			var geoIpv6Reader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Log(LogLevelDebug, "reading geo IPv6 database from cache", nil)

				geoIpv6Reader, err = s.storage.ReadDatabase(DbTypeGeoIpv6)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return errors2.Wrap(err, "failed to read geo IPv6 database during initialization")
				}
				defer func() {
					if geoIpv6Reader != nil {
						_ = geoIpv6Reader.Close()
					}
				}()
			}
			if geoIpv6Reader == nil {
				// No cached database.
				if s.disableDl {
					return errors2.Wrap(ErrNoCacheAndNoDownload, "cannot download geo IPv6 database")
				}

				// Try downloading it.
				err = s.DownloadAndLoadGeoIpv6Mmdb()
				if err != nil {
					return errors2.Wrap(err, "failed to download geo IPv6 database during initialization")
				}

				downloadedGeoIpv6Unix = time.Now().Unix()
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv6Reader, DbTypeGeoIpv6)
				if err != nil {
					return errors2.Wrap(err, "failed to load geo IPv6 database during initialization")
				}
			}
		}

		if !s.isRunning {
			return nil
		}

		if downloadedTorExitUnix != 0 {
			checkpoints.TorExitDb.LastUpdatedUnix = downloadedTorExitUnix
		}
		if downloadedDatacenterUnix != 0 {
			checkpoints.DatacenterDb.LastUpdatedUnix = downloadedDatacenterUnix
		}
		if downloadedGeoIpv4Unix != 0 {
			checkpoints.GeoIpv4Db.LastUpdatedUnix = downloadedGeoIpv4Unix
		}
		if downloadedGeoIpv6Unix != 0 {
			checkpoints.GeoIpv6Db.LastUpdatedUnix = downloadedGeoIpv6Unix
		}

		// Save checkpoints.
		// This is necessary because there could have been database downloads, or checkpoints have never been saved.
		err = s.storage.WriteCheckpoints(checkpoints)
		if err != nil {
			return errors2.Wrap(err, "failed to save checkpoints after initial load")
		}

		if !s.isRunning {
			return nil
		}

		// In the background, save checkpoint updates.
		go func() {
			for update := range s.updates {
				switch update.Type {
				case DbTypeTorExit:
					checkpoints.TorExitDb.LastUpdatedUnix = update.Ts.Unix()
					break
				case DbTypeDatacenter:
					checkpoints.DatacenterDb.LastUpdatedUnix = update.Ts.Unix()
					break
				case DbTypeGeoIpv4:
					checkpoints.GeoIpv4Db.LastUpdatedUnix = update.Ts.Unix()
					break
				case DbTypeGeoIpv6:
					checkpoints.GeoIpv6Db.LastUpdatedUnix = update.Ts.Unix()
					break
				default:
					s.logger.Log(LogLevelError, fmt.Sprintf("received checkpoint update for unknown database type \"%s\", this is a bug in the library", update.Type), nil)
				}

				err := s.storage.WriteCheckpoints(checkpoints)
				if err != nil {
					s.logger.Log(LogLevelError, fmt.Sprintf("failed to save checkpoints after receiving checkpoint update for %s database", update.Type), err)
				}
			}
		}()

		if !s.disableDl {
			// Start updaters for enabled databases.
			if s.torExitSrc != nil {
				go s.runUpdater(
					time.Unix(checkpoints.TorExitDb.LastUpdatedUnix, 0),
					s.torExitSrc.RefreshInterval,
					DbTypeTorExit,
					s.DownloadAndLoadTorExitNodeDatabase,
				)
			}
			if s.datacenterSrc != nil {
				go s.runUpdater(
					time.Unix(checkpoints.DatacenterDb.LastUpdatedUnix, 0),
					s.datacenterSrc.RefreshInterval,
					DbTypeDatacenter,
					s.DownloadAndLoadDatacenterRangesDatabase,
				)
			}
			if s.geoIpv4Src != nil {
				go s.runUpdater(
					time.Unix(checkpoints.GeoIpv4Db.LastUpdatedUnix, 0),
					s.geoIpv4Src.RefreshInterval,
					DbTypeGeoIpv4,
					s.DownloadAndLoadGeoIpv4Mmdb,
				)
			}
			if s.geoIpv6Src != nil {
				go s.runUpdater(
					time.Unix(checkpoints.GeoIpv6Db.LastUpdatedUnix, 0),
					s.geoIpv6Src.RefreshInterval,
					DbTypeGeoIpv6,
					s.DownloadAndLoadGeoIpv6Mmdb,
				)
			}
		}

		s.logger.Log(LogLevelInfo, "finished initializing Ipdb", nil)

		return nil
	}

	if options.LoadDatabasesInBackground {
		s.logger.Log(LogLevelDebug, "loading databases in the background, as requested by Ipdb options", nil)
		go func() {
			if err := setup(); err != nil {
				s.logger.Log(LogLevelError, "failed to initialize Ipdb in the background", err)
			}
		}()
	} else {
		if err := setup(); err != nil {
			return nil, nil
		}
	}

	return s, nil
}

// runUpdater runs the updater for the specified DB type.
func (s *Ipdb) runUpdater(lastUpdate time.Time, updateInterval time.Duration, dbType DbType, updateFunc func() error) {
	if !s.isRunning {
		return
	}

	s.logger.Log(LogLevelDebug, fmt.Sprintf("running updater for %s database", dbType.String()), nil)

	update := func() error {
		if err := updateFunc(); err != nil {
			return err
		}

		if !s.isRunning {
			return ErrIpdbClosed
		}
		s.updates <- ipdbUpdate{
			Ts:   time.Now(),
			Type: dbType,
		}

		// Databases are big, and we want to limit the amount of garbage in memory.
		// Run the GC manually.
		runtime.GC()

		return nil
	}

	firstUpdateTs := lastUpdate.Add(updateInterval)
	firstTimeout := time.NewTimer(firstUpdateTs.Sub(time.Now()))

	// Wait for next update time.
	<-firstTimeout.C
	if !s.isRunning {
		return
	}

	err := update()
	if err != nil {
		s.logger.Log(LogLevelError, fmt.Sprintf("failed to do first scheduled update of %s database", dbType.String()), err)
	}

	ticker := time.NewTicker(updateInterval)
	for s.isRunning {
		<-ticker.C
		if !s.isRunning {
			return
		}

		err = update()
		if err != nil {
			s.logger.Log(LogLevelError, fmt.Sprintf("failed to do scheduled update of %s database", dbType.String()), err)
		}
	}
}

// openDataSource opens a data source.
// The caller must close the returned reader.
// If the data source has no sources, ErrDataSourceNoSource is returned.
func (s *Ipdb) openDataSource(src *DataSource) (io.ReadCloser, error) {
	var reader io.ReadCloser

	if src.Get != nil {
		s.logger.Log(LogLevelDebug, "starting download of database with source Get function", nil)

		var err error
		reader, err = src.Get()
		if err != nil {
			return nil, errors2.Wrap(err, "failed to get database (source Get function)")
		}

		s.logger.Log(LogLevelDebug, "finished download of database with source Get function", nil)
	} else if len(src.Urls) > 0 {
		pipeReader, pipeWriter := io.Pipe()

		go func() {
			var err error
			var resp *http.Response

			failures := make([]error, 0, len(src.Urls))

			for _, srcUrl := range src.Urls {
				func() {
					s.logger.Log(LogLevelDebug, fmt.Sprintf("starting download of database with source URL \"%s\"", srcUrl), nil)
					req := &http.Request{
						Method: http.MethodGet,
						URL:    srcUrl,
					}
					resp, err = s.httpClient.Do(req)
					if err != nil {
						msg := fmt.Sprintf("failed to download database (source URL \"%s\")", srcUrl)
						failures = append(failures, errors2.Wrap(err, msg))
						s.logger.Log(LogLevelError, msg, err)
						return
					}

					defer func() {
						_ = resp.Body.Close()
					}()

					if resp.StatusCode != http.StatusOK {
						const bodyPreviewBytes = 1024
						// Try to read first N bytes of body to get a better error message.
						bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, bodyPreviewBytes))

						msg := fmt.Sprintf("failed to download database (source URL \"%s\") because status code was %d (expected 200): %s", srcUrl, resp.StatusCode, string(bodyBytes))
						failures = append(failures, errors2.Wrap(err, msg))
						s.logger.Log(LogLevelError, msg, err)
						return
					}

					bytesWritten, err := io.Copy(pipeWriter, resp.Body)
					if err != nil {
						msg := fmt.Sprintf("error while reading database (source URL \"%s\", bytes written: %d)", srcUrl, bytesWritten)
						failures = append(failures, errors2.Wrap(err, msg))
						s.logger.Log(LogLevelError, msg, err)
						return
					}

					s.logger.Log(LogLevelDebug, fmt.Sprintf("finished download of database with source URL \"%s\"", srcUrl), nil)
				}()

				// Write a newline to ensure the next URL body is read on a new line.
				_, _ = pipeWriter.Write([]byte("\n"))
			}

			if len(failures) == len(src.Urls) {
				// All URLs failed; close the pipe writer with ErrAllUrlsFailed and the errors.
				failures = append(failures, ErrAllUrlsFailed)
				_ = pipeWriter.CloseWithError(errors.Join(failures...))
			} else {
				_ = pipeWriter.Close()
			}
		}()

		reader = pipeReader
	} else {
		return nil, ErrDataSourceNoSource
	}

	return reader, nil
}

// loadRangesFromReader reads all IP addresses and CIDR ranges from the reader and inserts them into the specified ranger.
// Calls assignFunc with the ranger after reading all IP addresses and CIDR ranges.
// Any bare IP addresses without a CIDR range will be treated as /32 for IPv4 and /128 for IPv6.
// Does not close the reader.
func (s *Ipdb) loadRangesFromReader(reader io.Reader, assignFunc func(ranger cidranger.Ranger)) error {
	ranger := cidranger.NewPCTrieRanger()
	var err error

	const maxFailures = 10
	failures := make([]error, 0, maxFailures)

	goodLines := 0

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() && len(failures) < maxFailures {
		line := scanner.Text()

		// Skip empty lines and comments.
		if line == "" || line[0] == '#' {
			continue
		}

		var ipNet *net.IPNet

		if strings.Contains(line, "/") {
			// CIDR range.
			_, ipNet, err = net.ParseCIDR(line)
			if err != nil {
				msg := fmt.Sprintf("failed to parse CIDR range \"%s\"", line)
				s.logger.Log(LogLevelError, msg, err)
				failErr := errors2.Wrap(err, msg)
				failures = append(failures, failErr)
				continue
			}
		} else {
			// No CIDR range, treat as bare IP address.
			addr, err := netip.ParseAddr(line)
			if err != nil {
				msg := fmt.Sprintf("failed to parse bare IP address \"%s\"", line)
				failErr := errors2.Wrap(err, msg)
				s.logger.Log(LogLevelError, msg, err)
				failures = append(failures, failErr)
				continue
			}

			var ip net.IP
			var mask net.IPMask
			if addr.Is6() {
				ipBytes := addr.As16()
				ip = ipBytes[:]
				mask = net.CIDRMask(128, 128)
			} else {
				ipBytes := addr.As4()
				ip = ipBytes[:]
				mask = net.CIDRMask(32, 32)
			}

			ipNet = &net.IPNet{
				IP:   ip,
				Mask: mask,
			}
		}

		err = ranger.Insert(cidranger.NewBasicRangerEntry(*ipNet))
		if err != nil {
			return errors2.Wrapf(err, "failed to insert datacenter IP range \"%s\"", line)
		}

		goodLines++
	}

	if len(failures) > goodLines {
		return errors2.Wrapf(errors.Join(failures...), "encountered %d parse failures while loading datacenter ranges, but only %d lines were successfully parsed. file is probably malformed; expected newline-separated list of CIDR ranges or bare IP addresses. this error wraps the encountered parse errors.", len(failures), goodLines)
	}

	assignFunc(ranger)

	return nil
}

func (s *Ipdb) torExitAssignFunc(ranger cidranger.Ranger) {
	s.torExitRangerMutex.Lock()
	s.torExitRanger = ranger
	s.hasTorExit = true
	s.torExitRangerMutex.Unlock()
}
func (s *Ipdb) dcRangesAssignFunc(ranger cidranger.Ranger) {
	s.datacenterRangerMutex.Lock()
	s.datacenterRanger = ranger
	s.hasDatacenter = true
	s.datacenterRangerMutex.Unlock()
}

// Does not close the reader.
func (s *Ipdb) loadTorExitNodeDatabaseFromReader(reader io.Reader) error {
	err := s.loadRangesFromReader(reader, s.torExitAssignFunc)
	if err != nil {
		return errors2.Wrap(err, "failed to load Tor exit node database from reader")
	}

	return nil
}

// Does not close the reader.
func (s *Ipdb) loadDatacenterRangesFromReader(reader io.Reader) error {
	err := s.loadRangesFromReader(reader, s.dcRangesAssignFunc)
	if err != nil {
		return errors2.Wrap(err, "failed to load datacenter ranges from reader")
	}
	return nil
}

// Does not close the reader.
func (s *Ipdb) loadGeoIpMmdbFromReader(reader io.Reader, dbType DbType) error {
	s.logger.Log(LogLevelDebug, fmt.Sprintf("loading %s database", dbType.String()), nil)

	// Buffer the entire file into memory.
	geoIpMmdbBytes, err := io.ReadAll(reader)
	if err != nil {
		return errors2.Wrap(err, "failed to read GeoIP database")
	}

	db, err := maxminddb.FromBytes(geoIpMmdbBytes)
	if err != nil {
		return errors2.Wrap(err, "failed to parse GeoIP database")
	}

	if dbType == DbTypeGeoIpv4 {
		s.geoIpv4MmdbMutex.Lock()
		s.geoIpv4Mmdb = db
		s.hasGeoIpv4 = true
		s.geoIpv4MmdbMutex.Unlock()
	} else {
		s.geoIpv6MmdbMutex.Lock()
		s.geoIpv6Mmdb = db
		s.hasGeoIpv6 = true
		s.geoIpv6MmdbMutex.Unlock()
	}

	return nil
}

// DownloadAndLoadTorExitNodeDatabase downloads the Tor exit node database and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadTorExitNodeDatabase() error {
	s.logger.Log(LogLevelDebug, "downloading and loading Tor exit node database", nil)

	reader, err := s.openDataSource(s.torExitSrc)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return errors2.Wrap(err, "failed to read from Tor exit node database source")
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error, 1)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbTypeTorExit, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadTorExitNodeDatabaseFromReader(parseReader)
	if err != nil {
		wrapped := errors2.Wrap(err, "failed to parse Tor exit node database")
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return errors2.Wrap(err, "failed to write Tor exit node database")
	}

	return nil
}

// DownloadAndLoadDatacenterRangesDatabase downloads the datacenter ranges database and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadDatacenterRangesDatabase() error {
	s.logger.Log(LogLevelDebug, "downloading and loading datacenter ranges database", nil)

	reader, err := s.openDataSource(s.datacenterSrc)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return errors2.Wrap(err, "failed to read from datacenter ranges database source")
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbTypeDatacenter, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadDatacenterRangesFromReader(parseReader)
	if err != nil {
		wrapped := errors2.Wrap(err, "failed to parse datacenter ranges database")
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return errors2.Wrap(err, "failed to write datacenter ranges database")
	}

	return nil
}

// DownloadAndLoadGeoIpv4Mmdb downloads the GeoIPv4 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv4Mmdb() error {
	s.logger.Log(LogLevelDebug, "downloading and loading GeoIPv4 MMDB", nil)

	reader, err := s.openDataSource(s.geoIpv4Src)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return errors2.Wrap(err, "failed to read from GeoIPv4 MMDB source")
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbTypeGeoIpv4, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadGeoIpMmdbFromReader(parseReader, DbTypeGeoIpv4)
	if err != nil {
		wrapped := errors2.Wrap(err, "failed to parse GeoIPv4 MMDB database")
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return errors2.Wrap(err, "failed to write GeoIPv4 MMDB database")
	}

	return nil
}

// DownloadAndLoadGeoIpv6Mmdb downloads the GeoIPv6 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv6Mmdb() error {
	s.logger.Log(LogLevelDebug, "downloading and loading GeoIPv6 MMDB", nil)

	reader, err := s.openDataSource(s.geoIpv6Src)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return errors2.Wrap(err, "failed to read from GeoIPv6 MMDB source")
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbTypeGeoIpv6, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadGeoIpMmdbFromReader(parseReader, DbTypeGeoIpv6)
	if err != nil {
		wrapped := errors2.Wrap(err, "failed to parse GeoIPv6 MMDB database")
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return errors2.Wrap(err, "failed to write GeoIPv6 MMDB database")
	}

	return nil
}

func (s *Ipdb) Close() error {
	close(s.updates)

	s.isRunning = false

	if s.geoIpv4Mmdb != nil {
		if err := s.geoIpv4Mmdb.Close(); err != nil {
			return errors2.Wrap(err, "failed to close GeoIPv4 MMDB database")
		}
	}
	if s.geoIpv6Mmdb != nil {
		if err := s.geoIpv6Mmdb.Close(); err != nil {
			return errors2.Wrap(err, "failed to close GeoIPv6 MMDB database")
		}
	}

	// Dereference databases to allow them to be garbage collected.
	s.torExitRanger = nil
	s.datacenterRanger = nil
	s.geoIpv4Mmdb = nil
	s.geoIpv6Mmdb = nil

	// Force garbage collection.
	runtime.GC()

	return nil
}

// IsIpTorExitNode returns whether the specified IP address is a Tor exit node.
// If the required database is not initialized, returns NotInitializedError.
func (s *Ipdb) IsIpTorExitNode(ip netip.Addr) (bool, error) {
	if !s.isRunning {
		return false, ErrIpdbClosed
	}

	lock := s.torExitRangerMutex.RLock()
	defer s.torExitRangerMutex.RUnlock(lock)

	if !s.hasTorExit {
		return false, NewNotInitializedError(DbTypeTorExit, fmt.Sprintf("cannot check if IP \"%s\" is a Tor exit node", ip.String()))
	}

	has, err := s.torExitRanger.Contains(ip.AsSlice())
	if err != nil {
		return false, errors2.Wrapf(err, "failed to check if IP \"%s\" is a Tor exit node", ip.String())
	}

	return has, nil
}

// IsIpDatacenter returns whether the specified IP address is a datacenter IP.
// If the required database is not initialized, returns NotInitializedError.
func (s *Ipdb) IsIpDatacenter(ip netip.Addr) (bool, error) {
	if !s.isRunning {
		return false, ErrIpdbClosed
	}

	lock := s.datacenterRangerMutex.RLock()
	defer s.datacenterRangerMutex.RUnlock(lock)

	if !s.hasDatacenter {
		return false, NewNotInitializedError(DbTypeDatacenter, fmt.Sprintf("cannot check if IP \"%s\" is a datacenter IP", ip.String()))
	}

	has, err := s.datacenterRanger.Contains(ip.AsSlice())
	if err != nil {
		return false, errors2.Wrapf(err, "failed to check if IP \"%s\" is a datacenter IP", ip.String())
	}

	return has, nil
}

// ResolveIpIsoCountry resolves the ISO 3166 country code for the specified IP address.
// The country code is uppercase.
// If no country can be found, defaults to defaultCc.
// If the required database is not initialized, returns NotInitializedError.
//
// To avoid returning an invalid ISO 3166 country code, the defaultCc parameter should ideally be a valid ISO 3166 country code.
// A good placeholder value is "AQ", which is the ISO 3166 country code for Antarctica.
func (s *Ipdb) ResolveIpIsoCountry(ip netip.Addr, defaultCc string) (string, error) {
	if !s.isRunning {
		return "", ErrIpdbClosed
	}

	var db *maxminddb.Reader
	if ip.Is6() {
		lock := s.geoIpv6MmdbMutex.RLock()
		defer s.geoIpv6MmdbMutex.RUnlock(lock)

		if !s.hasGeoIpv4 {
			return "", NewNotInitializedError(DbTypeGeoIpv6, fmt.Sprintf("cannot get country code for IP \"%s\"", ip.String()))
		}

		db = s.geoIpv6Mmdb
	} else {
		lock := s.geoIpv4MmdbMutex.RLock()
		defer s.geoIpv4MmdbMutex.RUnlock(lock)

		if !s.hasGeoIpv4 {
			return "", NewNotInitializedError(DbTypeGeoIpv4, fmt.Sprintf("cannot get country code for IP \"%s\"", ip.String()))
		}

		db = s.geoIpv4Mmdb
	}

	var record struct {
		CountryCode string `maxminddb:"country_code"`
	}

	err := db.Lookup(ip).Decode(&record)
	if err != nil {
		return "", errors2.Wrapf(err, "failed to lookup IP %s in geoIP database", ip.String())
	}

	cc := record.CountryCode
	if cc == "" {
		return defaultCc, nil
	} else {
		return cc, nil
	}
}
