package ipdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/yl2chen/cidranger"
)

const defaultHttpClientTimeout = 10 * time.Second

type dbUpdate struct {
	Ts   time.Time
	Name string
}
type dbSrcRanger struct {
	Has             bool
	Src             *DataSource
	Mu              *xsync.RBMutex
	Ranger          cidranger.Ranger
	LastUpdatedUnix int64
}

// Ipdb stores and updates IP databases.
//
// Includes functionality to:
//   - Resolve ISO 3166 country codes for an IP address
//   - Check if an IP address is a Tor exit node
//   - Check if an IP address appears in a specific list of ranges.
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
	logger     *slog.Logger
	updates    chan dbUpdate

	rangeDbs map[string]*dbSrcRanger

	geoIpv4Src *DataSource
	geoIpv6Src *DataSource

	geoIpv4MmdbMutex *xsync.RBMutex
	geoIpv6MmdbMutex *xsync.RBMutex

	geoIpv4Mmdb *maxminddb.Reader
	geoIpv6Mmdb *maxminddb.Reader

	hasGeoIpv4 bool
	hasGeoIpv6 bool

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

	// By default, Ipdb uses slog.Default.
	// If Logger is specified, it will use it instead.
	Logger *slog.Logger

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
	LoadDatabasesInBackground bool

	// A mapping of database names to their underlying IP range sources.
	// Each source's URL must point to a file containing a newline-separated list of IP addresses and/or CIDR ranges.
	// Empty lines are ignored.
	RangeSources map[string]*DataSource

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

	var logger *slog.Logger
	if options.Logger == nil {
		logger = slog.Default()
	} else {
		logger = options.Logger
	}

	// Create source rangers.
	rangeDbs := make(map[string]*dbSrcRanger)
	for name, src := range options.RangeSources {
		rangeDbs[name] = &dbSrcRanger{
			Has:             false,
			Src:             src,
			Mu:              xsync.NewRBMutex(),
			Ranger:          cidranger.NewPCTrieRanger(),
			LastUpdatedUnix: 0,
		}
	}

	s := &Ipdb{
		storage:    options.StorageDriver,
		disableDl:  options.DisableDownload,
		httpClient: httpClient,
		logger:     logger,
		updates:    make(chan dbUpdate, 8),

		rangeDbs: rangeDbs,

		geoIpv4Src: options.GeoIpv4Source,
		geoIpv6Src: options.GeoIpv6Source,

		geoIpv4MmdbMutex: xsync.NewRBMutex(),
		geoIpv6MmdbMutex: xsync.NewRBMutex(),

		isRunning: true,
	}

	s.logger.Info("initializing",
		"service", "ipdb.Ipdb",
	)

	var downloadedGeoIpv4Unix int64
	var downloadedGeoIpv6Unix int64

	alreadyHadCheckpoints := false
	checkpoints, err := s.storage.ReadCheckpoints()
	if err == nil {
		alreadyHadCheckpoints = true
	} else {
		if errors.Is(err, syscall.ENOENT) {
			checkpoints = &AllCheckpoints{
				Checkpoints: make(map[string]Checkpoint),
			}
		} else {
			return nil, fmt.Errorf("failed to load checkpoints during initialization: %w", err)
		}
	}

	setup := func() error {
		var err error

		toClose := make([]io.Closer, 0, len(rangeDbs))
		defer func() {
			for _, c := range toClose {
				if c != nil {
					_ = c.Close()
				}
			}
		}()

		for name, data := range rangeDbs {
			// Read databases.
			if !s.isRunning {
				return nil
			}

			var reader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Debug("reading range database from cache",
					"service", "ipdb.Ipdb",
					"database_name", name,
				)

				reader, err = s.storage.ReadDatabase(name)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return fmt.Errorf(`failed to read database with name "%s" during initialization: %w`, name, err)
				}
				toClose = append(toClose, reader)
			}
			if reader == nil {
				// No cached database.
				if s.disableDl {
					return fmt.Errorf(`cannot download database with name "%s" during initialization: %w`, name, ErrNoCacheAndNoDownload)
				}

				// Try downloading it.
				err = s.DownloadAndLoadRangeDatabase(name)
				if err != nil {
					return fmt.Errorf(`failed to download database with name "%s" during initialization: %w`, name, err)
				}

				data.LastUpdatedUnix = time.Now().Unix()
			} else {
				err = s.loadRangesFromReader(reader, name)
				if err != nil {
					return fmt.Errorf(`failed to load database with name "%s" during initialization: %w`, name, err)
				}
			}
		}

		if !s.isRunning {
			return nil
		}
		if s.geoIpv4Src == nil {
			s.logger.Info("no data source for geo IPv4 database; corresponding database will not be loaded",
				"service", "ipdb.Ipdb",
			)
		} else {
			var geoIpv4Reader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Debug("reading geo IPv4 database from cache",
					"service", "ipdb.Ipdb",
				)

				geoIpv4Reader, err = s.storage.ReadDatabase(DbNameGeoIpv4)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return fmt.Errorf("failed to read geo IPv4 database during initialization: %w", err)
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
					return fmt.Errorf("cannot download geo IPv4 database: %w", ErrNoCacheAndNoDownload)
				}

				// Try downloading it.
				err = s.DownloadAndLoadGeoIpv4Mmdb()
				if err != nil {
					return fmt.Errorf("failed to download geo IPv4 database during initialization: %w", err)
				}

				downloadedGeoIpv4Unix = time.Now().Unix()
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv4Reader, 4)
				if err != nil {
					return fmt.Errorf("failed to load geo IPv4 database during initialization: %w", err)
				}
			}
		}

		if !s.isRunning {
			return nil
		}
		if s.geoIpv6Src == nil {
			s.logger.Info("no data source for geo IPv6 database; corresponding database will not be loaded",
				"service", "ipdb.Ipdb",
			)
		} else {
			var geoIpv6Reader io.ReadCloser
			if alreadyHadCheckpoints {
				s.logger.Debug("reading geo IPv6 database from cache",
					"service", "ipdb.Ipdb",
				)

				geoIpv6Reader, err = s.storage.ReadDatabase(DbNameGeoIpv6)
				if err != nil && !errors.Is(err, syscall.ENOENT) {
					return fmt.Errorf("failed to read geo IPv6 database during initialization: %w", err)
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
					return fmt.Errorf("cannot download geo IPv6 database: %w", ErrNoCacheAndNoDownload)
				}

				// Try downloading it.
				err = s.DownloadAndLoadGeoIpv6Mmdb()
				if err != nil {
					return fmt.Errorf("failed to download geo IPv6 database during initialization: %w", err)
				}

				downloadedGeoIpv6Unix = time.Now().Unix()
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv6Reader, 6)
				if err != nil {
					return fmt.Errorf("failed to load geo IPv6 database during initialization: %w", err)
				}
			}
		}

		if !s.isRunning {
			return nil
		}

		// Populate checkpoints as needed.
		for name, data := range rangeDbs {
			var chkPnt Checkpoint
			var has bool
			chkPnt, has = checkpoints.Checkpoints[name]
			if !has {
				chkPnt = Checkpoint{
					LastUpdatedUnix: 0,
				}
			}

			if data.LastUpdatedUnix != 0 {
				chkPnt.LastUpdatedUnix = data.LastUpdatedUnix
			}

			checkpoints.Checkpoints[name] = chkPnt
		}
		{
			var chkPnt Checkpoint
			var has bool
			chkPnt, has = checkpoints.Checkpoints[DbNameGeoIpv4]
			if !has {
				chkPnt = Checkpoint{
					LastUpdatedUnix: 0,
				}
			}

			if downloadedGeoIpv4Unix != 0 {
				chkPnt.LastUpdatedUnix = downloadedGeoIpv4Unix
			}

			checkpoints.Checkpoints[DbNameGeoIpv4] = chkPnt
		}
		{
			var chkPnt Checkpoint
			var has bool
			chkPnt, has = checkpoints.Checkpoints[DbNameGeoIpv6]
			if !has {
				chkPnt = Checkpoint{
					LastUpdatedUnix: 0,
				}
			}

			if downloadedGeoIpv6Unix != 0 {
				chkPnt.LastUpdatedUnix = downloadedGeoIpv6Unix
			}

			checkpoints.Checkpoints[DbNameGeoIpv6] = chkPnt
		}

		// Save checkpoints.
		// This is necessary because there could have been database downloads, or checkpoints have never been saved.
		err = s.storage.WriteCheckpoints(checkpoints)
		if err != nil {
			return fmt.Errorf("failed to save checkpoints after initial load: %w", err)
		}

		if !s.isRunning {
			return nil
		}

		// In the background, save checkpoint updates.
		go func() {
			for update := range s.updates {
				var chkPnt Checkpoint
				var has bool
				chkPnt, has = checkpoints.Checkpoints[update.Name]
				if has {
					chkPnt.LastUpdatedUnix = update.Ts.Unix()
				} else {
					chkPnt = Checkpoint{
						LastUpdatedUnix: update.Ts.Unix(),
					}
				}
				checkpoints.Checkpoints[update.Name] = chkPnt

				err := s.storage.WriteCheckpoints(checkpoints)
				if err != nil {
					s.logger.Error("failed to save checkpoints after receiving checkpoint update",
						"service", "ipdb.Ipdb",
						"database_name", update.Name,
						"error", err,
					)
				}
			}
		}()

		if !s.disableDl {
			// Start updaters for enabled databases.
			for name, data := range rangeDbs {
				chkPnt := checkpoints.Checkpoints[name]
				go s.runUpdater(
					name,
					time.Unix(chkPnt.LastUpdatedUnix, 0),
					data.Src.RefreshInterval,
				)
			}
			{
				chkPnt := checkpoints.Checkpoints[DbNameGeoIpv4]
				go s.runUpdater(
					DbNameGeoIpv4,
					time.Unix(chkPnt.LastUpdatedUnix, 0),
					s.geoIpv4Src.RefreshInterval,
				)
			}
			{
				chkPnt := checkpoints.Checkpoints[DbNameGeoIpv6]
				go s.runUpdater(
					DbNameGeoIpv6,
					time.Unix(chkPnt.LastUpdatedUnix, 0),
					s.geoIpv6Src.RefreshInterval,
				)
			}
		}

		s.logger.Info("finished initializing Ipdb",
			"service", "ipdb.Ipdb",
		)

		return nil
	}

	if options.LoadDatabasesInBackground {
		s.logger.Debug("loading databases in the background, as requested by Ipdb options",
			"service", "ipdb.Ipdb",
		)
		go func() {
			if err := setup(); err != nil {
				s.logger.Error("failed to initialize Ipdb in the background",
					"service", "ipdb.Ipdb",
					"error", err,
				)
			}
		}()
	} else {
		if err := setup(); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// runUpdater runs the updater for the specified DB type.
func (s *Ipdb) runUpdater(name string, lastUpdate time.Time, updateInterval time.Duration) {
	if !s.isRunning {
		return
	}

	s.logger.Debug("running updater for database",
		"service", "ipdb.Ipdb",
		"database_name", name,
	)

	update := func() error {
		var err error
		switch name {
		case DbNameGeoIpv4:
			err = s.DownloadAndLoadGeoIpv4Mmdb()
		case DbNameGeoIpv6:
			err = s.DownloadAndLoadGeoIpv6Mmdb()
		default:
			err = s.DownloadAndLoadRangeDatabase(name)
		}
		if err != nil {
			return err
		}

		if !s.isRunning {
			return ErrIpdbClosed
		}
		s.updates <- dbUpdate{
			Ts:   time.Now(),
			Name: name,
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
		s.logger.Error("failed to do first scheduled update of database",
			"service", "ipdb.Ipdb",
			"database_name", name,
			"error", err,
		)
	}

	ticker := time.NewTicker(updateInterval)
	for s.isRunning {
		<-ticker.C
		if !s.isRunning {
			return
		}

		err = update()
		if err != nil {
			s.logger.Error("failed to do scheduled update of database",
				"service", "ipdb.Ipdb",
				"database_name", name,
				"error", err,
			)
		}
	}
}

// openDataSource opens a data source.
// The caller must close the returned reader.
// If the data source has no sources, ErrDataSourceNoSource is returned.
func (s *Ipdb) openDataSource(src *DataSource) (io.ReadCloser, error) {
	var reader io.ReadCloser

	if src.Get != nil {
		s.logger.Debug("starting download of database with source Get function",
			"service", "ipdb.Ipdb",
		)

		var err error
		reader, err = src.Get()
		if err != nil {
			return nil, fmt.Errorf("failed to get database (source Get function): %w", err)
		}

		s.logger.Debug("finished download of database with source Get function",
			"service", "ipdb.Ipdb",
		)
	} else if len(src.Urls) > 0 {
		pipeReader, pipeWriter := io.Pipe()

		go func() {
			var err error
			var resp *http.Response

			failures := make([]error, 0, len(src.Urls))

			for _, srcUrl := range src.Urls {
				func() {
					s.logger.Debug("starting download of database",
						"service", "ipdb.Ipdb",
						"source_url", srcUrl.String(),
					)
					req := &http.Request{
						Method: http.MethodGet,
						URL:    srcUrl,
					}
					resp, err = s.httpClient.Do(req)
					if err != nil {
						failures = append(failures, fmt.Errorf(`ipdb: failed to download database (source URL "%s"): %w`, srcUrl, err))
						s.logger.Error("failed to download database",
							"service", "ipdb.Ipdb",
							"error", err,
						)
						return
					}

					defer func() {
						_ = resp.Body.Close()
					}()

					if resp.StatusCode != http.StatusOK {
						const bodyPreviewBytes = 1024
						// Try to read first N bytes of body to get a better error message.
						bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, bodyPreviewBytes))

						failures = append(failures, fmt.Errorf(`ipdb: failed to download database (source URL "%s") because status code was %d (expected 200): %s: %w`, srcUrl, resp.StatusCode, string(bodyBytes), err))
						s.logger.Error("failed to download database due to unexpected status code",
							"service", "ipdb.Ipdb",
							"source_url", srcUrl,
							"status_code", resp.StatusCode,
							"expected_status_code", http.StatusOK,
							"body", string(bodyBytes),
						)
						return
					}

					bytesWritten, err := io.Copy(pipeWriter, resp.Body)
					if err != nil {
						failures = append(failures, fmt.Errorf(`ipdb: error while reading database (source URL "%s", bytes written: %d): %w`, srcUrl, bytesWritten, err))
						s.logger.Error("error while reading database",
							"service", "ipdb.Ipdb",
							"error", err,
							"source_url", srcUrl,
							"bytes_written", bytesWritten,
						)
						return
					}

					s.logger.Debug("finished download of database",
						"service", "ipdb.Ipdb",
						"source_url", srcUrl,
					)
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

// loadRangesFromReader reads all IP addresses and CIDR ranges from the reader and inserts them into the database with the specified name.
// Any bare IP addresses without a CIDR range will be treated as /32 for IPv4 and /128 for IPv6.
// Does not close the reader.
// Assumes the database name exists, panics if not; checking the database name is the responsibility of the caller.
func (s *Ipdb) loadRangesFromReader(reader io.Reader, name string) error {
	data := s.rangeDbs[name]

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
				s.logger.Error(msg,
					"service", "ipdb.Ipdb",
					"database_name", name,
					"line", line,
					"error", err,
				)
				failErr := fmt.Errorf("ipdb: %s: %w", msg, err)
				failures = append(failures, failErr)
				continue
			}
		} else {
			// No CIDR range, treat as bare IP address.
			addr, err := netip.ParseAddr(line)
			if err != nil {
				msg := fmt.Sprintf("failed to parse bare IP address \"%s\"", line)
				s.logger.Error(msg,
					"service", "ipdb.Ipdb",
					"database_name", name,
					"line", line,
					"error", err,
				)
				failErr := fmt.Errorf("ipdb: %s: %w", msg, err)
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
			return fmt.Errorf("ipdb: failed to insert IP range \"%s\" into database \"%s\": %w", line, name, err)
		}

		goodLines++
	}

	if len(failures) > goodLines {
		return fmt.Errorf("encountered %d parse failures while loading ranges for database \"%s\", but only %d lines were successfully parsed. file is probably malformed; expected newline-separated list of CIDR ranges or bare IP addresses. this error wraps the encountered parse errors. error(s): %w", len(failures), name, goodLines, errors.Join(failures...))
	}

	data.Mu.Lock()
	data.Has = true
	data.Ranger = ranger
	data.Mu.Unlock()

	return nil
}

// Does not close the reader.
// IP version must be 4 or 6.
// If not 4 or 6, defaults to 4.
func (s *Ipdb) loadGeoIpMmdbFromReader(reader io.Reader, ipVersion int) error {
	s.logger.Debug("loading geoIP database",
		"service", "ipdb.Ipdb",
		"ip_version", ipVersion,
	)

	// Buffer the entire file into memory.
	geoIpMmdbBytes, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read GeoIP database: %w", err)
	}

	db, err := maxminddb.OpenBytes(geoIpMmdbBytes)
	if err != nil {
		return fmt.Errorf("failed to parse GeoIP database: %w", err)
	}

	if ipVersion == 6 {
		s.geoIpv6MmdbMutex.Lock()
		s.hasGeoIpv6 = true
		if s.geoIpv6Mmdb != nil {
			err = s.geoIpv6Mmdb.Close()
			if err != nil {
				s.logger.Error("failed to close old GeoIPv6 MMDB database",
					"service", "ipdb.Ipdb",
					"error", err,
				)
			}
		}
		s.geoIpv6Mmdb = db
		s.geoIpv6MmdbMutex.Unlock()
	} else {
		s.geoIpv4MmdbMutex.Lock()
		s.hasGeoIpv4 = true
		if s.geoIpv4Mmdb != nil {
			err = s.geoIpv4Mmdb.Close()
			if err != nil {
				s.logger.Error("failed to close old GeoIPv4 MMDB database",
					"service", "ipdb.Ipdb",
					"error", err,
				)
			}
		}
		s.geoIpv4Mmdb = db
		s.geoIpv4MmdbMutex.Unlock()
	}

	return nil
}

// DownloadAndLoadRangeDatabase downloads the database with the specified name and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadRangeDatabase(name string) error {
	ctx := context.Background()

	data, has := s.rangeDbs[name]
	if !has {
		return NewNoSuchDatabaseError(name)
	}

	s.logger.Log(ctx, slog.LevelDebug, "downloading and loading randge database",
		"service", "ipdb.Ipdb",
		"database_name", name,
	)

	reader, err := s.openDataSource(data.Src)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return fmt.Errorf(`failed to read from source of data with name "%s": %w`, name, err)
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error, 1)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(name, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadRangesFromReader(parseReader, name)
	if err != nil {
		wrapped := fmt.Errorf(`failed to parse database with name "%s": %w`, name, err)
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return fmt.Errorf(`failed to write database with name "%s": %w`, name, err)
	}

	return nil
}

// DownloadAndLoadGeoIpv4Mmdb downloads the GeoIPv4 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv4Mmdb() error {
	s.logger.Debug("downloading and loading GeoIPv4 MMDB",
		"service", "ipdb.Ipdb",
	)

	reader, err := s.openDataSource(s.geoIpv4Src)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return fmt.Errorf("failed to read from GeoIPv4 MMDB source: %w", err)
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbNameGeoIpv4, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadGeoIpMmdbFromReader(parseReader, 4)
	if err != nil {
		wrapped := fmt.Errorf("failed to parse GeoIPv4 MMDB database: %w", err)
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return fmt.Errorf("failed to write GeoIPv4 MMDB database: %w", err)
	}

	return nil
}

// DownloadAndLoadGeoIpv6Mmdb downloads the GeoIPv6 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv6Mmdb() error {
	s.logger.Debug("downloading and loading GeoIPv6 MMDB",
		"service", "ipdb.Ipdb",
	)

	reader, err := s.openDataSource(s.geoIpv6Src)
	defer func() {
		if reader != nil {
			_ = reader.Close()
		}
	}()
	if err != nil {
		return fmt.Errorf("failed to read from GeoIPv6 MMDB source: %w", err)
	}

	pipeReader, pipeWriter := io.Pipe()

	writeErrChan := make(chan error)
	go func() {
		writeErrChan <- s.storage.WriteDatabase(DbNameGeoIpv6, pipeReader)
	}()

	parseReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.loadGeoIpMmdbFromReader(parseReader, 6)
	if err != nil {
		wrapped := fmt.Errorf("failed to parse GeoIPv6 MMDB database: %w", err)
		_ = pipeWriter.CloseWithError(wrapped)
		return wrapped
	}

	_ = pipeWriter.Close()

	if err := <-writeErrChan; err != nil {
		return fmt.Errorf("failed to write GeoIPv6 MMDB database: %w", err)
	}

	return nil
}

func (s *Ipdb) Close() error {
	close(s.updates)

	s.isRunning = false

	if s.geoIpv4Mmdb != nil {
		if err := s.geoIpv4Mmdb.Close(); err != nil {
			return fmt.Errorf("failed to close GeoIPv4 MMDB database: %w", err)
		}
	}
	if s.geoIpv6Mmdb != nil {
		if err := s.geoIpv6Mmdb.Close(); err != nil {
			return fmt.Errorf("failed to close GeoIPv6 MMDB database: %w", err)
		}
	}

	// Assign nil to all databases to allow the original ones to be freed by the GC.
	for _, data := range s.rangeDbs {
		data.Mu.Lock()
		data.Ranger = nil
		data.Mu.Unlock()
	}
	s.geoIpv4Mmdb = nil
	s.geoIpv6Mmdb = nil
	runtime.GC()

	return nil
}

// IsIpInRangeDb returns whether an IP was found in the specified IP range database.
// If the database does not exist, returns a NoSuchDatabaseError.
// If the database has not been initialized, returns a NotInitializedError.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
func (s *Ipdb) IsIpInRangeDb(dbName string, ip netip.Addr) (bool, error) {
	if !s.isRunning {
		return false, ErrIpdbClosed
	}

	data, has := s.rangeDbs[dbName]
	if !has {
		return false, NewNoSuchDatabaseError(dbName)
	}

	tok := data.Mu.RLock()
	defer data.Mu.RUnlock(tok)

	if !data.Has || data.Ranger == nil {
		return false, NewNotInitializedError(dbName, fmt.Sprintf(`cannot check if IP "%s" is in range database "%s"`, ip.String(), dbName))
	}

	has, err := data.Ranger.Contains(ip.AsSlice())
	if err != nil {
		return false, fmt.Errorf(`failed to check if IP "%s" is in range database "%s": %w`, ip.String(), dbName, err)
	}

	return has, nil
}

// ResolveIpIsoCountry resolves the ISO 3166 country code for the specified IP address.
// The country code is uppercase.
// If no country can be found, defaults to defaultCc.
// If the required database is not initialized, returns a NotInitializedError.
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
			return "", NewNotInitializedError(DbNameGeoIpv6, fmt.Sprintf(`cannot get country code for IPv6 "%s"`, ip.String()))
		}

		db = s.geoIpv6Mmdb
	} else {
		lock := s.geoIpv4MmdbMutex.RLock()
		defer s.geoIpv4MmdbMutex.RUnlock(lock)

		if !s.hasGeoIpv4 {
			return "", NewNotInitializedError(DbNameGeoIpv4, fmt.Sprintf(`cannot get country code for IPv4 "%s"`, ip.String()))
		}

		db = s.geoIpv4Mmdb
	}

	var record struct {
		CountryCode string `maxminddb:"country_code"`
	}

	err := db.Lookup(ip).Decode(&record)
	if err != nil {
		return "", fmt.Errorf(`failed to lookup IP "%s" in geoIP database: %w`, ip.String(), err)
	}

	cc := record.CountryCode
	if cc == "" {
		return defaultCc, nil
	} else {
		return cc, nil
	}
}
