package ipdb

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/yl2chen/cidranger"
)

// DefaultMaxDatabaseSize is the default maximum size of a single database, in bytes.
// See Options.MaxDatabaseSize.
const DefaultMaxDatabaseSize int64 = 1 * 1024 * 1024 * 1024 // 1GiB

const (
	// DefaultHttpTimeout is the default HTTP client's timeout for an entire download, including reading the body.
	// It is used as http.Client.Timeout when Options.HttpClient is nil.
	DefaultHttpTimeout = 5 * time.Minute

	// DefaultHttpResponseHeaderTimeout is the default HTTP client's timeout for receiving response headers after
	// sending a request.
	// It is used as http.Transport.ResponseHeaderTimeout when Options.HttpClient is nil.
	DefaultHttpResponseHeaderTimeout = 30 * time.Second
)

const (
	// The delay before the first retry of a failed scheduled update.
	// Doubles on each consecutive failure, up to the source's refresh interval.
	defaultUpdateRetryDelay = 1 * time.Minute

	// The maximum number of range parse failures to log and include in errors per load.
	maxReportedParseFailures = 10

	// The number of bytes of a non-200 response body to include in errors.
	bodyPreviewBytes = 1024
)

type dbKind int

const (
	dbKindRange dbKind = iota
	dbKindGeo
)

// database is a single in-memory database and its source.
type database struct {
	name string
	kind dbKind
	src  *DataSource

	// updateMu serializes downloads of the database.
	updateMu sync.Mutex

	// loaded is the loaded database, or nil if it has not been loaded or the instance has been closed.
	//
	// Lookups load the pointer once and use that snapshot without locking.
	// Updates replace the pointer with a new snapshot; lookups still using the old snapshot finish with it,
	// and it is freed by the GC once they are done. Snapshots are never modified after being stored.
	loaded atomic.Pointer[loadedDb]
}

// loadedDb is an immutable snapshot of a loaded database.
type loadedDb struct {
	// ranger is the loaded range database.
	// Nil if the database is not a range database.
	ranger cidranger.Ranger

	// mmdb is the loaded geoIP database.
	// Nil if the database is not a geoIP database.
	//
	// The reader is intentionally never closed, because lookups may still be using it after it has been replaced.
	// This is safe because it is always created with maxminddb.OpenBytes, so it does not hold a memory-mapped file
	// or any other resource that needs releasing: it only references a byte slice owned by the Go heap.
	// For such readers, Reader.Close only resets the reader's internal fields (it only unmaps memory for readers
	// created with maxminddb.Open), which frees nothing. It would also cause a data race with in-progress lookups:
	// maxminddb documents that Close must not be called concurrently with other Reader or Result methods.
	// Instead, the reader and its data are freed by the GC once the last lookup using it is done.
	// Last verified against maxminddb-golang v2.9.0; re-check this when upgrading it.
	//
	// If readers are ever created with maxminddb.Open instead, they would hold a memory-mapped file.
	// Not closing them would still not leak, since maxminddb registers a cleanup that unmaps the file when the reader
	// is garbage collected, but the mapping would then be held until the GC runs.
	mmdb *maxminddb.Reader
}

func newDatabase(name string, kind dbKind, src *DataSource) *database {
	return &database{
		name: name,
		kind: kind,
		src:  src,
	}
}

func (db *database) isLoaded() bool {
	return db.loaded.Load() != nil
}

// Ipdb stores and updates IP databases.
//
// Includes functionality to:
//   - Resolve ISO 3166 country codes for an IP address
//   - Check if an IP address appears in a specific list of ranges (such as Tor exit nodes or datacenter ranges)
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
	maxDbSize  int64

	retryPolicy RetryPolicy

	updateRetryDelay time.Duration

	// The databases.
	// These are never modified after construction.
	rangeDbs map[string]*database
	geoIpv4  *database
	geoIpv6  *database

	// chkMu guards checkpoints and serializes checkpoint writes.
	chkMu       sync.Mutex
	checkpoints *AllCheckpoints

	// ctx is canceled when the instance is closed.
	ctx    context.Context
	cancel context.CancelFunc

	// aliveMu guards starting background goroutines against Close.
	aliveMu   sync.Mutex
	wg        sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once
}

// DataSource stores source information for IP data.
// It includes the URLs the data can be fetched from and the time to wait between updating the data from them.
type DataSource struct {
	// Urls are the URLs where the IP data is located.
	// Either Get or Urls must be provided; Get takes precedence over Urls.
	//
	// For range databases, the contents of all URLs are merged into a single database.
	// URLs that fail to download or do not contain a valid list are handled by Options.RetryPolicy.
	// If Options.RetryPolicy is not set, the failed URL is logged and skipped, and the database is updated from the
	// remaining URLs.
	//
	// For geoIP databases, the URLs are mirrors that are tried in order; the first one that yields a valid database is
	// used.
	Urls []*url.URL

	// Get is a function to get the IP data.
	// Either Get or Urls must be provided; Get takes precedence over Urls.
	// The returned reader is closed by the Ipdb instance.
	//
	// Get should return within a reasonable amount of time; Ipdb.Close waits for running calls to finish.
	Get func() (io.ReadCloser, error)

	// RefreshInterval is the interval between updating the data from the source.
	// Required and must be positive, unless Options.DisableDownload is true.
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
	// If nil, uses a default HTTP client based on http.DefaultTransport, with a timeout of DefaultHttpTimeout per
	// download and a timeout of DefaultHttpResponseHeaderTimeout for receiving response headers.
	HttpClient *http.Client

	// If true, disables downloading from sources and only uses cached database files.
	//
	// Important: You must still provide sources for the databases you want to use, regardless of whether download is
	// disabled.
	DisableDownload bool

	// If true, NewIpdb returns without waiting for databases to be loaded, and they are loaded in the background instead.
	//
	// This option is intended for speeding up startup during development, where waiting for databases to load or
	// download every time the program starts gets in the way. It is unsuitable for production, because:
	//   - Any methods on Ipdb that require databases return a NotInitializedError until the databases have loaded.
	//   - If loading fails, the error is only logged, and the Ipdb instance is closed. Loading is not retried, and
	//     the instance's methods return ErrIpdbClosed from then on. Without this option, NewIpdb would instead
	//     return the error, so the program can handle it.
	LoadDatabasesInBackground bool

	// The maximum size of a single database, in bytes.
	// Downloads and cached files larger than this are rejected with ErrDatabaseTooLarge.
	// For range databases with multiple URLs, the limit applies to the merged database.
	// If zero or negative, DefaultMaxDatabaseSize is used.
	MaxDatabaseSize int64

	// Decides what happens when a source of a database fails to download: retry it, skip it, or fail the update of the
	// whole database.
	// See RetryPolicy.
	// A download that completes but does not contain a valid database also counts as a failure.
	//
	// If nil, failed sources are logged and skipped without retrying, so a broken URL never stops a database
	// from being updated from its other URLs.
	// See SkipAllSources.
	RetryPolicy RetryPolicy

	// A mapping of database names to their underlying IP range sources.
	// Each source must yield a newline-separated list of IP addresses and/or CIDR ranges.
	// Empty lines and lines that start with "#" are ignored.
	//
	// Names must not be empty, must be at most DbNameMaxSize bytes long, and must not be DbNameGeoIpv4 or
	// DbNameGeoIpv6. The storage driver may impose additional restrictions.
	RangeSources map[string]*DataSource

	// The source for the IPv4 country database.
	// It must yield a MaxMindDB (.mmdb) file that stores the ISO 3166 country code in either a top-level "country_code"
	// field (as used by many third-party mirrors) or in "country.iso_code" (as used by MaxMind's GeoLite2 and GeoIP2
	// databases).
	// See: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/
	//
	// Resolving IPv4 country codes will be disabled if this field is nil.
	GeoIpv4Source *DataSource

	// The source for the IPv6 country database.
	// It must yield a MaxMindDB (.mmdb) file that stores the ISO 3166 country code in either a top-level "country_code"
	// field (as used by many third-party mirrors) or in "country.iso_code" (as used by MaxMind's GeoLite2 and GeoIP2
	// databases).
	// See: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data/
	//
	// Resolving IPv6 country codes will be disabled if this field is nil.
	GeoIpv6Source *DataSource
}

func newDefaultHttpClient() *http.Client {
	transport := http.DefaultTransport
	if t, ok := transport.(*http.Transport); ok {
		clone := t.Clone()
		clone.ResponseHeaderTimeout = DefaultHttpResponseHeaderTimeout
		transport = clone
	}

	return &http.Client{
		Timeout:   DefaultHttpTimeout,
		Transport: transport,
	}
}

// validateDataSource returns an error if the data source cannot be used.
func validateDataSource(src *DataSource, disableDl bool) error {
	if src == nil {
		return errors.New("data source is nil")
	}
	if src.Get == nil && len(src.Urls) == 0 {
		return ErrDataSourceNoSource
	}
	for i, srcUrl := range src.Urls {
		if srcUrl == nil {
			return fmt.Errorf("URL at index %d is nil", i)
		}
	}
	if !disableDl && src.RefreshInterval <= 0 {
		return fmt.Errorf("%w (got %s)", ErrInvalidRefreshInterval, src.RefreshInterval)
	}

	return nil
}

// newIpdb validates options and creates an Ipdb instance without loading anything or starting any goroutines.
func newIpdb(options Options) (*Ipdb, error) {
	if options.StorageDriver == nil {
		return nil, errors.New("ipdb: Options.StorageDriver is required")
	}

	httpClient := options.HttpClient
	if httpClient == nil {
		httpClient = newDefaultHttpClient()
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	maxDbSize := options.MaxDatabaseSize
	if maxDbSize <= 0 {
		maxDbSize = DefaultMaxDatabaseSize
	}

	retryPolicy := options.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = SkipAllSources
	}

	nameValidator, _ := options.StorageDriver.(DbNameValidator)

	rangeDbs := make(map[string]*database, len(options.RangeSources))
	for name, src := range options.RangeSources {
		switch {
		case name == "":
			return nil, fmt.Errorf("ipdb: %w: range database name must not be empty", ErrInvalidDbName)
		case len(name) > DbNameMaxSize:
			return nil, fmt.Errorf(`ipdb: invalid range database name %q: %w`, name, ErrDbNameTooLong)
		case name == DbNameGeoIpv4 || name == DbNameGeoIpv6:
			return nil, fmt.Errorf(`ipdb: %w: range database name %q is reserved`, ErrInvalidDbName, name)
		}
		if nameValidator != nil {
			if err := nameValidator.ValidateDatabaseName(name); err != nil {
				return nil, fmt.Errorf(`ipdb: range database name %q was rejected by the storage driver: %w`, name, err)
			}
		}
		if err := validateDataSource(src, options.DisableDownload); err != nil {
			return nil, fmt.Errorf(`ipdb: invalid data source for range database %q: %w`, name, err)
		}

		rangeDbs[name] = newDatabase(name, dbKindRange, src)
	}

	var geoIpv4 *database
	if options.GeoIpv4Source != nil {
		if err := validateDataSource(options.GeoIpv4Source, options.DisableDownload); err != nil {
			return nil, fmt.Errorf("ipdb: invalid data source for geo IPv4 database: %w", err)
		}
		geoIpv4 = newDatabase(DbNameGeoIpv4, dbKindGeo, options.GeoIpv4Source)
	}

	var geoIpv6 *database
	if options.GeoIpv6Source != nil {
		if err := validateDataSource(options.GeoIpv6Source, options.DisableDownload); err != nil {
			return nil, fmt.Errorf("ipdb: invalid data source for geo IPv6 database: %w", err)
		}
		geoIpv6 = newDatabase(DbNameGeoIpv6, dbKindGeo, options.GeoIpv6Source)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Ipdb{
		storage:    options.StorageDriver,
		disableDl:  options.DisableDownload,
		httpClient: httpClient,
		logger:     logger,
		maxDbSize:  maxDbSize,

		retryPolicy: retryPolicy,

		updateRetryDelay: defaultUpdateRetryDelay,

		rangeDbs: rangeDbs,
		geoIpv4:  geoIpv4,
		geoIpv6:  geoIpv6,

		checkpoints: &AllCheckpoints{
			Checkpoints: make(map[string]Checkpoint),
		},

		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// NewIpdb creates a new Ipdb instance.
// Blocks until the databases are loaded, unless Options.LoadDatabasesInBackground is true.
// If loading fails, returns the error, or with Options.LoadDatabasesInBackground, closes the returned instance.
// There should only be one instance of Ipdb per storage driver or storage location, and ideally only one per process.
// If error is nil, the returned Ipdb instance will never be nil.
func NewIpdb(options Options) (*Ipdb, error) {
	s, err := newIpdb(options)
	if err != nil {
		return nil, err
	}

	s.logger.Info("initializing",
		"service", "ipdb.Ipdb",
	)

	checkpoints, err := s.storage.ReadCheckpoints()
	if err == nil {
		if checkpoints != nil && checkpoints.Checkpoints != nil {
			s.checkpoints = checkpoints
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = s.Close()
		return nil, fmt.Errorf("failed to load checkpoints during initialization: %w", err)
	}

	if options.LoadDatabasesInBackground {
		s.logger.Debug("loading databases in the background, as requested by Ipdb options",
			"service", "ipdb.Ipdb",
		)
		s.goRun(s.initInBackground)

		return s, nil
	}

	if err := s.init(); err != nil {
		_ = s.Close()
		return nil, err
	}

	return s, nil
}

// goRun runs the function in a goroutine that Close will wait for.
// Does nothing if the instance is closed.
func (s *Ipdb) goRun(fn func()) {
	s.aliveMu.Lock()
	defer s.aliveMu.Unlock()

	if s.closed.Load() {
		return
	}

	s.wg.Go(fn)
}

// sleep waits for the specified duration.
// Returns false if the instance was closed while waiting.
func (s *Ipdb) sleep(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// allDbs returns all enabled databases.
func (s *Ipdb) allDbs() []*database {
	dbs := make([]*database, 0, len(s.rangeDbs)+2)
	for _, db := range s.rangeDbs {
		dbs = append(dbs, db)
	}
	if s.geoIpv4 != nil {
		dbs = append(dbs, s.geoIpv4)
	}
	if s.geoIpv6 != nil {
		dbs = append(dbs, s.geoIpv6)
	}
	return dbs
}

// init loads all databases that are not loaded yet, saves checkpoints and starts the updaters.
func (s *Ipdb) init() error {
	dbs := s.allDbs()

	for _, db := range dbs {
		if s.closed.Load() {
			return ErrIpdbClosed
		}
		if db.isLoaded() {
			continue
		}
		if err := s.initDb(db); err != nil {
			return err
		}
	}

	// Save checkpoints.
	// This is necessary because checkpoints may have never been saved, or may be missing databases.
	s.chkMu.Lock()
	for _, db := range dbs {
		if _, has := s.checkpoints.Checkpoints[db.name]; !has {
			s.checkpoints.Checkpoints[db.name] = Checkpoint{}
		}
	}
	lastUpdates := make(map[string]time.Time, len(dbs))
	for _, db := range dbs {
		lastUpdates[db.name] = time.Unix(s.checkpoints.Checkpoints[db.name].LastUpdatedUnix, 0)
	}
	err := s.storage.WriteCheckpoints(s.checkpoints)
	s.chkMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to save checkpoints after initial load: %w", err)
	}

	if !s.disableDl {
		for _, db := range dbs {
			s.goRun(func() {
				s.runUpdater(db, lastUpdates[db.name])
			})
		}
	}

	s.logger.Info("finished initializing Ipdb",
		"service", "ipdb.Ipdb",
	)

	return nil
}

// initInBackground runs init once, closing the instance if it fails.
// See Options.LoadDatabasesInBackground.
func (s *Ipdb) initInBackground() {
	err := s.init()
	if err == nil || s.closed.Load() {
		return
	}

	s.logger.Error("failed to initialize Ipdb in the background, closing it; the Ipdb instance will return ErrIpdbClosed from now on",
		"service", "ipdb.Ipdb",
		"error", err,
	)

	// Close waits for all goroutines started by goRun, including this one, so it must be called from another
	// goroutine to avoid a deadlock. It returns as soon as this goroutine has finished.
	go func() {
		_ = s.Close()
	}()
}

// initDb loads the database from cache, falling back to downloading it if there is no usable cached copy.
func (s *Ipdb) initDb(db *database) error {
	s.logger.Debug("reading database from cache",
		"service", "ipdb.Ipdb",
		"database_name", db.name,
	)

	reader, err := s.storage.ReadDatabase(db.name)
	switch {
	case err == nil && reader != nil:
		var data []byte
		data, err = s.readAllLimited(reader)
		_ = reader.Close()
		if err == nil {
			err = s.load(db, data)
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrIpdbClosed) {
			return err
		}
		if s.disableDl {
			return fmt.Errorf(`failed to load cached database with name %q during initialization, and downloading is disabled: %w`, db.name, err)
		}

		s.logger.Warn("cached database could not be loaded, downloading a fresh copy",
			"service", "ipdb.Ipdb",
			"database_name", db.name,
			"error", err,
		)
	case err == nil || errors.Is(err, fs.ErrNotExist):
		// No cached database.
		if s.disableDl {
			return fmt.Errorf(`cannot download database with name %q during initialization: %w`, db.name, ErrNoCacheAndNoDownload)
		}
	default:
		return fmt.Errorf(`failed to read database with name %q during initialization: %w`, db.name, err)
	}

	if err = s.refresh(db); err != nil {
		return fmt.Errorf(`failed to download database with name %q during initialization: %w`, db.name, err)
	}

	return nil
}

// retryDelay returns the time to wait before retrying an update after the specified number of consecutive failures.
func retryDelay(base time.Duration, failures int, interval time.Duration) time.Duration {
	delay := base << min(max(failures-1, 0), 16)
	if delay <= 0 || delay > interval {
		return interval
	}
	return delay
}

// runUpdater periodically updates the database until the instance is closed.
func (s *Ipdb) runUpdater(db *database, lastUpdate time.Time) {
	interval := db.src.RefreshInterval

	s.logger.Debug("running updater for database",
		"service", "ipdb.Ipdb",
		"database_name", db.name,
	)

	failures := 0
	wait := time.Until(lastUpdate.Add(interval))

	for {
		if !s.sleep(max(wait, 0)) {
			return
		}

		err := s.refresh(db)
		if s.closed.Load() {
			return
		}
		if err == nil {
			// Databases are big, and the update leaves the downloaded data and the replaced database behind as garbage.
			// Run the GC now to free that memory right away, instead of letting it build up until the next GC cycle.
			runtime.GC()

			failures = 0
			// The interval is intentionally measured from the end of the update rather than on a fixed schedule:
			// some sources (such as Tor exit node lists) require a minimum time between requests, which a fixed
			// schedule could violate after a slow update. The schedule drifting by the update's duration is acceptable.
			wait = interval
			continue
		}

		failures++
		wait = retryDelay(s.updateRetryDelay, failures, interval)

		s.logger.Error("failed to do scheduled update of database",
			"service", "ipdb.Ipdb",
			"database_name", db.name,
			"error", err,
			"consecutive_failures", failures,
			"retry_in", wait.String(),
		)
	}
}

// recordUpdate saves a checkpoint for the database with the specified name.
func (s *Ipdb) recordUpdate(name string, ts time.Time) {
	s.chkMu.Lock()
	defer s.chkMu.Unlock()

	chkPnt := s.checkpoints.Checkpoints[name]
	chkPnt.LastUpdatedUnix = ts.Unix()
	s.checkpoints.Checkpoints[name] = chkPnt

	if err := s.storage.WriteCheckpoints(s.checkpoints); err != nil {
		s.logger.Error("failed to save checkpoints after database update",
			"service", "ipdb.Ipdb",
			"database_name", name,
			"error", err,
		)
	}
}

// redactUrl returns the URL as a string without credentials or query parameters, for use in logs and errors.
// Source URLs may contain secrets such as license keys.
func redactUrl(srcUrl *url.URL) string {
	redacted := *srcUrl
	redacted.User = nil
	redacted.Fragment = ""
	redacted.RawFragment = ""
	redacted.ForceQuery = false
	if redacted.RawQuery != "" {
		redacted.RawQuery = "REDACTED"
	}
	return redacted.String()
}

// readAllLimited reads the entire reader, returning ErrDatabaseTooLarge if it is larger than the max database size.
func (s *Ipdb) readAllLimited(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, s.maxDbSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > s.maxDbSize {
		return nil, fmt.Errorf("%w (limit is %d bytes)", ErrDatabaseTooLarge, s.maxDbSize)
	}
	return data, nil
}

// fetchUrl downloads the entire body of the URL.
// An incomplete download results in an error; partial data is never returned.
func (s *Ipdb) fetchUrl(srcUrl *url.URL) ([]byte, error) {
	redacted := redactUrl(srcUrl)

	s.logger.Debug("starting download of database",
		"service", "ipdb.Ipdb",
		"source_url", redacted,
	)

	req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, srcUrl.String(), nil)
	if err != nil {
		return nil, fmt.Errorf(`failed to create request (source URL %q)`, redacted)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		// url.Error includes the full URL in its message, so unwrap it.
		if urlErr, ok := errors.AsType[*url.Error](err); ok {
			err = urlErr.Err
		}
		return nil, fmt.Errorf(`failed to download database (source URL %q): %w`, redacted, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		// Try to read first N bytes of body to get a better error message.
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, bodyPreviewBytes))

		return nil, fmt.Errorf(`failed to download database (source URL %q): %w`, redacted, &HttpStatusError{
			StatusCode:  resp.StatusCode,
			BodyPreview: string(bodyBytes),
		})
	}

	data, err := s.readAllLimited(resp.Body)
	if err != nil {
		return nil, fmt.Errorf(`error while reading database (source URL %q): %w`, redacted, err)
	}

	s.logger.Debug("finished download of database",
		"service", "ipdb.Ipdb",
		"source_url", redacted,
		"bytes", len(data),
	)

	return data, nil
}

// downloadAndLoad downloads the database from its source and loads it into memory.
// Returns the raw data that was loaded.
// The in-memory database is only replaced if the downloaded data is complete and valid.
func (s *Ipdb) downloadAndLoad(db *database) ([]byte, error) {
	src := db.src

	if src.Get != nil {
		// The Get function is the only source, so skipping it fails the update.
		var data []byte
		var ranger cidranger.Ranger
		var mmdb *maxminddb.Reader
		action, err := s.fetchSource(db, nil, func() error {
			var err error
			if data, err = s.getFromFunc(db); err != nil {
				return err
			}
			ranger, mmdb, err = s.parse(db, data)
			return err
		})
		if err != nil {
			if action == RetryActionFail && !errors.Is(err, ErrIpdbClosed) {
				return nil, fmt.Errorf("%w: %w", ErrFailedByRetryPolicy, err)
			}
			return nil, err
		}

		if err = s.install(db, ranger, mmdb); err != nil {
			return nil, err
		}
		return data, nil
	}

	if len(src.Urls) == 0 {
		return nil, ErrDataSourceNoSource
	}

	var failures []error
	// handleFailure records a source failure, returning a non-nil error if the update must stop.
	handleFailure := func(action RetryAction, err error) error {
		if errors.Is(err, ErrIpdbClosed) && s.closed.Load() {
			return ErrIpdbClosed
		}
		failures = append(failures, err)
		if action == RetryActionFail {
			return errors.Join(append(failures, ErrFailedByRetryPolicy)...)
		}
		return nil
	}

	if db.kind == dbKindGeo {
		// URLs are mirrors; use the first one that yields a valid database.
		for _, srcUrl := range src.Urls {
			var data []byte
			var mmdb *maxminddb.Reader
			action, err := s.fetchSource(db, srcUrl, func() error {
				var err error
				if data, err = s.fetchUrl(srcUrl); err != nil {
					return err
				}
				if _, mmdb, err = s.parse(db, data); err != nil {
					return fmt.Errorf(`downloaded database was invalid (source URL %q): %w`, redactUrl(srcUrl), err)
				}
				return nil
			})
			if err != nil {
				if stopErr := handleFailure(action, err); stopErr != nil {
					return nil, stopErr
				}
				continue
			}

			if err = s.install(db, nil, mmdb); err != nil {
				return nil, err
			}
			return data, nil
		}

		return nil, errors.Join(append(failures, ErrAllUrlsFailed)...)
	}

	// Merge all URLs that yield a valid list.
	// Each list is validated on its own, so one broken URL cannot fail the whole database.
	var merged bytes.Buffer
	var nets []net.IPNet
	succeeded := 0
	for _, srcUrl := range src.Urls {
		var data []byte
		var urlNets []net.IPNet
		action, err := s.fetchSource(db, srcUrl, func() error {
			var err error
			if data, err = s.fetchUrl(srcUrl); err != nil {
				return err
			}
			if urlNets, err = s.parseRangeList(db.name, bytes.NewReader(data)); err != nil {
				return fmt.Errorf(`downloaded list was invalid (source URL %q): %w`, redactUrl(srcUrl), err)
			}
			return nil
		})
		if err != nil {
			if stopErr := handleFailure(action, err); stopErr != nil {
				return nil, stopErr
			}
			continue
		}

		if int64(merged.Len())+int64(len(data))+1 > s.maxDbSize {
			return nil, fmt.Errorf("%w (limit is %d bytes)", ErrDatabaseTooLarge, s.maxDbSize)
		}

		merged.Write(data)
		// Write a newline to ensure the next URL body starts on a new line.
		merged.WriteByte('\n')
		nets = append(nets, urlNets...)
		succeeded++
	}

	if succeeded == 0 {
		return nil, errors.Join(append(failures, ErrAllUrlsFailed)...)
	}

	ranger, err := buildRanger(db.name, nets)
	if err != nil {
		return nil, err
	}
	if err = s.install(db, ranger, nil); err != nil {
		return nil, err
	}

	return merged.Bytes(), nil
}

// getFromFunc reads the database from the source's Get function.
func (s *Ipdb) getFromFunc(db *database) ([]byte, error) {
	s.logger.Debug("starting download of database with source Get function",
		"service", "ipdb.Ipdb",
		"database_name", db.name,
	)

	reader, err := db.src.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get database (source Get function): %w", err)
	}
	if reader == nil {
		return nil, errors.New("failed to get database (source Get function): returned reader was nil")
	}

	data, err := s.readAllLimited(reader)
	_ = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read database (source Get function): %w", err)
	}

	return data, nil
}

// fetchSource runs fetch, consulting the retry policy after each failure, until it succeeds or the policy gives up.
// srcUrl is the URL being fetched, or nil for a Get function.
// If the policy gives up, returns the last error and the action the policy chose (RetryActionSkip or RetryActionFail).
// If the instance is closed, returns ErrIpdbClosed with RetryActionFail.
func (s *Ipdb) fetchSource(db *database, srcUrl *url.URL, fetch func() error) (RetryAction, error) {
	source := "Get function"
	if srcUrl != nil {
		source = redactUrl(srcUrl)
	}

	for attempt := 1; ; attempt++ {
		if s.closed.Load() {
			return RetryActionFail, ErrIpdbClosed
		}

		err := fetch()
		if err == nil {
			return RetryActionSkip, nil
		}
		if s.closed.Load() {
			return RetryActionFail, ErrIpdbClosed
		}

		decision := s.retryPolicy(SourceFailure{
			DbName:  db.name,
			Url:     srcUrl,
			Attempt: attempt,
			Err:     err,
		})

		switch decision.Action {
		case RetryActionRetry:
			delay := max(decision.Delay, 0)
			s.logger.Warn("failed to download database source, retrying",
				"service", "ipdb.Ipdb",
				"database_name", db.name,
				"source", source,
				"attempt", attempt,
				"retry_in", delay.String(),
				"error", err,
			)
			if !s.sleep(delay) {
				return RetryActionFail, ErrIpdbClosed
			}
		case RetryActionFail:
			s.logger.Error("failed to download database source, failing the database update",
				"service", "ipdb.Ipdb",
				"database_name", db.name,
				"source", source,
				"attempt", attempt,
				"error", err,
			)
			return RetryActionFail, err
		default:
			s.logger.Error("failed to download database source, skipping it",
				"service", "ipdb.Ipdb",
				"database_name", db.name,
				"source", source,
				"attempt", attempt,
				"error", err,
			)
			return RetryActionSkip, err
		}
	}
}

// refresh downloads the database, loads it into memory, writes it to the cache and saves a checkpoint for it.
//
// If the download fails or yields invalid data, the in-memory database and the cache are left untouched.
// If writing to the cache fails, the in-memory database is still updated, but an error is returned and no checkpoint is saved.
func (s *Ipdb) refresh(db *database) error {
	if s.closed.Load() {
		return ErrIpdbClosed
	}

	db.updateMu.Lock()
	defer db.updateMu.Unlock()

	s.logger.Debug("downloading and loading database",
		"service", "ipdb.Ipdb",
		"database_name", db.name,
	)

	data, err := s.downloadAndLoad(db)
	if err != nil {
		if s.closed.Load() {
			return ErrIpdbClosed
		}
		return err
	}

	if s.closed.Load() {
		return ErrIpdbClosed
	}

	err = s.storage.WriteDatabase(db.name, io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		return fmt.Errorf(`database with name %q was loaded into memory, but writing it to the cache failed: %w`, db.name, err)
	}

	s.recordUpdate(db.name, time.Now())

	return nil
}

// parseRangeLine parses a CIDR range or bare IP address.
// Any bare IP addresses will be treated as /32 for IPv4 and /128 for IPv6.
// IPv4-mapped IPv6 addresses and ranges are converted to IPv4.
func parseRangeLine(line string) (net.IPNet, error) {
	var prefix netip.Prefix

	if strings.Contains(line, "/") {
		// CIDR range.
		var err error
		prefix, err = netip.ParsePrefix(line)
		if err != nil {
			return net.IPNet{}, fmt.Errorf(`failed to parse CIDR range %q: %w`, line, err)
		}
	} else {
		// No CIDR range, treat as bare IP address.
		addr, err := netip.ParseAddr(line)
		if err != nil {
			return net.IPNet{}, fmt.Errorf(`failed to parse bare IP address %q: %w`, line, err)
		}
		addr = addr.WithZone("")
		prefix = netip.PrefixFrom(addr, addr.BitLen())
	}

	addr := prefix.Addr()
	bits := prefix.Bits()

	// The ranger treats IPv4-mapped IPv6 addresses as IPv4, so they must be given an IPv4 mask.
	const v4InV6PrefixBits = 96
	if addr.Is4In6() && bits >= v4InV6PrefixBits {
		addr = addr.Unmap()
		bits -= v4InV6PrefixBits
	}

	prefix = netip.PrefixFrom(addr, bits).Masked()

	return net.IPNet{
		IP:   prefix.Addr().AsSlice(),
		Mask: net.CIDRMask(bits, addr.BitLen()),
	}, nil
}

// parseRanges reads all IP addresses and CIDR ranges from the reader into a new ranger.
// The entire input is always read; an error is returned if the input cannot be read completely,
// contains no ranges, or contains more malformed lines than valid ones.
func (s *Ipdb) parseRanges(name string, reader io.Reader) (cidranger.Ranger, error) {
	nets, err := s.parseRangeList(name, reader)
	if err != nil {
		return nil, err
	}
	return buildRanger(name, nets)
}

// buildRanger creates a ranger containing the specified ranges.
func buildRanger(name string, nets []net.IPNet) (ranger cidranger.Ranger, err error) {
	// The ranger is fed with remote data, and panics on some unusual inputs.
	// A bad list must not be able to take down the process.
	defer func() {
		if r := recover(); r != nil {
			ranger = nil
			err = fmt.Errorf(`panic while loading ranges for database %q: %v`, name, r)
		}
	}()

	ranger = cidranger.NewPCTrieRanger()
	for _, ipNet := range nets {
		if insertErr := ranger.Insert(cidranger.NewBasicRangerEntry(ipNet)); insertErr != nil {
			return nil, fmt.Errorf(`failed to insert IP range %q into database %q: %w`, ipNet.String(), name, insertErr)
		}
	}

	return ranger, nil
}

// parseRangeList reads all IP addresses and CIDR ranges from the reader.
// The entire input is always read; an error is returned if the input cannot be read completely,
// contains no ranges, or contains more malformed lines than valid ones.
func (s *Ipdb) parseRangeList(name string, reader io.Reader) ([]net.IPNet, error) {
	var nets []net.IPNet
	failures := make([]error, 0, maxReportedParseFailures)
	failureCount := 0
	goodLines := 0

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()

		// Strip comments and surrounding whitespace, and skip empty lines.
		if idx := strings.IndexByte(line, '#'); idx != -1 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		ipNet, parseErr := parseRangeLine(line)
		if parseErr != nil {
			failureCount++
			if len(failures) < maxReportedParseFailures {
				failures = append(failures, parseErr)
				s.logger.Error("failed to parse line in range database",
					"service", "ipdb.Ipdb",
					"database_name", name,
					"line", line,
					"error", parseErr,
				)
			}
			continue
		}

		nets = append(nets, ipNet)
		goodLines++
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, fmt.Errorf(`failed to read ranges for database %q: %w`, name, scanErr)
	}

	if failureCount > goodLines {
		return nil, fmt.Errorf(`encountered %d parse failures while loading ranges for database %q, but only %d lines were successfully parsed. file is probably malformed; expected newline-separated list of CIDR ranges or bare IP addresses. this error wraps the first %d parse errors. error(s): %w`, failureCount, name, goodLines, len(failures), errors.Join(failures...))
	}
	if goodLines == 0 {
		return nil, fmt.Errorf(`failed to load ranges for database %q: %w`, name, ErrNoRanges)
	}

	if failureCount > 0 {
		s.logger.Warn("some lines in range database could not be parsed and were skipped",
			"service", "ipdb.Ipdb",
			"database_name", name,
			"skipped_lines", failureCount,
			"loaded_lines", goodLines,
		)
	}

	return nets, nil
}

// load validates the raw database data and, if valid, replaces the in-memory database with it.
// For geoIP databases, the data must not be modified after calling this function.
func (s *Ipdb) load(db *database, data []byte) error {
	ranger, mmdb, err := s.parse(db, data)
	if err != nil {
		return err
	}
	return s.install(db, ranger, mmdb)
}

// parse validates the raw database data and builds an in-memory database from it, without installing it.
// Returns a ranger for range databases, or a reader for geoIP databases.
// For geoIP databases, the data must not be modified after calling this function.
func (s *Ipdb) parse(db *database, data []byte) (cidranger.Ranger, *maxminddb.Reader, error) {
	if db.kind == dbKindGeo {
		mmdb, err := maxminddb.OpenBytes(data)
		if err != nil {
			return nil, nil, fmt.Errorf(`failed to parse geoIP database %q: %w`, db.name, err)
		}
		return nil, mmdb, nil
	}

	ranger, err := s.parseRanges(db.name, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	return ranger, nil, nil
}

// install replaces the in-memory database with the specified ranger or geoIP reader.
// The previous database is not closed; see loadedDb.mmdb for why that is safe.
func (s *Ipdb) install(db *database, ranger cidranger.Ranger, mmdb *maxminddb.Reader) error {
	if s.closed.Load() {
		return ErrIpdbClosed
	}

	snapshot := &loadedDb{ranger: ranger, mmdb: mmdb}
	db.loaded.Store(snapshot)

	// Close may have run between the check above and the store, in which case it may already have cleared the
	// database, and this store would have put it back. Close sets the closed flag before clearing databases,
	// so checking the flag again after storing guarantees that one of the two clears it.
	// CompareAndSwap avoids clearing a newer snapshot stored by someone else.
	if s.closed.Load() {
		db.loaded.CompareAndSwap(snapshot, nil)
		return ErrIpdbClosed
	}

	return nil
}

// DownloadAndLoadRangeDatabase downloads the database with the specified name, loads it into memory and writes it to the cache.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
//
// If the download fails or yields invalid data, the existing database is left untouched.
// If the database does not exist, returns a NoSuchDatabaseError.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
func (s *Ipdb) DownloadAndLoadRangeDatabase(name string) error {
	db, has := s.rangeDbs[name]
	if !has {
		return NewNoSuchDatabaseError(name)
	}

	return s.refresh(db)
}

// DownloadAndLoadGeoIpv4Mmdb downloads the GeoIPv4 MMDB, loads it into memory and writes it to the cache.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
//
// If the download fails or yields invalid data, the existing database is left untouched.
// If Options.GeoIpv4Source was nil, returns an error wrapping ErrDataSourceNoSource.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
func (s *Ipdb) DownloadAndLoadGeoIpv4Mmdb() error {
	if s.geoIpv4 == nil {
		return fmt.Errorf("cannot download GeoIPv4 MMDB because no source was configured for it: %w", ErrDataSourceNoSource)
	}

	return s.refresh(s.geoIpv4)
}

// DownloadAndLoadGeoIpv6Mmdb downloads the GeoIPv6 MMDB, loads it into memory and writes it to the cache.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
//
// If the download fails or yields invalid data, the existing database is left untouched.
// If Options.GeoIpv6Source was nil, returns an error wrapping ErrDataSourceNoSource.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
func (s *Ipdb) DownloadAndLoadGeoIpv6Mmdb() error {
	if s.geoIpv6 == nil {
		return fmt.Errorf("cannot download GeoIPv6 MMDB because no source was configured for it: %w", ErrDataSourceNoSource)
	}

	return s.refresh(s.geoIpv6)
}

// Close stops all background updates and frees all databases.
// Running downloads are canceled, and Close waits for background work to stop before returning.
// The Ipdb instance is no longer usable after closure; its methods return ErrIpdbClosed.
//
// It is safe to call Close multiple times and from multiple goroutines.
// Subsequent calls do nothing.
// The returned error is currently always nil.
func (s *Ipdb) Close() error {
	s.closeOnce.Do(func() {
		s.aliveMu.Lock()
		s.closed.Store(true)
		s.aliveMu.Unlock()

		s.cancel()
		s.wg.Wait()

		// Clear the databases so they can be freed by the GC once in-progress lookups are done.
		// GeoIP readers are intentionally not closed; see loadedDb.mmdb for why that is safe.
		for _, db := range s.allDbs() {
			db.loaded.Store(nil)
		}
	})

	return nil
}

// IsIpInRangeDb returns whether an IP was found in the specified IP range database.
// IPv4-mapped IPv6 addresses are treated as IPv4 addresses.
// If the database does not exist, returns a NoSuchDatabaseError.
// If the database has not been initialized, returns a NotInitializedError.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
func (s *Ipdb) IsIpInRangeDb(dbName string, ip netip.Addr) (bool, error) {
	if s.closed.Load() {
		return false, ErrIpdbClosed
	}

	db, has := s.rangeDbs[dbName]
	if !has {
		return false, NewNoSuchDatabaseError(dbName)
	}

	if !ip.IsValid() {
		return false, fmt.Errorf(`cannot check if invalid IP is in range database %q`, dbName)
	}

	loaded := db.loaded.Load()
	if loaded == nil {
		if s.closed.Load() {
			return false, ErrIpdbClosed
		}
		return false, NewNotInitializedError(dbName, fmt.Sprintf(`cannot check if IP %q is in range database %q`, ip.String(), dbName))
	}

	has, err := loaded.ranger.Contains(ip.Unmap().WithZone("").AsSlice())
	if err != nil {
		return false, fmt.Errorf(`failed to check if IP %q is in range database %q: %w`, ip.String(), dbName, err)
	}

	return has, nil
}

// ResolveIpIsoCountry resolves the ISO 3166 country code for the specified IP address.
// The country code is uppercase.
// IPv4-mapped IPv6 addresses are treated as IPv4 addresses.
// If no country can be found, defaults to defaultCc.
// If the required database is not initialized, returns a NotInitializedError.
// If the Ipdb instance has been closed, returns ErrIpdbClosed.
//
// To avoid returning an invalid ISO 3166 country code, the defaultCc parameter should ideally be a valid ISO 3166 country code.
// A good placeholder value is "AQ", which is the ISO 3166 country code for Antarctica.
func (s *Ipdb) ResolveIpIsoCountry(ip netip.Addr, defaultCc string) (string, error) {
	if s.closed.Load() {
		return "", ErrIpdbClosed
	}

	if !ip.IsValid() {
		return "", errors.New("cannot get country code for invalid IP")
	}

	ip = ip.Unmap().WithZone("")

	db := s.geoIpv4
	dbName := DbNameGeoIpv4
	if ip.Is6() {
		db = s.geoIpv6
		dbName = DbNameGeoIpv6
	}

	if db == nil {
		return "", NewNotInitializedError(dbName, fmt.Sprintf(`cannot get country code for IP %q because no source was configured for the database`, ip.String()))
	}

	loaded := db.loaded.Load()
	if loaded == nil {
		if s.closed.Load() {
			return "", ErrIpdbClosed
		}
		return "", NewNotInitializedError(dbName, fmt.Sprintf(`cannot get country code for IP %q`, ip.String()))
	}

	result := loaded.mmdb.Lookup(ip)
	if err := result.Err(); err != nil {
		return "", fmt.Errorf(`failed to lookup IP %q in geoIP database: %w`, ip.String(), err)
	}
	if !result.Found() {
		return defaultCc, nil
	}

	// Many third-party mirrors store the country code in a top-level "country_code" field,
	// while MaxMind's own databases store it in "country.iso_code".
	var cc string
	if err := result.DecodePath(&cc, "country_code"); err != nil {
		return "", fmt.Errorf(`failed to decode country code for IP %q from geoIP database: %w`, ip.String(), err)
	}
	if cc == "" {
		if err := result.DecodePath(&cc, "country", "iso_code"); err != nil {
			return "", fmt.Errorf(`failed to decode country code for IP %q from geoIP database: %w`, ip.String(), err)
		}
	}

	if cc == "" {
		return defaultCc, nil
	}

	return cc, nil
}
