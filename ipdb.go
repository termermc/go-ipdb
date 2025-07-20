package ipdb_geoip_tools

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
	"os"
	"syscall"
	"time"
)

const listFetchTimeout = 10 * time.Second

// IpdbType is a type of IP database.
type IpdbType string

func (t IpdbType) String() string {
	return string(t)
}

const (
	// IpdbTypeTorExit is the type for the Tor exit IPs database.
	IpdbTypeTorExit = "tor exit IPs"

	// IpdbTypeDatacenter is the type for the datacenter IPs database.
	IpdbTypeDatacenter = "datacenter IPs"

	// IpdbTypeGeoIpv4 is the type for the IPv4 to country code database.
	IpdbTypeGeoIpv4 = "IPv4 geolocation"

	// IpdbTypeGeoIpv6 is the type for the IPv6 to country code database.
	IpdbTypeGeoIpv6 = "IPv6 geolocation"
)

// ErrInvalidIpdbType is returned when an invalid IpdbType value is specified.
var ErrInvalidIpdbType = errors.New("invalid IpdbType value")

// NotInitializedError is returned when a function is run that required an IP database to be initialized, but it was not initialized.
// Includes the IP database type that was required but not initialized.
type NotInitializedError struct {
	// The type of database that was not initialized.
	Type IpdbType

	// The message.
	Msg string
}

func (err *NotInitializedError) Error() string {
	return fmt.Sprintf("ipdb %s not initialized: %s", err.Type, err.Msg)
}

// NewNotInitializedError creates a new NewNotInitializedError instance with the specified database type and message.
func NewNotInitializedError(ipdbType IpdbType, msg string) *NotInitializedError {
	return &NotInitializedError{
		Type: ipdbType,
		Msg:  msg,
	}
}

type ipdbUpdate struct {
	Ts   time.Time
	Type IpdbType
}

// Ipdb stores and updates IP databases.
//
// Includes functionality to:
//   - Resolve ISO 3166 country codes for an IP address
//   - Check if an IP address is a Tor exit node
//   - Check if an IP address is used by a datacenter (as opposed to a residential IP)
//
// Databases are cached on disk and updated periodically from data sources.
// Note that databases are stored in-memory.
//
// Create an instance with NewIpdb; do not create an empty Ipdb struct and attempt to use it.
//
// There should only be one instance of Ipdb per storage driver or storage location, and ideally only one per process.
type Ipdb struct {
	storage    StorageDriver
	disableDl  bool
	httpClient *http.Client
	logger     IpdbLogger
	updates    chan ipdbUpdate

	torExitSrc    *DataSource
	datacenterSrc *DataSource
	geoIpv4Src    *DataSource
	geoIpv6Src    *DataSource

	torExitIps            map[netip.Addr]struct{}
	torExitIpsMutex       *xsync.RBMutex
	datacenterRanger      cidranger.Ranger
	datacenterRangerMutex *xsync.RBMutex
	geoIpv4Mmdb           *maxminddb.Reader
	geoIpv4MmdbMutex      *xsync.RBMutex
	geoIpv6Mmdb           *maxminddb.Reader
	geoIpv6MmdbMutex      *xsync.RBMutex

	hasTorExt     bool
	hasDatacenter bool
	hasGeoIpv4    bool
	hasGeoIpv6    bool

	updaterIsRunning bool
}

// DataSource stores source information for IP data.
// It includes the URL the data can be fetched from and the time to wait between updating the data from the URL.
type DataSource struct {
	// Url is the URL where the IP data is located.
	// Either Get or Url must be provided; Get takes precedence over Url.
	Url *url.URL

	// Get is a function to get the IP data.
	// Either Get or Url must be provided; Get takes precedence over Url.
	Get func() (io.ReadCloser, error)

	// RefreshInterval is the interval between updating the data from the source.
	RefreshInterval time.Duration
}

// IpdbLogger is a function called by Ipdb to log messages.
// Note that the "err" field passed to it may be nil.
// If isErrorLevel is true, the message should be logged as an error message.
type IpdbLogger func(msg string, err error, isErrorLevel bool)

// DefaultIpdbLogger is the default logger for Ipdb.
// It logs messages to stdout and stderr.
var DefaultIpdbLogger IpdbLogger = func(msg string, err error, isErrorLevel bool) {
	var f *os.File
	if isErrorLevel {
		f = os.Stderr
	} else {
		f = os.Stdout
	}

	if err == nil {
		_, _ = fmt.Fprintf(f, "[ipdb] %s", msg)
	} else {
		_, _ = fmt.Fprintf(f, "[ipdb] %s (error = %v)", msg, err)
	}
}

// ErrNoCacheAndNoDownload is returned when there is no cached database, and downloading is disabled so there is no way to get the database.
var ErrNoCacheAndNoDownload = errors.New("no cached copy of database existed, and downloading is disabled")

// IpdbOptions are options for creating an Ipdb instance.
// Any omitted DataSource fields will be disabled and unavailable, even if cached files for them exist.
type IpdbOptions struct {
	// The storage driver used to store cached databases and checkpoint information.
	// Unless you have a custom driver you want to use, you should most likely use FsStorageDriver.
	// Required.
	StorageDriver StorageDriver

	// If true, disables downloading from sources and only uses cached database files.
	//
	// Important: You must still provide sources for the databases you want to use, regardless of whether download is disabled.
	DisableDownload bool

	// If true, the Ipdb instance will be created without waiting for databases to be loaded.
	// The databases will be loaded asynchronously in the background.
	//
	// Important: Any methods on Ipdb that require databases to be initialized will fail until the databases have loaded.
	// If this is true, you should also provide a function for the OnError field to handle errors that might occur while loading the databases in the background.
	LoadDatabasesInBackground bool

	// Overrides the default HTTP client if not nil.
	// If nil, uses a default HTTP client with a 10-second timeout.
	HttpClient *http.Client

	// By default, Ipdb prints log messages to stdout and stderr.
	// If Logger is specified, it will call this function instead.
	// Note that err may be nil.
	Logger IpdbLogger

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
// Blocks until the databases are loaded, unless IpdbOptions.LoadDatabasesInBackground is true.
// There should only be one instance of Ipdb per storage driver or storage location, and ideally only one per process.
func NewIpdb(options IpdbOptions) (*Ipdb, error) {
	var httpClient *http.Client
	if options.HttpClient == nil {
		httpClient = &http.Client{
			Timeout: listFetchTimeout,
		}
	} else {
		httpClient = options.HttpClient
	}

	var logger IpdbLogger
	if options.Logger == nil {
		logger = DefaultIpdbLogger
	} else {
		logger = options.Logger
	}

	s := &Ipdb{
		storage:    options.StorageDriver,
		disableDl:  options.DisableDownload,
		httpClient: httpClient,
		logger:     logger,
		updates:    make(chan ipdbUpdate, 8),

		torExitIpsMutex:       xsync.NewRBMutex(),
		datacenterRangerMutex: xsync.NewRBMutex(),
		geoIpv4MmdbMutex:      xsync.NewRBMutex(),
		geoIpv6MmdbMutex:      xsync.NewRBMutex(),

		updaterIsRunning: true,
	}

	s.logger("initializing Ipdb", nil, false)

	setup := func() error {
		// Read databases.
		if !s.updaterIsRunning {
			return nil
		}
		if s.torExitSrc == nil {
			s.logger("no data source for Tor exit nodes database; corresponding database will not be loaded", nil, false)
		} else {
			torExitReader, err := s.storage.ReadDatabase(IpdbTypeTorExit)
			if err != nil && !errors.Is(err, syscall.ENOENT) {
				return errors2.Wrap(err, "failed to read tor exit nodes database during initialization")
			}
			defer func() {
				_ = torExitReader.Close()
			}()
			if torExitReader == nil {
				// No cached database.
				if s.disableDl {
					return errors2.Wrap(ErrNoCacheAndNoDownload, "cannot download tor exit nodes database")
				}

				// Try downloading it.
				err = s.DownloadAndLoadTorExitNodeDatabase()
				if err != nil {
					return errors2.Wrap(err, "failed to download tor exit nodes database during initialization")
				}
			} else {
				err = s.loadTorExitNodeDatabaseFromReader(torExitReader)
				if err != nil {
					return errors2.Wrap(err, "failed to load tor exit nodes database during initialization")
				}
			}
		}

		if !s.updaterIsRunning {
			return nil
		}
		if s.datacenterSrc == nil {
			s.logger("no data source for datacenter ranges database; corresponding database will not be loaded", nil, false)
		} else {
			dcRangesReader, err := s.storage.ReadDatabase(IpdbTypeDatacenter)
			if err != nil && !errors.Is(err, syscall.ENOENT) {
				return errors2.Wrap(err, "failed to read datacenter ranges database during initialization")
			}
			defer func() {
				_ = dcRangesReader.Close()
			}()
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
			} else {
				err = s.loadDatacenterRangesFromReader(dcRangesReader)
				if err != nil {
					return errors2.Wrap(err, "failed to load datacenter ranges database during initialization")
				}
			}
		}

		if !s.updaterIsRunning {
			return nil
		}
		if s.geoIpv4Src == nil {
			s.logger("no data source for geo IPv4 database; corresponding database will not be loaded", nil, false)
		} else {
			geoIpv4Reader, err := s.storage.ReadDatabase(IpdbTypeGeoIpv4)
			if err != nil && !errors.Is(err, syscall.ENOENT) {
				return errors2.Wrap(err, "failed to read geo IPv4 database during initialization")
			}
			defer func() {
				_ = geoIpv4Reader.Close()
			}()
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
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv4Reader, IpdbTypeGeoIpv4)
				if err != nil {
					return errors2.Wrap(err, "failed to load geo IPv4 database during initialization")
				}
			}
		}

		if !s.updaterIsRunning {
			return nil
		}
		if s.geoIpv6Src == nil {
			s.logger("no data source for geo IPv6 database; corresponding database will not be loaded", nil, false)
		} else {
			geoIpv6Reader, err := s.storage.ReadDatabase(IpdbTypeGeoIpv6)
			if err != nil && !errors.Is(err, syscall.ENOENT) {
				return errors2.Wrap(err, "failed to read geo IPv6 database during initialization")
			}
			defer func() {
				_ = geoIpv6Reader.Close()
			}()
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
			} else {
				err = s.loadGeoIpMmdbFromReader(geoIpv6Reader, IpdbTypeGeoIpv6)
				if err != nil {
					return errors2.Wrap(err, "failed to load geo IPv6 database during initialization")
				}
			}
		}

		if !s.updaterIsRunning {
			return nil
		}
		checkpoints, err := s.storage.ReadCheckpoints()
		if err != nil {
			if errors.Is(err, syscall.ENOENT) {
				checkpoints = &AllCheckpointsDefault
			} else {
				return errors2.Wrap(err, "failed to load checkpoints during initialization")
			}
		}

		if !s.updaterIsRunning {
			return nil
		}

		// In the background, save checkpoint updates.
		go func() {
			for update := range s.updates {
				switch update.Type {
				case IpdbTypeTorExit:
					checkpoints.TorExitDb.LastUpdatedUnix = update.Ts.Unix()
					break
				case IpdbTypeDatacenter:
					checkpoints.DatacenterDb.LastUpdatedUnix = update.Ts.Unix()
					break
				case IpdbTypeGeoIpv4:
					checkpoints.GeoIpv4Db.LastUpdatedUnix = update.Ts.Unix()
					break
				case IpdbTypeGeoIpv6:
					checkpoints.GeoIpv6Db.LastUpdatedUnix = update.Ts.Unix()
					break
				default:
					s.logger(fmt.Sprintf("received checkpoint update for unknown database type \"%s\", this is a bug in the library", update.Type), nil, true)
				}

				err := s.storage.WriteCheckpoints(checkpoints)
				if err != nil {
					s.logger(fmt.Sprintf("failed to save checkpoints after receiving checkpoint update for %s database", update.Type), err, true)
				}
			}
		}()

		// Start updaters for enabled databases.
		if s.torExitSrc != nil {
			go s.runUpdater(
				time.Unix(checkpoints.TorExitDb.LastUpdatedUnix, 0),
				s.torExitSrc.RefreshInterval,
				IpdbTypeTorExit,
				s.DownloadAndLoadTorExitNodeDatabase,
			)
		}
		if s.datacenterSrc != nil {
			go s.runUpdater(
				time.Unix(checkpoints.DatacenterDb.LastUpdatedUnix, 0),
				s.datacenterSrc.RefreshInterval,
				IpdbTypeDatacenter,
				s.DownloadAndLoadDatacenterRangesDatabase,
			)
		}
		if s.geoIpv4Src != nil {
			go s.runUpdater(
				time.Unix(checkpoints.GeoIpv4Db.LastUpdatedUnix, 0),
				s.geoIpv4Src.RefreshInterval,
				IpdbTypeGeoIpv4,
				s.DownloadAndLoadGeoIpv4Mmdb,
			)
		}
		if s.geoIpv6Src != nil {
			go s.runUpdater(
				time.Unix(checkpoints.GeoIpv6Db.LastUpdatedUnix, 0),
				s.geoIpv6Src.RefreshInterval,
				IpdbTypeGeoIpv6,
				s.DownloadAndLoadGeoIpv6Mmdb,
			)
		}

		return nil
	}

	if options.LoadDatabasesInBackground {
		s.logger("loading databases in the background, as requested by Ipdb options", nil, false)
		go func() {
			if err := setup(); err != nil {
				s.logger("failed to initialize Ipdb in the background", err, true)
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
func (s *Ipdb) runUpdater(lastUpdate time.Time, updateInterval time.Duration, dbType IpdbType, updateFunc func() error) {
	if !s.updaterIsRunning {
		return
	}

	s.logger(fmt.Sprintf("running updater for %s database", dbType.String()), nil, false)

	update := func() error {
		if err := updateFunc(); err != nil {
			return err
		}

		s.updates <- ipdbUpdate{
			Ts:   time.Now(),
			Type: dbType,
		}

		return nil
	}

	firstUpdateTs := lastUpdate.Add(updateInterval)
	firstTimeout := time.NewTimer(firstUpdateTs.Sub(time.Now()))

	// Wait for next update time
	<-firstTimeout.C
	if !s.updaterIsRunning {
		return
	}

	err := update()
	if err != nil {
		s.logger(fmt.Sprintf("failed to do first scheduled update of %s database", dbType.String()), err, true)
	}

	ticker := time.NewTicker(s.torExitSrc.RefreshInterval)
	for s.updaterIsRunning {
		<-ticker.C
		if !s.updaterIsRunning {
			return
		}

		err = update()
		if err != nil {
			s.logger(fmt.Sprintf("failed to do scheduled update of %s database", dbType.String()), err, true)
		}
	}
}

// openDataSource opens a data source.
// The caller must close the returned reader.
func (s *Ipdb) openDataSource(src *DataSource) (io.ReadCloser, error) {
	var reader io.ReadCloser

	if src.Get == nil {
		req := &http.Request{
			Method: http.MethodGet,
			URL:    src.Url,
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, errors2.Wrap(err, "failed to download database (source URL)")
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to download database: status code %d", resp.StatusCode)
		}

		reader = resp.Body
	} else {
		var err error
		reader, err = src.Get()
		if err != nil {
			return nil, errors2.Wrap(err, "failed to get database (source Get function)")
		}
	}

	return reader, nil
}

// Does not close the reader.
func (s *Ipdb) loadTorExitNodeDatabaseFromReader(reader io.Reader) error {
	ips := make(map[netip.Addr]struct{})
	var ip netip.Addr
	var err error

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// TODO Convert bare IP to CIDR /32 for IPv4 and /128 for IPv6

		ip, err = netip.ParseAddr(line)
		if err != nil {
			return errors2.Wrapf(err, "failed to parse Tor IP \"%s\"", line)
		}
		ips[ip] = struct{}{}
	}

	s.torExitIpsMutex.Lock()
	s.torExitIps = ips
	s.hasTorExt = true
	s.torExitIpsMutex.Unlock()

	return nil
}

// Does not close the reader.
func (s *Ipdb) loadDatacenterRangesFromReader(reader io.Reader) error {
	ranger := cidranger.NewPCTrieRanger()
	var err error

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var ipnet *net.IPNet
		_, ipnet, err = net.ParseCIDR(line)
		if err != nil {
			return errors2.Wrapf(err, "failed to parse datacenter IP range \"%s\"", line)
		}
		err = ranger.Insert(cidranger.NewBasicRangerEntry(*ipnet))
		if err != nil {
			return errors2.Wrapf(err, "failed to insert datacenter IP range \"%s\"", line)
		}
	}

	s.datacenterRangerMutex.Lock()
	s.datacenterRanger = ranger
	s.hasDatacenter = true
	s.datacenterRangerMutex.Unlock()

	return nil
}

// Does not close the reader.
func (s *Ipdb) loadGeoIpMmdbFromReader(reader io.Reader, dbType IpdbType) error {
	s.logger("Loading "+dbType.String()+" database", nil, false)

	// Buffer the entire file into memory.
	geoIpMmdbBytes, err := io.ReadAll(reader)
	if err != nil {
		return errors2.Wrap(err, "failed to read GeoIP database")
	}

	db, err := maxminddb.FromBytes(geoIpMmdbBytes)
	if err != nil {
		return errors2.Wrap(err, "failed to parse GeoIP database")
	}

	if dbType == IpdbTypeGeoIpv4 {
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
	s.logger("Downloading and loading Tor exit node database", nil, false)

	reader, err := s.openDataSource(s.torExitSrc)
	if err != nil {
		return errors2.Wrap(err, "failed to read from Tor exit node database source")
	}

	defer func() {
		_ = reader.Close()
	}()

	pipeReader, pipeWriter := io.Pipe()

	parseErrChan := make(chan error)
	go func() {
		parseErrChan <- s.loadTorExitNodeDatabaseFromReader(pipeReader)
	}()

	persistReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.storage.WriteDatabase(IpdbTypeTorExit, persistReader)
	if err != nil {
		return errors2.Wrap(err, "failed to write Tor exit node database")
	}

	if err := <-parseErrChan; err != nil {
		return errors2.Wrap(err, "failed to parse Tor exit node database")
	}

	return nil
}

// DownloadAndLoadDatacenterRangesDatabase downloads the datacenter ranges database and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadDatacenterRangesDatabase() error {
	s.logger("Downloading and loading datacenter ranges database", nil, false)

	reader, err := s.openDataSource(s.datacenterSrc)
	if err != nil {
		return errors2.Wrap(err, "failed to read from datacenter ranges database source")
	}

	defer func() {
		_ = reader.Close()
	}()

	pipeReader, pipeWriter := io.Pipe()

	parseErrChan := make(chan error)
	go func() {
		parseErrChan <- s.loadDatacenterRangesFromReader(pipeReader)
	}()

	persistReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.storage.WriteDatabase(IpdbTypeDatacenter, persistReader)
	if err != nil {
		return errors2.Wrap(err, "failed to write datacenter ranges database")
	}

	if err := <-parseErrChan; err != nil {
		return errors2.Wrap(err, "failed to parse datacenter ranges database")
	}

	return nil
}

// DownloadAndLoadGeoIpv4Mmdb downloads the GeoIPv4 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv4Mmdb() error {
	s.logger("Downloading and loading GeoIPv4 MMDB", nil, false)

	reader, err := s.openDataSource(s.geoIpv4Src)
	if err != nil {
		return errors2.Wrap(err, "failed to read from GeoIPv4 MMDB source")
	}

	defer func() {
		_ = reader.Close()
	}()

	pipeReader, pipeWriter := io.Pipe()

	parseErrChan := make(chan error)
	go func() {
		parseErrChan <- s.loadGeoIpMmdbFromReader(pipeReader, IpdbTypeGeoIpv4)
	}()

	persistReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.storage.WriteDatabase(IpdbTypeGeoIpv4, persistReader)
	if err != nil {
		return errors2.Wrap(err, "failed to write GeoIPv4 MMDB database")
	}

	if err := <-parseErrChan; err != nil {
		return errors2.Wrap(err, "failed to parse GeoIPv4 MMDB database")
	}

	return nil
}

// DownloadAndLoadGeoIpv6Mmdb downloads the GeoIPv6 MMDB and loads it into memory.
// You most likely do not need to call this function, as loading databases is handled automatically by the Ipdb instance.
func (s *Ipdb) DownloadAndLoadGeoIpv6Mmdb() error {
	s.logger("Downloading and loading GeoIPv6 MMDB", nil, false)

	reader, err := s.openDataSource(s.geoIpv6Src)
	if err != nil {
		return errors2.Wrap(err, "failed to read from GeoIPv6 MMDB source")
	}

	defer func() {
		_ = reader.Close()
	}()

	pipeReader, pipeWriter := io.Pipe()

	parseErrChan := make(chan error)
	go func() {
		parseErrChan <- s.loadGeoIpMmdbFromReader(pipeReader, IpdbTypeGeoIpv6)
	}()

	persistReader := noOpReadCloser{io.TeeReader(reader, pipeWriter)}

	err = s.storage.WriteDatabase(IpdbTypeGeoIpv6, persistReader)
	if err != nil {
		return errors2.Wrap(err, "failed to write GeoIPv6 MMDB database")
	}

	if err := <-parseErrChan; err != nil {
		return errors2.Wrap(err, "failed to parse GeoIPv6 MMDB database")
	}

	return nil
}

func (s *Ipdb) Close() error {
	s.updaterIsRunning = false

	close(s.updates)

	if err := s.geoIpv4Mmdb.Close(); err != nil {
		return err
	}
	if err := s.geoIpv6Mmdb.Close(); err != nil {
		return err
	}

	return nil
}

// IsIpTorExitNode returns whether the specified IP address is a Tor exit node.
// If the required database is not initialized, returns NotInitializedError.
func (s *Ipdb) IsIpTorExitNode(ip netip.Addr) (bool, error) {
	lock := s.torExitIpsMutex.RLock()
	defer s.torExitIpsMutex.RUnlock(lock)

	if !s.hasTorExt {
		return false, NewNotInitializedError(IpdbTypeTorExit, fmt.Sprintf("cannot check if IP \"%s\" is a Tor exit node", ip.String()))
	}

	_, has := s.torExitIps[ip]
	return has, nil
}

// IsIpDatacenter returns whether the specified IP address is a datacenter IP.
// If the required database is not initialized, returns NotInitializedError.
func (s *Ipdb) IsIpDatacenter(ip netip.Addr) (bool, error) {
	lock := s.datacenterRangerMutex.RLock()
	defer s.datacenterRangerMutex.RUnlock(lock)

	if !s.hasDatacenter {
		return false, NewNotInitializedError(IpdbTypeDatacenter, fmt.Sprintf("cannot check if IP \"%s\" is a datacenter IP", ip.String()))
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
func (s *Ipdb) ResolveIpIsoCountry(ip netip.Addr, defaultCc string) (string, error) {
	var db *maxminddb.Reader
	if ip.Is6() {
		lock := s.geoIpv6MmdbMutex.RLock()
		defer s.geoIpv6MmdbMutex.RUnlock(lock)

		if !s.hasGeoIpv4 {
			return "", NewNotInitializedError(IpdbTypeGeoIpv6, fmt.Sprintf("cannot get country code for IP \"%s\"", ip.String()))
		}

		db = s.geoIpv6Mmdb
	} else {
		lock := s.geoIpv4MmdbMutex.RLock()
		defer s.geoIpv4MmdbMutex.RUnlock(lock)

		if !s.hasGeoIpv4 {
			return "", NewNotInitializedError(IpdbTypeGeoIpv4, fmt.Sprintf("cannot get country code for IP \"%s\"", ip.String()))
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
