package ipdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A read error part-way through a list fails the load, rather than a truncated list
// being accepted as complete.
func TestParseRanges_ReturnsReadError(t *testing.T) {
	s := newRangeTestIpdb(t, staticSource(""))

	reader := &failAfterReader{
		data: []byte("1.2.3.4\n"),
		err:  io.ErrUnexpectedEOF,
	}

	_, err := s.parseRanges(testDb, reader)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("parseRanges returned %v for a reader that failed with %v", err, io.ErrUnexpectedEOF)
	}
}

// Malformed lines are skipped without stopping the parse, so valid entries after
// them are still loaded.
func TestParseRanges_ReadsPastMalformedLines(t *testing.T) {
	var sb strings.Builder
	for i := range 20 {
		_, _ = fmt.Fprintf(&sb, "10.0.%d.1\n", i)
	}
	for range 10 {
		sb.WriteString("this-is-not-an-ip\n")
	}
	sb.WriteString("198.51.100.7\n")

	s := newRangeTestIpdb(t, staticSource(sb.String()))
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	mustContain(t, s, "198.51.100.7", true)
}

// A list that is mostly garbage is rejected and does not replace existing data.
func TestParseRanges_RejectsMostlyMalformedList(t *testing.T) {
	var body atomic.Value
	body.Store("1.2.3.4\n")

	s := newRangeTestIpdb(t, stringSource(&body))
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	body.Store("<html>\n<body>\nrate limited\n</body>\n</html>\n9.9.9.9\n")
	if err := s.DownloadAndLoadRangeDatabase(testDb); err == nil {
		t.Errorf("expected an error for a mostly malformed list")
	}

	mustContain(t, s, "1.2.3.4", true)
	mustContain(t, s, "9.9.9.9", false)
}

// IPv4-mapped IPv6 entries are stored as IPv4 and can be mixed with IPv4 and IPv6
// entries in any order.
func TestParseRanges_Ipv4MappedIpv6Entries(t *testing.T) {
	bodies := []string{
		"::ffff:1.2.3.4\n8.8.8.8\n::ffff:10.0.0.0/104\n",
		"8.8.8.8\n::ffff:10.0.0.0/104\n::ffff:1.2.3.4\n",
		"::ffff:10.0.0.0/104\n::ffff:1.2.3.4\n8.8.8.8\n",
	}
	for _, body := range bodies {
		s := newRangeTestIpdb(t, staticSource(body+"2001:db8::/32\nfe80::1%eth0\n"))
		if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
			t.Fatalf("DownloadAndLoadRangeDatabase failed for %q: %v", body, err)
		}

		for _, ip := range []string{"1.2.3.4", "::ffff:1.2.3.4", "8.8.8.8", "::ffff:8.8.8.8", "10.20.30.40", "::ffff:10.20.30.40", "2001:db8::1", "fe80::1"} {
			mustContain(t, s, ip, true)
		}
		for _, ip := range []string{"1.2.3.5", "11.0.0.1", "200.1.1.1", "2001:db9::1"} {
			mustContain(t, s, ip, false)
		}
	}
}

// Surrounding whitespace, CRLF line endings, full-line comments and trailing
// comments are ignored.
func TestParseRanges_TrimsWhitespaceAndComments(t *testing.T) {
	s := newRangeTestIpdb(t, staticSource("# header\r\n  1.2.3.4  \r\n\t10.0.0.0/8 # private\n\n   # indented comment\n9.9.9.9#dns\n"))
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	for _, ip := range []string{"1.2.3.4", "10.1.2.3", "9.9.9.9"} {
		mustContain(t, s, ip, true)
	}
}

// A non-200 response produces a well-formed error that includes the status code
// and the start of the body.
func TestDownload_Non200ErrorIncludesStatusAndBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server exploded"))
	}))
	defer ts.Close()

	s := newRangeTestIpdb(t, urlSource(ts.URL))

	err := s.DownloadAndLoadRangeDatabase(testDb)
	if err == nil || !errors.Is(err, ErrAllUrlsFailed) {
		t.Fatalf("DownloadAndLoadRangeDatabase returned %v, want ErrAllUrlsFailed", err)
	}
	if strings.Contains(err.Error(), "%!") {
		t.Errorf("malformed error message for non-200 response: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "server exploded") {
		t.Errorf("error message does not include the status code and body: %q", err.Error())
	}
}

// With multiple URLs, a download that is cut off mid-body is skipped entirely.
// Its partial last line must not be loaded, since it could parse as a much larger
// range than intended (here "128.0.0.0/1" instead of "128.0.0.0/16").
func TestDownload_SkipsTruncatedUrl(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/truncated" {
			_, _ = w.Write([]byte("7.7.7.7"))
			return
		}

		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("9.9.9.9\n128.0.0.0/1"))
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer ts.Close()

	storage := newMemStorage()
	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		RangeSources: map[string]*DataSource{
			testDb: urlSource(ts.URL+"/truncated", ts.URL+"/a", ts.URL+"/b"),
		},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	mustContain(t, s, "7.7.7.7", true)
	mustContain(t, s, "200.1.2.3", false)
	mustContain(t, s, "9.9.9.9", false)

	if cached, _ := storage.cached(testDb); cached != "7.7.7.7\n7.7.7.7\n" {
		t.Errorf("unexpected cached database: %q", cached)
	}
}

// A list with no ranges is rejected and leaves both the in-memory database and
// the cache untouched.
func TestDownload_EmptyListDoesNotReplaceDatabase(t *testing.T) {
	var body atomic.Value
	body.Store("1.2.3.4\n")

	storage := newMemStorage()
	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		RangeSources:  map[string]*DataSource{testDb: stringSource(&body)},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	for _, empty := range []string{"", "\n\n# nothing here\n"} {
		body.Store(empty)
		if err := s.DownloadAndLoadRangeDatabase(testDb); !errors.Is(err, ErrNoRanges) {
			t.Errorf("DownloadAndLoadRangeDatabase with body %q returned %v, want ErrNoRanges", empty, err)
		}
	}

	mustContain(t, s, "1.2.3.4", true)
	if cached, _ := storage.cached(testDb); cached != "1.2.3.4\n" {
		t.Errorf("cache was overwritten: %q", cached)
	}
}

// If writing to the cache fails, the error is reported but the complete download
// is still loaded into memory.
func TestDownload_StorageFailureKeepsNewDataInMemory(t *testing.T) {
	var sb strings.Builder
	for i := range 100000 {
		_, _ = fmt.Fprintf(&sb, "10.%d.%d.1\n", i/256%256, i%256)
	}
	sb.WriteString("1.2.3.4\n")

	storage := newMemStorage()
	storage.failWrites = true

	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		RangeSources:  map[string]*DataSource{testDb: staticSource(sb.String())},
	})

	if err := s.DownloadAndLoadRangeDatabase(testDb); err == nil {
		t.Errorf("expected the storage write failure to be reported")
	}

	// The complete download is still usable in memory.
	mustContain(t, s, "1.2.3.4", true)
	mustContain(t, s, "10.255.255.1", true)
}

// Databases larger than the maximum size are rejected, whether they come from a
// Get function, a single URL or several URLs merged together.
func TestDownload_EnforcesMaxDatabaseSize(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("1.2.3.4\n5.6.7.8\n"))
	}))
	defer ts.Close()

	for _, src := range []*DataSource{
		staticSource("1.2.3.4\n5.6.7.8\n9.9.9.9\n"),
		urlSource(ts.URL),
		// Each URL fits, but the merged database does not.
		urlSource(ts.URL+"/a", ts.URL+"/b"),
	} {
		s := newTestIpdb(t, Options{
			MaxDatabaseSize: 10,
			RangeSources:    map[string]*DataSource{testDb: src},
		})
		if len(src.Urls) == 2 {
			s.maxDbSize = 30
		}

		if err := s.DownloadAndLoadRangeDatabase(testDb); !errors.Is(err, ErrDatabaseTooLarge) {
			t.Errorf("DownloadAndLoadRangeDatabase returned %v, want ErrDatabaseTooLarge", err)
		}
	}
}

// Credentials and query parameters (such as license keys) in source URLs never
// appear in errors or logs.
func TestDownload_RedactsUrlSecrets(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	var logs bytes.Buffer
	withSecrets := strings.Replace(ts.URL, "http://", "http://user:hunter2@", 1) + "/db?license_key=supersecret#frag"
	// The second URL fails before a response is received.
	unreachable := "http://user:hunter2@127.0.0.1:1/db?license_key=supersecret"

	s := newTestIpdb(t, Options{
		Logger:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		RangeSources: map[string]*DataSource{testDb: urlSource(withSecrets, unreachable)},
	})

	err := s.DownloadAndLoadRangeDatabase(testDb)
	if err == nil || !errors.Is(err, ErrAllUrlsFailed) {
		t.Fatalf("DownloadAndLoadRangeDatabase returned %v, want ErrAllUrlsFailed", err)
	}

	for _, secret := range []string{"hunter2", "supersecret"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error contains secret %q: %v", secret, err)
		}
		if strings.Contains(logs.String(), secret) {
			t.Errorf("logs contain secret %q: %s", secret, logs.String())
		}
	}
	if !strings.Contains(err.Error(), "/db?REDACTED") {
		t.Errorf("error does not identify the failing URL: %v", err)
	}
}

// Downloading or querying a geoIP database without a configured source returns an
// error.
func TestGeoIp_UnconfiguredSourceReturnsError(t *testing.T) {
	s := newTestIpdb(t, Options{})

	if err := s.DownloadAndLoadGeoIpv4Mmdb(); !errors.Is(err, ErrDataSourceNoSource) {
		t.Errorf("DownloadAndLoadGeoIpv4Mmdb returned %v, want ErrDataSourceNoSource", err)
	}
	if err := s.DownloadAndLoadGeoIpv6Mmdb(); !errors.Is(err, ErrDataSourceNoSource) {
		t.Errorf("DownloadAndLoadGeoIpv6Mmdb returned %v, want ErrDataSourceNoSource", err)
	}

	var notInit *NotInitializedError
	if _, err := s.ResolveIpIsoCountry(netip.MustParseAddr("1.2.3.4"), "ZZ"); !errors.As(err, &notInit) {
		t.Errorf("ResolveIpIsoCountry returned %v, want NotInitializedError", err)
	}
}

// An invalid geoIP download returns an error, is not cached, and leaves no
// goroutines behind.
func TestGeoIp_InvalidDownloadIsRejectedCleanly(t *testing.T) {
	storage := newMemStorage()
	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		GeoIpv4Source: staticSource("definitely not an mmdb"),
	})

	if err := s.DownloadAndLoadGeoIpv4Mmdb(); err == nil {
		t.Fatalf("expected parse error from DownloadAndLoadGeoIpv4Mmdb")
	}
	if _, has := storage.cached(DbNameGeoIpv4); has {
		t.Errorf("invalid geoIP database was written to the cache")
	}

	time.Sleep(50 * time.Millisecond)
	if goroutineStackContains("(*Ipdb).") {
		t.Errorf("a goroutine is still running Ipdb code after DownloadAndLoadGeoIpv4Mmdb returned")
	}
}

// Multiple URLs on a geoIP source are mirrors: they are tried in order and the
// first valid database is used.
func TestGeoIp_UrlsAreMirrors(t *testing.T) {
	mmdb, err := os.ReadFile(filepath.Join("testdata", "GeoIP2-Country-Test.mmdb"))
	if err != nil {
		t.Fatalf("failed to read test data: %v", err)
	}

	var goodHits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/down":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/garbage":
			_, _ = w.Write([]byte("<html>this is not an mmdb</html>"))
		default:
			goodHits.Add(1)
			_, _ = w.Write(mmdb)
		}
	}))
	defer ts.Close()

	storage := newMemStorage()
	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		GeoIpv4Source: urlSource(ts.URL+"/down", ts.URL+"/garbage", ts.URL+"/good", ts.URL+"/good2"),
	})
	if err := s.DownloadAndLoadGeoIpv4Mmdb(); err != nil {
		t.Fatalf("DownloadAndLoadGeoIpv4Mmdb failed: %v", err)
	}

	cc, err := s.ResolveIpIsoCountry(netip.MustParseAddr("2.125.160.216"), "ZZ")
	if err != nil || cc != "GB" {
		t.Errorf("ResolveIpIsoCountry = %q, %v; want GB", cc, err)
	}
	if got := goodHits.Load(); got != 1 {
		t.Errorf("%d working mirrors were downloaded, want 1", got)
	}
	if cached, _ := storage.cached(DbNameGeoIpv4); cached != string(mmdb) {
		t.Errorf("cached geoIP database is not identical to the downloaded one")
	}
}

// Country codes are resolved from MaxMind's own schema (country.iso_code) for IPv4,
// IPv6 and IPv4-mapped IPv6 addresses.
//
// testdata/GeoIP2-Country-Test.mmdb is MaxMind's own test database.
func TestResolveIpIsoCountry(t *testing.T) {
	s := newTestIpdb(t, Options{
		GeoIpv4Source: mmdbSource(),
		GeoIpv6Source: mmdbSource(),
	})
	if err := s.DownloadAndLoadGeoIpv4Mmdb(); err != nil {
		t.Fatalf("DownloadAndLoadGeoIpv4Mmdb failed: %v", err)
	}
	if err := s.DownloadAndLoadGeoIpv6Mmdb(); err != nil {
		t.Fatalf("DownloadAndLoadGeoIpv6Mmdb failed: %v", err)
	}

	cases := map[string]string{
		"2.125.160.216": "GB",
		// IPv4-mapped IPv6 addresses are resolved as IPv4.
		"::ffff:2.125.160.216": "GB",
		"2001:218::1":          "JP",
		// Not in the database.
		"10.0.0.1": "ZZ",
	}
	for ip, want := range cases {
		cc, err := s.ResolveIpIsoCountry(netip.MustParseAddr(ip), "ZZ")
		if err != nil {
			t.Fatalf("ResolveIpIsoCountry(%s) failed: %v", ip, err)
		}
		if cc != want {
			t.Errorf("ResolveIpIsoCountry(%s) = %q, want %q", ip, cc, want)
		}
	}

	if _, err := s.ResolveIpIsoCountry(netip.Addr{}, "ZZ"); err == nil {
		t.Errorf("expected an error for an invalid IP")
	}
}

// Unusable range database names are rejected when the instance is created.
func TestNewIpdb_RejectsInvalidDatabaseNames(t *testing.T) {
	drv, err := NewFsStorageDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	cases := map[string]error{
		"":                                   ErrInvalidDbName,
		DbNameGeoIpv4:                        ErrInvalidDbName,
		DbNameGeoIpv6:                        ErrInvalidDbName,
		checkpointsFilename:                  ErrInvalidDbName,
		"..":                                 ErrInvalidDbName,
		strings.Repeat("a", DbNameMaxSize+1): ErrDbNameTooLong,
	}
	for name, want := range cases {
		_, err := NewIpdb(Options{
			StorageDriver: drv,
			Logger:        testLogger(),
			RangeSources:  map[string]*DataSource{name: staticSource("1.2.3.4\n")},
		})
		if !errors.Is(err, want) {
			t.Errorf("NewIpdb with range database name %q returned %v, want %v", name, err, want)
		}
	}
}

// Data sources with a non-positive refresh interval or nothing to download from are
// rejected when the instance is created.
func TestNewIpdb_RejectsInvalidDataSources(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		src := staticSource("1.2.3.4\n")
		src.RefreshInterval = interval

		_, err := NewIpdb(Options{
			StorageDriver: newMemStorage(),
			Logger:        testLogger(),
			RangeSources:  map[string]*DataSource{testDb: src},
		})
		if !errors.Is(err, ErrInvalidRefreshInterval) {
			t.Errorf("NewIpdb with RefreshInterval %s returned %v, want ErrInvalidRefreshInterval", interval, err)
		}
	}

	// A source without a Get function or URLs is rejected up front as well.
	_, err := NewIpdb(Options{
		StorageDriver: newMemStorage(),
		Logger:        testLogger(),
		GeoIpv4Source: &DataSource{RefreshInterval: time.Hour},
	})
	if !errors.Is(err, ErrDataSourceNoSource) {
		t.Errorf("NewIpdb with an empty source returned %v, want ErrDataSourceNoSource", err)
	}
}

// Databases and checkpoints written by one instance are picked up by the next.
func TestNewIpdb_RoundTrip(t *testing.T) {
	drv, err := NewFsStorageDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	src, calls := flakySource(0, "1.2.3.4\n10.0.0.0/8\n")
	options := Options{
		StorageDriver: drv,
		Logger:        testLogger(),
		RangeSources:  map[string]*DataSource{testDb: src},
		GeoIpv4Source: mmdbSource(),
		GeoIpv6Source: mmdbSource(),
	}

	for range 2 {
		s, err := NewIpdb(options)
		if err != nil {
			t.Fatalf("NewIpdb failed: %v", err)
		}

		mustContain(t, s, "10.1.2.3", true)
		mustContain(t, s, "11.1.2.3", false)
		if cc, err := s.ResolveIpIsoCountry(netip.MustParseAddr("2001:218::1"), "ZZ"); err != nil || cc != "JP" {
			t.Errorf("ResolveIpIsoCountry = %q, %v; want JP", cc, err)
		}

		var noSuchDb *NoSuchDatabaseError
		if _, err := s.IsIpInRangeDb("missing", netip.MustParseAddr("1.2.3.4")); !errors.As(err, &noSuchDb) {
			t.Errorf("IsIpInRangeDb for a missing database returned %v, want NoSuchDatabaseError", err)
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// The second instance used the cache; the checkpoint is recent, so no update was due.
	if got := calls.Load(); got != 1 {
		t.Errorf("source was called %d times, want 1", got)
	}

	chk, err := drv.ReadCheckpoints()
	if err != nil {
		t.Fatalf("ReadCheckpoints failed: %v", err)
	}
	for _, name := range []string{testDb, DbNameGeoIpv4, DbNameGeoIpv6} {
		if chk.Checkpoints[name].LastUpdatedUnix == 0 {
			t.Errorf("no checkpoint was saved for database %q", name)
		}
	}
}

// A corrupt cached database is replaced by a fresh download during startup.
func TestNewIpdb_RecoversFromCorruptCache(t *testing.T) {
	dir := t.TempDir()
	drv, err := NewFsStorageDriver(dir)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, DbNameGeoIpv4), []byte("corrupt"), 0644); err != nil {
		t.Fatalf("failed to write corrupt database: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, testDb), []byte("<html>not a list</html>\n"), 0644); err != nil {
		t.Fatalf("failed to write corrupt database: %v", err)
	}

	options := Options{
		StorageDriver: drv,
		Logger:        testLogger(),
		RangeSources:  map[string]*DataSource{testDb: staticSource("1.2.3.4\n")},
		GeoIpv4Source: mmdbSource(),
	}

	s, err := NewIpdb(options)
	if err != nil {
		t.Fatalf("NewIpdb failed with corrupt cache: %v", err)
	}

	mustContain(t, s, "1.2.3.4", true)
	if cc, err := s.ResolveIpIsoCountry(netip.MustParseAddr("2.125.160.216"), "ZZ"); err != nil || cc != "GB" {
		t.Errorf("ResolveIpIsoCountry = %q, %v; want GB", cc, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// The cache was repaired, so it can now be used without downloading.
	options.DisableDownload = true
	options.RangeSources[testDb].Get = func() (io.ReadCloser, error) {
		t.Errorf("source was used even though downloading is disabled")
		return nil, errors.New("downloading is disabled")
	}

	s, err = NewIpdb(options)
	if err != nil {
		t.Fatalf("NewIpdb failed with repaired cache and downloading disabled: %v", err)
	}
	defer func() {
		_ = s.Close()
	}()

	mustContain(t, s, "1.2.3.4", true)

	// No temp files may be left behind.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), tmpFilePrefix) {
			t.Errorf("temp file was left behind: %s", entry.Name())
		}
	}
}

// With downloading disabled, a missing or corrupt cache is an error.
func TestNewIpdb_DisableDownloadRequiresUsableCache(t *testing.T) {
	storage := newMemStorage()
	options := Options{
		StorageDriver:   storage,
		Logger:          testLogger(),
		DisableDownload: true,
		RangeSources:    map[string]*DataSource{testDb: staticSource("1.2.3.4\n")},
	}

	if _, err := NewIpdb(options); !errors.Is(err, ErrNoCacheAndNoDownload) {
		t.Errorf("NewIpdb without cache returned %v, want ErrNoCacheAndNoDownload", err)
	}

	storage.dbs[testDb] = []byte("garbage\n")
	if _, err := NewIpdb(options); err == nil {
		t.Errorf("NewIpdb with corrupt cache and downloading disabled did not fail")
	}
}

// With LoadDatabasesInBackground, NewIpdb returns before the databases are loaded,
// and they become usable once background loading finishes.
func TestNewIpdb_BackgroundInitLoadsDatabases(t *testing.T) {
	release := make(chan struct{})
	storage := newMemStorage()
	s, err := NewIpdb(Options{
		StorageDriver: storage,
		Logger:        testLogger(),
		RangeSources: map[string]*DataSource{testDb: {
			RefreshInterval: time.Hour,
			Get: func() (io.ReadCloser, error) {
				<-release
				return io.NopCloser(strings.NewReader("1.2.3.4\n")), nil
			},
		}},
		LoadDatabasesInBackground: true,
	})
	if err != nil {
		t.Fatalf("NewIpdb failed: %v", err)
	}
	defer func() {
		_ = s.Close()
	}()

	var notInit *NotInitializedError
	if _, err := s.IsIpInRangeDb(testDb, netip.MustParseAddr("1.2.3.4")); !errors.As(err, &notInit) {
		t.Errorf("IsIpInRangeDb before loading finished returned %v, want NotInitializedError", err)
	}

	close(release)
	waitFor(t, "background initialization to succeed", func() bool {
		return isLoaded(s, "1.2.3.4")
	})
	waitFor(t, "checkpoints to be saved", func() bool {
		chk, err := storage.ReadCheckpoints()
		return err == nil && chk.Checkpoints[testDb].LastUpdatedUnix != 0
	})
}

// With LoadDatabasesInBackground, a failed initialization is not retried: the
// instance is closed, and its methods return ErrIpdbClosed.
func TestNewIpdb_BackgroundInitFailureClosesInstance(t *testing.T) {
	src, calls := flakySource(1000, "1.2.3.4\n")
	s, err := NewIpdb(Options{
		StorageDriver:             newMemStorage(),
		Logger:                    testLogger(),
		RangeSources:              map[string]*DataSource{testDb: src},
		LoadDatabasesInBackground: true,
	})
	if err != nil {
		t.Fatalf("NewIpdb failed: %v", err)
	}

	waitFor(t, "the instance to be closed", func() bool {
		_, err := s.IsIpInRangeDb(testDb, netip.MustParseAddr("1.2.3.4"))
		return errors.Is(err, ErrIpdbClosed)
	})

	// Close is idempotent, so calling it again waits for the background Close to finish.
	if err := s.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("source was called %d times, want 1", got)
	}
	if goroutineStackContains("(*Ipdb).") {
		t.Errorf("a goroutine is still running Ipdb code after the instance was closed")
	}
}

// A failed scheduled update is retried after a short delay, and the next update
// after a success waits for the full refresh interval.
func TestUpdater_RetriesFailedUpdates(t *testing.T) {
	src, calls := flakySource(3, "1.2.3.4\n")
	s := newRangeTestIpdb(t, src)
	s.updateRetryDelay = time.Millisecond

	// The last update was long ago, so the first update is due immediately.
	s.goRun(func() {
		s.runUpdater(s.rangeDbs[testDb], time.Unix(0, 0))
	})

	waitFor(t, "scheduled update to succeed after retries", func() bool {
		return isLoaded(s, "1.2.3.4")
	})

	// After a successful update, the next one is a full refresh interval away.
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 4 {
		t.Errorf("source was called %d times, want 4", got)
	}
}

// The retry delay doubles with each failure and never exceeds the refresh interval.
func TestRetryDelay(t *testing.T) {
	cases := []struct {
		failures int
		interval time.Duration
		want     time.Duration
	}{
		{1, time.Hour, time.Minute},
		{2, time.Hour, 2 * time.Minute},
		{6, time.Hour, 32 * time.Minute},
		{7, time.Hour, time.Hour},
		{1000, time.Hour, time.Hour},
		{1, 10 * time.Second, 10 * time.Second},
	}
	for _, c := range cases {
		if got := retryDelay(time.Minute, c.failures, c.interval); got != c.want {
			t.Errorf("retryDelay(1m, %d, %s) = %s, want %s", c.failures, c.interval, got, c.want)
		}
	}
}

// Close can run concurrently with queries and downloads and can be called more
// than once. Afterwards, all methods return ErrIpdbClosed.
//
// Run with the race detector to make this test meaningful.
func TestClose_ConcurrentWithQueriesAndIdempotent(t *testing.T) {
	s := newTestIpdb(t, Options{
		RangeSources:  map[string]*DataSource{testDb: staticSource("1.2.3.4\n")},
		GeoIpv4Source: mmdbSource(),
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}
	if err := s.DownloadAndLoadGeoIpv4Mmdb(); err != nil {
		t.Fatalf("DownloadAndLoadGeoIpv4Mmdb failed: %v", err)
	}

	addr := netip.MustParseAddr("1.2.3.4")
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, rangeErr := s.IsIpInRangeDb(testDb, addr)
				_, geoErr := s.ResolveIpIsoCountry(addr, "ZZ")
				_ = s.DownloadAndLoadRangeDatabase(testDb)
				if rangeErr != nil || geoErr != nil {
					if !errors.Is(rangeErr, ErrIpdbClosed) && rangeErr != nil {
						t.Errorf("IsIpInRangeDb returned unexpected error: %v", rangeErr)
					}
					if !errors.Is(geoErr, ErrIpdbClosed) && geoErr != nil {
						t.Errorf("ResolveIpIsoCountry returned unexpected error: %v", geoErr)
					}
					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	for range 3 {
		if err := s.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}
	wg.Wait()

	if _, err := s.IsIpInRangeDb(testDb, addr); !errors.Is(err, ErrIpdbClosed) {
		t.Errorf("IsIpInRangeDb after Close returned %v, want ErrIpdbClosed", err)
	}
	if err := s.DownloadAndLoadRangeDatabase(testDb); !errors.Is(err, ErrIpdbClosed) {
		t.Errorf("DownloadAndLoadRangeDatabase after Close returned %v, want ErrIpdbClosed", err)
	}
}

// Lookups run without locking while range and geoIP databases are replaced and the
// instance is closed. Every lookup must see a complete database (old or new), replaced
// geoIP readers must stay usable by lookups that still hold them, and no database may
// be left installed after Close, even if an update finishes while Close is running.
func TestLookups_ConcurrentWithUpdatesAndClose(t *testing.T) {
	var body atomic.Value
	body.Store("1.2.3.4\n")

	s := newTestIpdb(t, Options{
		RangeSources:  map[string]*DataSource{testDb: stringSource(&body)},
		GeoIpv4Source: mmdbSource(),
		GeoIpv6Source: mmdbSource(),
	})
	for _, load := range []func() error{
		func() error { return s.DownloadAndLoadRangeDatabase(testDb) },
		s.DownloadAndLoadGeoIpv4Mmdb,
		s.DownloadAndLoadGeoIpv6Mmdb,
	} {
		if err := load(); err != nil {
			t.Fatalf("initial load failed: %v", err)
		}
	}

	// Both versions of the range list contain 1.2.3.4; they differ in 5.6.7.8.
	rangeAddr := netip.MustParseAddr("1.2.3.4")
	geoV4 := netip.MustParseAddr("2.125.160.216")
	geoV6 := netip.MustParseAddr("2001:218::1")

	check := func(err error, what string) bool {
		if err == nil {
			return true
		}
		if !errors.Is(err, ErrIpdbClosed) {
			t.Errorf("%s returned unexpected error: %v", what, err)
		}
		return false
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				has, err := s.IsIpInRangeDb(testDb, rangeAddr)
				if !check(err, "IsIpInRangeDb") {
					return
				}
				if !has {
					t.Errorf("IsIpInRangeDb(%s) = false during an update, want true", rangeAddr)
					return
				}

				for addr, want := range map[netip.Addr]string{geoV4: "GB", geoV6: "JP"} {
					cc, err := s.ResolveIpIsoCountry(addr, "ZZ")
					if !check(err, "ResolveIpIsoCountry") {
						return
					}
					if cc != want {
						t.Errorf("ResolveIpIsoCountry(%s) = %q during an update, want %q", addr, cc, want)
						return
					}
				}
			}
		}()
	}

	var updaters sync.WaitGroup
	for i, update := range []func() error{
		func() error { return s.DownloadAndLoadRangeDatabase(testDb) },
		s.DownloadAndLoadGeoIpv4Mmdb,
		s.DownloadAndLoadGeoIpv6Mmdb,
	} {
		updaters.Add(1)
		go func() {
			defer updaters.Done()
			for n := 0; ; n++ {
				if i == 0 {
					if n%2 == 0 {
						body.Store("1.2.3.4\n5.6.7.8\n")
					} else {
						body.Store("1.2.3.4\n")
					}
				}
				if err := update(); errors.Is(err, ErrIpdbClosed) {
					return
				} else if err != nil {
					t.Errorf("update failed: %v", err)
					return
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	wg.Wait()
	updaters.Wait()

	for _, db := range s.allDbs() {
		if db.isLoaded() {
			t.Errorf("database %q is still loaded after Close", db.name)
		}
	}
}

// Close cancels in-flight downloads and waits for all background goroutines to
// stop.
func TestClose_StopsBackgroundWork(t *testing.T) {
	requestStarted := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hang" {
			select {
			case requestStarted <- struct{}{}:
			default:
			}
			// Hang until the client gives up.
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte("1.2.3.4\n"))
	}))
	defer ts.Close()

	src := urlSource(ts.URL + "/ok")
	storage := newMemStorage()
	// This database is cached, but has no checkpoint, so its updater starts a download immediately.
	storage.dbs["hanging"] = []byte("5.6.7.8\n")
	s, err := NewIpdb(Options{
		StorageDriver: storage,
		Logger:        testLogger(),
		HttpClient:    ts.Client(),
		RangeSources: map[string]*DataSource{
			testDb:    src,
			"hanging": urlSource(ts.URL + "/hang"),
		},
	})
	if err != nil {
		t.Fatalf("NewIpdb failed: %v", err)
	}

	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the updater to start its download")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- s.Close()
	}()

	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Close did not return while a download was hanging")
	}

	if goroutineStackContains("(*Ipdb).") {
		t.Errorf("a goroutine is still running Ipdb code after Close returned")
	}
}
