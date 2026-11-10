package ipdb

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rangeServer serves "/good" as a valid list, "/missing" as a 404, "/html" as an HTML page with status 200,
// and "/flaky" as a 503 for the first flakyFailures requests and a valid list after that.
func rangeServer(t *testing.T, flakyFailures int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	var flakyCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/good":
			_, _ = w.Write([]byte("1.2.3.4\n"))
		case "/good2":
			_, _ = w.Write([]byte("5.6.7.8\n"))
		case "/html":
			_, _ = w.Write([]byte("<html>\n<body>\nWe moved!\n</body>\n</html>\n"))
		case "/flaky":
			if flakyCalls.Add(1) <= flakyFailures {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("9.9.9.9\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)

	return ts, &flakyCalls
}

// With the default policy, a URL that 404s or serves something that is not a list
// is skipped, and the database is updated from its other URLs.
func TestRetryPolicy_DefaultSkipsBrokenUrls(t *testing.T) {
	ts, _ := rangeServer(t, 0)

	storage := newMemStorage()
	s := newTestIpdb(t, Options{
		StorageDriver: storage,
		RangeSources: map[string]*DataSource{
			testDb: urlSource(ts.URL+"/missing", ts.URL+"/good", ts.URL+"/html", ts.URL+"/good2"),
		},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	mustContain(t, s, "1.2.3.4", true)
	mustContain(t, s, "5.6.7.8", true)
	if cached, _ := storage.cached(testDb); cached != "1.2.3.4\n\n5.6.7.8\n\n" {
		t.Errorf("unexpected cached database: %q", cached)
	}
}

// If every URL is skipped, the update fails and the database keeps its data.
func TestRetryPolicy_AllUrlsSkippedKeepsData(t *testing.T) {
	ts, _ := rangeServer(t, 0)

	src := urlSource(ts.URL + "/good")
	s := newRangeTestIpdb(t, src)
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	src.Urls = urlSource(ts.URL+"/missing", ts.URL+"/html").Urls
	err := s.DownloadAndLoadRangeDatabase(testDb)
	if !errors.Is(err, ErrAllUrlsFailed) {
		t.Fatalf("DownloadAndLoadRangeDatabase returned %v, want ErrAllUrlsFailed", err)
	}
	statusErr, is := errors.AsType[*HttpStatusError](err)
	if !is || statusErr.StatusCode != http.StatusNotFound {
		t.Errorf("error does not wrap an HttpStatusError with status 404: %v", err)
	}

	mustContain(t, s, "1.2.3.4", true)
}

// The policy can retry a source, and receives the database name, URL, attempt
// number and error for each failure.
func TestRetryPolicy_Retry(t *testing.T) {
	ts, flakyCalls := rangeServer(t, 2)

	var mu sync.Mutex
	var seen []SourceFailure
	s := newTestIpdb(t, Options{
		RangeSources: map[string]*DataSource{
			testDb: urlSource(ts.URL+"/flaky", ts.URL+"/good"),
		},
		RetryPolicy: func(f SourceFailure) RetryDecision {
			mu.Lock()
			seen = append(seen, f)
			mu.Unlock()

			var statusErr *HttpStatusError
			if errors.As(f.Err, &statusErr) && statusErr.StatusCode >= 500 && f.Attempt < 5 {
				return RetryDecision{Action: RetryActionRetry, Delay: time.Millisecond}
			}
			return RetryDecision{Action: RetryActionSkip}
		},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	mustContain(t, s, "9.9.9.9", true)
	mustContain(t, s, "1.2.3.4", true)
	if got := flakyCalls.Load(); got != 3 {
		t.Errorf("flaky URL was requested %d times, want 3", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("policy was called %d times, want 2", len(seen))
	}
	for i, f := range seen {
		if f.DbName != testDb || f.Url == nil || f.Url.Path != "/flaky" || f.Attempt != i+1 {
			t.Errorf("unexpected failure passed to policy: %+v", f)
		}
	}
}

// RetryActionFail fails the whole update immediately: the remaining URLs are not
// downloaded and the database keeps its data.
func TestRetryPolicy_Fail(t *testing.T) {
	var goodCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/good" {
			goodCalls.Add(1)
			_, _ = w.Write([]byte("1.2.3.4\n"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	src := urlSource(ts.URL + "/good")
	s := newTestIpdb(t, Options{
		RangeSources: map[string]*DataSource{testDb: src},
		RetryPolicy: func(f SourceFailure) RetryDecision {
			return RetryDecision{Action: RetryActionFail}
		},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	src.Urls = urlSource(ts.URL+"/missing", ts.URL+"/good").Urls
	err := s.DownloadAndLoadRangeDatabase(testDb)
	if !errors.Is(err, ErrFailedByRetryPolicy) {
		t.Fatalf("DownloadAndLoadRangeDatabase returned %v, want ErrFailedByRetryPolicy", err)
	}
	if _, is := errors.AsType[*HttpStatusError](err); !is {
		t.Errorf("error does not wrap the source's error: %v", err)
	}
	if got := goodCalls.Load(); got != 1 {
		t.Errorf("URL after the failed one was downloaded %d times in total, want 1", got)
	}

	mustContain(t, s, "1.2.3.4", true)
}

// The policy also applies to geoIP mirrors: RetryActionFail stops trying further mirrors.
func TestRetryPolicy_FailStopsGeoMirrors(t *testing.T) {
	ts, _ := rangeServer(t, 0)

	var calls atomic.Int32
	s := newTestIpdb(t, Options{
		GeoIpv4Source: urlSource(ts.URL+"/missing", ts.URL+"/html"),
		RetryPolicy: func(f SourceFailure) RetryDecision {
			calls.Add(1)
			if f.DbName != DbNameGeoIpv4 {
				t.Errorf("policy got database name %q, want %q", f.DbName, DbNameGeoIpv4)
			}
			return RetryDecision{Action: RetryActionFail}
		},
	})

	if err := s.DownloadAndLoadGeoIpv4Mmdb(); !errors.Is(err, ErrFailedByRetryPolicy) {
		t.Fatalf("DownloadAndLoadGeoIpv4Mmdb returned %v, want ErrFailedByRetryPolicy", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("policy was called %d times, want 1", got)
	}
}

// The policy applies to Get functions, which are passed to it with a nil URL.
func TestRetryPolicy_GetSource(t *testing.T) {
	src, calls := flakySource(2, "1.2.3.4\n")
	s := newTestIpdb(t, Options{
		RangeSources: map[string]*DataSource{testDb: src},
		RetryPolicy: func(f SourceFailure) RetryDecision {
			if f.Url != nil {
				t.Errorf("policy got URL %v for a Get source, want nil", f.Url)
			}
			return RetryDecision{Action: RetryActionRetry}
		},
	})
	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}

	mustContain(t, s, "1.2.3.4", true)
	if got := calls.Load(); got != 3 {
		t.Errorf("Get was called %d times, want 3", got)
	}
}

// Closing the instance cuts a retry delay short.
func TestRetryPolicy_CloseInterruptsRetryDelay(t *testing.T) {
	ts, _ := rangeServer(t, 0)

	policyCalled := make(chan struct{}, 1)
	s := newTestIpdb(t, Options{
		RangeSources: map[string]*DataSource{testDb: urlSource(ts.URL + "/missing")},
		RetryPolicy: func(f SourceFailure) RetryDecision {
			select {
			case policyCalled <- struct{}{}:
			default:
			}
			return RetryDecision{Action: RetryActionRetry, Delay: time.Hour}
		},
	})

	done := make(chan error, 1)
	go func() {
		done <- s.DownloadAndLoadRangeDatabase(testDb)
	}()

	<-policyCalled
	closeDone := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closeDone)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrIpdbClosed) {
			t.Errorf("DownloadAndLoadRangeDatabase returned %v, want ErrIpdbClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retry delay was not interrupted by Close")
	}
	<-closeDone
}

// One database's broken source does not stop other databases from updating.
func TestRetryPolicy_BrokenDatabaseDoesNotBlockOthers(t *testing.T) {
	ts, _ := rangeServer(t, 0)

	s := newTestIpdb(t, Options{
		RangeSources: map[string]*DataSource{
			"broken": urlSource(ts.URL + "/missing"),
			testDb:   urlSource(ts.URL + "/good"),
		},
		RetryPolicy: func(f SourceFailure) RetryDecision {
			return RetryDecision{Action: RetryActionRetry, Delay: time.Hour}
		},
	})

	go func() {
		_ = s.DownloadAndLoadRangeDatabase("broken")
	}()

	if err := s.DownloadAndLoadRangeDatabase(testDb); err != nil {
		t.Fatalf("DownloadAndLoadRangeDatabase failed: %v", err)
	}
	mustContain(t, s, "1.2.3.4", true)
}
