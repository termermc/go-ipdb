package ipdb

// Integration tests that download real range lists.
// They are skipped with -short, and skipped if the lists cannot be reached.

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

var integrationRangeUrls = map[string]string{
	"quxlabs": "https://raw.githubusercontent.com/termermc/privacy-provider-ip-ranges/refs/heads/master/ranges/quxlabs.txt",
	"tmobile": "https://raw.githubusercontent.com/termermc/tmobile-ip-ranges/refs/heads/master/ranges.txt",
}

// fetchForIntegration downloads the URL, skipping the test if it cannot be reached.
func fetchForIntegration(t *testing.T, rawUrl string) []byte {
	t.Helper()

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(rawUrl)
	if err != nil {
		t.Skipf("cannot reach %s: %v", rawUrl, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		t.Skipf("cannot download %s: status %d", rawUrl, resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Skipf("cannot download %s: %v", rawUrl, err)
	}

	return data
}

// referencePrefixes parses a range list independently of the library.
func referencePrefixes(t *testing.T, data []byte) []netip.Prefix {
	t.Helper()

	var prefixes []netip.Prefix
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		prefix, err := netip.ParsePrefix(line)
		if err != nil {
			addr, addrErr := netip.ParseAddr(line)
			if addrErr != nil {
				t.Fatalf("reference parser could not parse line %q: %v", line, err)
			}
			prefix = netip.PrefixFrom(addr, addr.BitLen())
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("reference scanner failed: %v", err)
	}

	return prefixes
}

// lastAddr returns the last address in the prefix.
func lastAddr(prefix netip.Prefix) netip.Addr {
	b := prefix.Addr().AsSlice()
	for i := prefix.Bits(); i < len(b)*8; i++ {
		b[i/8] |= 0x80 >> (i % 8)
	}
	addr, _ := netip.AddrFromSlice(b)
	return addr
}

// outsideAddrs returns addresses just outside each end of the prefix.
func outsideAddrs(prefix netip.Prefix) []netip.Addr {
	var res []netip.Addr
	if prev := prefix.Addr().Prev(); prev.IsValid() {
		res = append(res, prev)
	}
	if next := lastAddr(prefix).Next(); next.IsValid() {
		res = append(res, next)
	}
	return res
}

func containedInAny(prefixes []netip.Prefix, addr netip.Addr) bool {
	for _, prefix := range prefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// checkAgainstReference checks that the database contains exactly the addresses covered by the prefixes,
// at the edges of each prefix.
func checkAgainstReference(t *testing.T, s *Ipdb, dbName string, prefixes []netip.Prefix) {
	t.Helper()

	check := func(addr netip.Addr, want bool) {
		t.Helper()

		got, err := s.IsIpInRangeDb(dbName, addr)
		if err != nil {
			t.Fatalf("IsIpInRangeDb(%s, %s) failed: %v", dbName, addr, err)
		}
		if got != want {
			t.Errorf("IsIpInRangeDb(%s, %s) = %v, want %v", dbName, addr, got, want)
		}
	}

	for _, prefix := range prefixes {
		check(prefix.Addr(), true)
		check(lastAddr(prefix), true)
		if prefix.Addr().Is4() {
			check(netip.AddrFrom16(prefix.Addr().As16()), true)
		}

		for _, addr := range outsideAddrs(prefix) {
			check(addr, containedInAny(prefixes, addr))
		}
	}

	for _, addr := range []string{"8.8.8.8", "1.1.1.1", "2001:4860:4860::8888"} {
		a := netip.MustParseAddr(addr)
		check(a, containedInAny(prefixes, a))
	}
}

func TestIntegrationRealRangeLists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network integration test in short mode")
	}

	// Download the lists independently to build the reference.
	all := make(map[string][]netip.Prefix, len(integrationRangeUrls))
	var merged []netip.Prefix
	for name, rawUrl := range integrationRangeUrls {
		prefixes := referencePrefixes(t, fetchForIntegration(t, rawUrl))
		if len(prefixes) == 0 {
			t.Fatalf("list %s has no ranges", name)
		}
		t.Logf("list %s has %d ranges", name, len(prefixes))

		all[name] = prefixes
		merged = append(merged, prefixes...)
	}

	sources := make(map[string]*DataSource, len(integrationRangeUrls)+1)
	mergedSource := &DataSource{RefreshInterval: time.Hour}
	for name, rawUrl := range integrationRangeUrls {
		parsed, err := url.Parse(rawUrl)
		if err != nil {
			t.Fatalf("failed to parse URL: %v", err)
		}
		sources[name] = &DataSource{RefreshInterval: time.Hour, Urls: []*url.URL{parsed}}
		mergedSource.Urls = append(mergedSource.Urls, parsed)
	}
	sources["merged"] = mergedSource

	drv, err := NewFsStorageDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	options := Options{
		StorageDriver: drv,
		Logger:        testLogger(),
		RangeSources:  sources,
	}

	s, err := NewIpdb(options)
	if err != nil {
		t.Fatalf("NewIpdb failed: %v", err)
	}
	for name, prefixes := range all {
		checkAgainstReference(t, s, name, prefixes)
	}
	checkAgainstReference(t, s, "merged", merged)
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// The cache written by the first instance must load identically without downloading.
	options.DisableDownload = true
	s, err = NewIpdb(options)
	if err != nil {
		t.Fatalf("NewIpdb from cache failed: %v", err)
	}
	defer func() {
		_ = s.Close()
	}()

	for name, prefixes := range all {
		checkAgainstReference(t, s, name, prefixes)
	}
	checkAgainstReference(t, s, "merged", merged)
}
