package ipdb

import (
	"fmt"
	"net/url"
	"time"
)

// SourceFailure describes a failed attempt to download one source of a database.
type SourceFailure struct {
	// DbName is the name of the database being updated.
	// This is either a range database name, DbNameGeoIpv4 or DbNameGeoIpv6.
	DbName string

	// Url is the URL that failed, or nil if the source is a DataSource.Get function.
	// It may contain credentials or query parameters such as license keys, so take care when logging it.
	Url *url.URL

	// Attempt is the number of attempts made to download this source during the current update, starting at 1.
	Attempt int

	// Err is the error that caused the attempt to fail.
	// A response with a non-200 status code produces an error that matches *HttpStatusError when checked with errors.As.
	// A download that is not a valid database produces an error that matches ErrNoRanges, ErrDatabaseTooLarge or
	// a parse error, depending on the problem.
	Err error
}

// RetryAction is the action to take after a source fails to download.
type RetryAction int

const (
	// RetryActionSkip gives up on the source for the current update.
	//
	// For range databases with multiple URLs, the database is updated from the remaining URLs,
	// and the ranges from the skipped URL are left out until it works again.
	// For geoIP databases with multiple URLs, the next mirror is tried.
	// If every source of a database is skipped, the update fails and the database keeps its current data.
	//
	// This is the zero value, and what happens if Options.RetryPolicy is nil.
	RetryActionSkip RetryAction = iota

	// RetryActionRetry tries to download the source again after RetryDecision.Delay.
	RetryActionRetry

	// RetryActionFail fails the update of the whole database immediately, without trying its remaining sources.
	// The database keeps its current data, and the update is retried later as for any other failed update.
	RetryActionFail
)

func (a RetryAction) String() string {
	switch a {
	case RetryActionSkip:
		return "skip"
	case RetryActionRetry:
		return "retry"
	case RetryActionFail:
		return "fail"
	default:
		return fmt.Sprintf("RetryAction(%d)", int(a))
	}
}

// RetryDecision is returned by a RetryPolicy to decide what happens after a source fails to download.
type RetryDecision struct {
	// Action is the action to take.
	// Unknown actions are treated as RetryActionSkip.
	Action RetryAction

	// Delay is the time to wait before retrying when Action is RetryActionRetry.
	// The wait is cut short if the Ipdb instance is closed.
	// Ignored for other actions.
	Delay time.Duration
}

// RetryPolicy decides what happens when a source of a database fails to download.
// It is called once for each failed attempt, and may be called concurrently for different databases.
//
// Retries made by the policy all happen within a single update.
// They are separate from the retries of failed updates, which happen with an increasing delay
// that never exceeds the database's DataSource.RefreshInterval.
//
// Example policy that retries server errors twice, and skips anything else (such as a 404):
//
//	func(f ipdb.SourceFailure) ipdb.RetryDecision {
//		statusErr, is := errors.AsType[*ipdb.HttpStatusError](f.Err)
//		if is && statusErr.StatusCode >= 500 && f.Attempt <= 2 {
//			return ipdb.RetryDecision{
//				Action: ipdb.RetryActionRetry,
//				Delay: 10 * time.Second,
//			}
//		}
//		return ipdb.RetryDecision{
//			Action: ipdb.RetryActionSkip,
//		}
//	}
type RetryPolicy func(failure SourceFailure) RetryDecision

// SkipAllSources is the default RetryPolicy.
// It skips any failed sources without retrying.
func SkipAllSources(_ SourceFailure) RetryDecision {
	return RetryDecision{
		Action: RetryActionSkip,
	}
}
