package types

type Error struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e *Error) Error() string {
	return e.Message
}

var NoActiveAccountsError = &Error{
	Message: "No active accounts",
	Code:    "no_active_accounts",
}

var ErrDownloadLinkNotFound = &Error{
	Message: "No download link found",
	Code:    "no_download_link",
}

var DownloadLinkExpiredError = &Error{
	Message: "Download link expired",
	Code:    "download_link_expired",
}

var EmptyDownloadLinkError = &Error{
	Message: "Download link is empty",
	Code:    "empty_download_link",
}

var InvalidDownloadLinkError = &Error{
	Message: "Download link is invalid",
	Code:    "invalid_download_link",
}

// ErrAvailabilityIndeterminate means a debrid CheckFile probe could not reach a
// verdict: the provider was unreachable, rate limited, rejected our credentials,
// or answered with something we cannot interpret (5xx, malformed body, transport
// failure). It is the third arm of the CheckFile contract:
//
//	nil                                -> the file is available
//	customerror.HosterUnavailableError -> the file is definitively gone
//	ErrAvailabilityIndeterminate       -> unknown; the check told us nothing
//
// It is the debrid analog of usenet.ErrAvailabilityIndeterminate and carries the
// same rule: callers MUST NOT treat it as available and MUST NOT treat it as
// dead. A definitively-dead verdict can trigger destructive repair actions, so
// an outage or a 429 must never be laundered into one — but it must equally
// never be scored healthy. Providers wrap it with %w so the cause survives:
//
//	fmt.Errorf("%w: ...: %w", types.ErrAvailabilityIndeterminate, err)
var ErrAvailabilityIndeterminate = &Error{
	Message: "Availability check indeterminate",
	Code:    "availability_indeterminate",
}
