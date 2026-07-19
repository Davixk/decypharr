// Command revive-entries is a standalone, offline recovery tool for the
// 2026-07-19 mass false-verdict incident. It runs on the NAS against the
// decypharr state directory while the daemon is STOPPED, selects usenet queue
// entries that were parked in State=error inside the incident window, and
// resets the revivable ones so the engine's boot-restore
// (restoreActiveDownloadJobs, pkg/manager/active_queue.go) picks them up again.
//
// It is conservative by design:
//   - dry-run by default (-apply required to mutate anything),
//   - every mutation goes through the storage package's guarded mutation APIs
//     (MutateQueuedIfPresent / MutateEntryIfPresent) so per-key locks are held
//     and CAS generation/revision tokens advance exactly like the daemon's own
//     writes,
//   - the selector is re-evaluated inside the mutation callback, so a row that
//     changed between scan and write is skipped, never clobbered,
//   - the error audit trail (LastError, LastErrorTime, ErrorCount) is left
//     intact and a tag is appended so revived entries stay identifiable.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// Incident error signatures. An entry is a candidate only when its LastError
// contains one of the three revivable patterns.
const (
	patternStatSegment = "articles missing on provider: failed to stat segment"
	patternNoGroups    = "no valid file groups found in NZB after"
	patternMountFiles  = "timeout waiting for mount files"

	// pattern430 marks a genuine provider-side article-not-found verdict.
	// Outside the incident window it is trusted and never revived.
	pattern430 = "NNTP ARTICLE_NOT_FOUND (code 430)"
)

// Incident window defaults (2026-07-19 05:39-05:47 PDT plus margin).
const (
	defaultFrom = "2026-07-19T05:35:00-07:00"
	defaultTo   = "2026-07-19T05:50:00-07:00"

	defaultTag       = "revived-20260719"
	defaultMaxErrors = 3
)

// Exit codes (distinct so operator scripts can tell the cases apart).
const (
	exitOK            = 0
	exitBadState      = 1
	exitZeroCandidate = 2
)

type classification string

const (
	classAAction classification = "A-action" // completed meta, was mid post-download action
	classAQueued classification = "A-queued" // completed meta, resumes via no-network ResumeExisting
	classB       classification = "B"        // meta missing/parsing but the XML source survives
	classC       classification = "C"        // not revivable offline; recourse: arr-side re-grab
)

type options struct {
	stateDir  string
	apply     bool
	from      time.Time
	to        time.Time
	maxErrors int
	tag       string
}

type census struct {
	candidates int
	aAction    int
	aQueued    int
	b          int
	c          int
	revived    int
	skipped    int // stale rows, vanished rows, mutation errors
	mainResets int
}

type verdict struct {
	class      classification
	metaStatus string
	xmlPresent bool
	reason     string // populated for class C
}

func main() {
	fs := flag.NewFlagSet("revive-entries", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	stateDir := fs.String("state", "", "decypharr state dir containing db/ and usenet/ (required)")
	apply := fs.Bool("apply", false, "apply the resets (default is dry-run: report only)")
	fromStr := fs.String("from", defaultFrom, "incident window start (RFC3339)")
	toStr := fs.String("to", defaultTo, "incident window end (RFC3339)")
	maxErrors := fs.Int("max-errors", defaultMaxErrors, "maximum ErrorCount for a revivable entry")
	tag := fs.String("tag", defaultTag, "tag appended to Tags of revived entries")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(exitBadState)
	}

	if *stateDir == "" {
		fmt.Fprintln(os.Stderr, "error: -state <dir> is required")
		fs.Usage()
		os.Exit(exitBadState)
	}
	from, err := time.Parse(time.RFC3339, *fromStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid -from %q: %v\n", *fromStr, err)
		os.Exit(exitBadState)
	}
	to, err := time.Parse(time.RFC3339, *toStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid -to %q: %v\n", *toStr, err)
		os.Exit(exitBadState)
	}
	if to.Before(from) {
		fmt.Fprintln(os.Stderr, "error: -to is before -from")
		os.Exit(exitBadState)
	}

	os.Exit(run(options{
		stateDir:  *stateDir,
		apply:     *apply,
		from:      from,
		to:        to,
		maxErrors: *maxErrors,
		tag:       *tag,
	}, os.Stdout, os.Stderr))
}

func run(opts options, stdout, stderr io.Writer) int {
	mode := "DRY-RUN (no changes will be written; pass -apply to reset)"
	if opts.apply {
		mode = "APPLY (queue and main rows WILL be mutated)"
	}
	fmt.Fprintf(stderr, `
============================================================================
  revive-entries — offline recovery for error-parked usenet entries
----------------------------------------------------------------------------
  !!! THE DECYPHARR DAEMON MUST BE STOPPED BEFORE RUNNING THIS TOOL !!!
  There is NO lock file. A second concurrent writer WILL corrupt the
  HYBR log under %s.
----------------------------------------------------------------------------
  Mode:   %s
  Window: %s .. %s
============================================================================
`, filepath.Join(opts.stateDir, "db"), mode,
		opts.from.Format(time.RFC3339), opts.to.Format(time.RFC3339))

	dbDir := filepath.Join(opts.stateDir, "db")
	if st, err := os.Stat(dbDir); err != nil || !st.IsDir() {
		fmt.Fprintf(stderr, "error: %s does not look like a decypharr state dir (missing db/)\n", opts.stateDir)
		return exitBadState
	}
	if _, err := os.Stat(filepath.Join(opts.stateDir, "config.json")); err != nil {
		fmt.Fprintf(stderr, "warning: %s has no config.json; a default one will be created (folder-naming dependent metadata may differ from production)\n", opts.stateDir)
	}

	// The storage, logger, and NZB-metadata layers all resolve their paths
	// (and FolderNaming, used by queue-row index metadata) from the global
	// config path, exactly like the daemon does.
	config.SetConfigPath(opts.stateDir)

	store, err := storage.NewStorage(dbDir)
	if err != nil {
		fmt.Fprintf(stderr, "error: open storage: %v\n", err)
		return exitBadState
	}
	// Close is mandatory: it flushes the hybrid store's sync loop to disk.
	defer func() {
		if cerr := store.Close(); cerr != nil {
			fmt.Fprintf(stderr, "error: closing storage: %v\n", cerr)
		}
	}()

	nzbs, err := usenet.NewNZBStorage()
	if err != nil {
		fmt.Fprintf(stderr, "error: open NZB metadata storage: %v\n", err)
		return exitBadState
	}

	candidates, err := store.FilterQueued(func(e *storage.Entry) bool {
		return matchesSelector(e, opts.from, opts.to, opts.maxErrors)
	})
	if err != nil {
		fmt.Fprintf(stderr, "error: scanning queue store: %v\n", err)
		return exitBadState
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].InfoHash < candidates[j].InfoHash })

	cens := census{candidates: len(candidates)}
	fmt.Fprintln(stdout, "# hash\tname\tclass\tmeta-status\txml-present\tdecision")

	for _, entry := range candidates {
		v := classify(opts.stateDir, nzbs, entry)
		decision := ""

		switch v.class {
		case classC:
			cens.c++
			decision = "skip-" + v.reason
		default:
			switch v.class {
			case classAAction:
				cens.aAction++
			case classAQueued:
				cens.aQueued++
			case classB:
				cens.b++
			}
			if !opts.apply {
				decision = "would-revive-as-" + string(v.class)
				if mainRowInError(store, entry.InfoHash) {
					decision += "+main"
				}
			} else {
				revived, mainReset, rerr := revive(store, entry.InfoHash, v.class, opts)
				switch {
				case rerr != nil && !revived:
					cens.skipped++
					decision = "skip-mutate-error"
					fmt.Fprintf(stderr, "warning: %s: queue mutation failed: %v\n", entry.InfoHash, rerr)
				case rerr != nil && revived:
					// Queue row is reset; only the cosmetic main-store pass failed.
					cens.revived++
					decision = "revived-as-" + string(v.class)
					fmt.Fprintf(stderr, "warning: %s: main-store reset failed (queue row already revived): %v\n", entry.InfoHash, rerr)
				case !revived:
					cens.skipped++
					decision = "skip-stale"
				default:
					cens.revived++
					decision = "revived-as-" + string(v.class)
					if mainReset {
						cens.mainResets++
						decision += "+main"
					}
				}
			}
		}

		fmt.Fprintf(stdout, "%s\t%s\t%s\t%s\t%v\t%s\n",
			entry.InfoHash, truncateName(entry.Name, 60), v.class, v.metaStatus, v.xmlPresent, decision)
	}

	fmt.Fprintf(stdout, "# census: candidates=%d A-action=%d A-queued=%d B=%d C=%d revived=%d skipped=%d main-store-resets=%d mode=%s\n",
		cens.candidates, cens.aAction, cens.aQueued, cens.b, cens.c, cens.revived, cens.skipped, cens.mainResets,
		map[bool]string{true: "apply", false: "dry-run"}[opts.apply])
	if cens.c > 0 {
		fmt.Fprintf(stdout, "# %d class-C entries are not revivable offline; recourse: arr-side re-grab\n", cens.c)
	}

	if cens.candidates == 0 {
		fmt.Fprintln(stderr, "no candidates matched the selector (nothing to do)")
		return exitZeroCandidate
	}
	return exitOK
}

// matchesSelector reports whether a queue row is an incident candidate. It is
// evaluated both during the scan and again inside the guarded mutation
// callback, which is what makes revived rows drop out of a second run.
func matchesSelector(e *storage.Entry, from, to time.Time, maxErrors int) bool {
	if e == nil || !e.IsNZB() {
		return false
	}
	if e.State != storage.EntryStateError {
		return false
	}
	if e.Bad {
		return false
	}
	if e.ErrorCount > maxErrors {
		return false
	}
	if e.LastErrorTime == nil {
		return false
	}
	t := *e.LastErrorTime
	inWindow := !t.Before(from) && !t.After(to)
	if !inWindow {
		// This also covers the explicit exclusion of genuine
		// "NNTP ARTICLE_NOT_FOUND (code 430)" verdicts outside the window.
		return false
	}
	return strings.Contains(e.LastError, patternStatSegment) ||
		strings.Contains(e.LastError, patternNoGroups) ||
		strings.Contains(e.LastError, patternMountFiles)
}

// classify decides how (and whether) a candidate can be revived, mirroring the
// checks boot-restore will make:
//
//   - Class A: NZB meta decodes with Status=completed and a generation that
//     matches the entry (or either side blank). A-action rows were mid
//     post-download action ("timeout waiting for mount files") and resume via
//     pass-1 ResumeAction; all other A rows go through pass-2's no-network
//     ResumeExisting branch.
//   - Class B: meta missing or still parsing, but the raw NZB XML source
//     survives (entry.Magnet path, meta.Path, or a usenet/nzbs/<id>.*.source
//     or .queued artifact) so pass-2 can re-parse it.
//   - Class C: neither. A generation mismatch or an undecodable meta blob is
//     also class C: the stale meta blocks both restore branches at boot.
func classify(stateDir string, nzbs *usenet.NZBStorage, e *storage.Entry) verdict {
	var meta *storage.NZB
	metaStatus := "missing"
	if m, err := nzbs.GetNZBHeader(e.InfoHash); err == nil {
		meta = m
		metaStatus = m.Status
		if metaStatus == "" {
			metaStatus = "(blank)"
		}
	} else if !errors.Is(err, usenet.ErrNZBNotFound) {
		return verdict{class: classC, metaStatus: "decode-error", xmlPresent: xmlSourcePresent(stateDir, e, nil), reason: "meta-decode-error"}
	}

	xml := xmlSourcePresent(stateDir, e, meta)

	if meta != nil {
		if meta.Generation != "" && e.NZBGeneration != "" && meta.Generation != e.NZBGeneration {
			return verdict{class: classC, metaStatus: metaStatus, xmlPresent: xml, reason: "generation-mismatch"}
		}
		switch meta.Status {
		case usenet.NZBStatusCompleted:
			if strings.Contains(e.LastError, patternMountFiles) {
				return verdict{class: classAAction, metaStatus: metaStatus, xmlPresent: xml}
			}
			return verdict{class: classAQueued, metaStatus: metaStatus, xmlPresent: xml}
		case usenet.NZBStatusParsing:
			if xml {
				return verdict{class: classB, metaStatus: metaStatus, xmlPresent: xml}
			}
			return verdict{class: classC, metaStatus: metaStatus, xmlPresent: xml, reason: "meta-parsing-no-xml"}
		default:
			return verdict{class: classC, metaStatus: metaStatus, xmlPresent: xml, reason: "meta-status-" + metaStatus}
		}
	}

	if xml {
		return verdict{class: classB, metaStatus: metaStatus, xmlPresent: xml}
	}
	return verdict{class: classC, metaStatus: metaStatus, xmlPresent: false, reason: "no-meta-no-xml"}
}

// xmlSourcePresent reports whether the raw NZB XML survives anywhere
// boot-restore's rebuild path can reach it.
func xmlSourcePresent(stateDir string, e *storage.Entry, meta *storage.NZB) bool {
	if e.Magnet != "" && fileExists(e.Magnet) {
		return true
	}
	if meta != nil && meta.Path != "" && fileExists(meta.Path) {
		return true
	}
	nzbDir := filepath.Join(stateDir, "usenet", "nzbs")
	for _, suffix := range []string{".source", ".queued"} {
		matches, err := filepath.Glob(filepath.Join(nzbDir, e.InfoHash+".*"+suffix))
		if err == nil && len(matches) > 0 {
			return true
		}
	}
	return false
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// resetEntryFields applies exactly the class-specific reset. It never touches
// InfoHash, NZBGeneration, Providers, Files, Magnet, SavePath, Action,
// CallbackURL, LastError, LastErrorTime, or ErrorCount.
//
//   - A-action: the pass-1 ResumeAction triple required by resumeClaimedAction
//     (State=downloading + Status=downloaded + IsDownloading=true).
//   - A-queued / B: the pass-2 rebuild shape (State=downloading +
//     Status=queued + IsDownloading=false + Progress=0).
func resetEntryFields(e *storage.Entry, cls classification, tag string) {
	e.State = storage.EntryStateDownloading
	if cls == classAAction {
		e.Status = debridTypes.TorrentStatusDownloaded
		e.IsDownloading = true
	} else {
		e.Status = debridTypes.TorrentStatusQueued
		e.IsDownloading = false
		e.Progress = 0
	}
	if tag != "" && !slices.Contains(e.Tags, tag) {
		e.Tags = append(e.Tags, tag)
	}
}

// revive resets the queue row through the guarded mutation API and, when the
// main store also holds the hash in State=error, applies the same cosmetic
// reset there. The selector is re-checked on the freshly loaded row so a row
// that changed since the scan is skipped rather than overwritten.
func revive(store *storage.Storage, hash string, cls classification, opts options) (revived, mainReset bool, err error) {
	stale := false
	_, present, err := store.MutateQueuedIfPresent(hash, func(cur *storage.Entry) (bool, error) {
		if !matchesSelector(cur, opts.from, opts.to, opts.maxErrors) {
			stale = true
			return false, nil
		}
		resetEntryFields(cur, cls, opts.tag)
		return true, nil
	})
	if err != nil {
		return false, false, err
	}
	if !present || stale {
		return false, false, nil
	}

	// Cosmetic consistency for the main store: only rows that mirror the error
	// state are touched; absent rows are skipped.
	_, _, merr := store.MutateEntryIfPresent(hash, func(cur *storage.Entry) (bool, error) {
		if cur.State != storage.EntryStateError {
			return false, nil
		}
		resetEntryFields(cur, cls, opts.tag)
		mainReset = true
		return true, nil
	})
	if merr != nil {
		return true, false, merr
	}
	return true, mainReset, nil
}

// mainRowInError reports (for dry-run output) whether the main store mirrors
// the error state and would also be reset by -apply.
func mainRowInError(store *storage.Storage, hash string) bool {
	e, err := store.Get(hash)
	return err == nil && e != nil && e.State == storage.EntryStateError
}

func truncateName(name string, maxRunes int) string {
	name = strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, name)
	runes := []rune(name)
	if len(runes) <= maxRunes {
		return name
	}
	return string(runes[:maxRunes])
}
