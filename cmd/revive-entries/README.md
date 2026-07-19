# revive-entries

Standalone, offline recovery tool for the 2026-07-19 mass false-verdict
incident: usenet queue entries parked in `State=error` between 05:39 and
05:47 PDT are inspected and, when safely revivable, reset so decypharr's
boot-restore picks them up again on the next daemon start.

## Invocation contract

- Run **on the NAS**, against the decypharr state directory (the directory
  that contains `db/`, `usenet/`, and `config.json`).
- The decypharr daemon **must be stopped** first. There is no lock file; a
  second concurrent writer will corrupt the HYBR log under `<state>/db`.
- Dry-run is the default. Nothing is mutated without `-apply`.
- After an `-apply` run, simply start the daemon: pass-1 of boot-restore
  resumes the entries that were mid post-download action, pass-2 re-queues
  the rest (completed metadata resumes without any network; surviving NZB
  XML re-parses).

```sh
# dry run (report only)
./revive-entries -state /volume1/decypharr

# real run
./revive-entries -state /volume1/decypharr -apply
```

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-state <dir>` | (required) | decypharr state dir containing `db/` and `usenet/` |
| `-apply` | `false` | actually write the resets; default is dry-run |
| `-from <RFC3339>` | `2026-07-19T05:35:00-07:00` | incident window start |
| `-to <RFC3339>` | `2026-07-19T05:50:00-07:00` | incident window end |
| `-max-errors N` | `3` | retry budget for **class B only**: skip B rows with `ErrorCount > N`. No-network classes (A, A-action, A2) are exempt |
| `-tag <string>` | `revived-20260719` | tag appended to revived entries' Tags |

## What it selects

Queue-store entries with protocol `nzb`, `State=error`, `Bad=false`,
`LastErrorTime` inside `[from, to]`, and a `LastError` containing one of the
incident signatures:

- `articles missing on provider: failed to stat segment`
- `no valid file groups found in NZB after`
- `timeout waiting for mount files`
- `failed to process NZB archives`
- `no valid files found in NZB`

The last two cover the dominant incident cohort (1,891 entries): the
archive-processing phase stamped `failed to process nzb: failed to process
NZB archives: no valid files found in NZB` when a collapsed substrate
dropped every file group and the parser swallowed the real cause.

`Bad=true` rows and genuine `NNTP ARTICLE_NOT_FOUND (code 430)` verdicts
outside the window are never touched.

### The ErrorCount cap is class-scoped

`ErrorCount` is **not** part of the base selector. The `-max-errors` budget
is applied after classification, and only to **class B**, whose revival
re-parses the NZB over the network — the one case where a prior-failure
budget still means something. Classes A, A-action and A2 resume from
completed metadata or un-flip a failed meta in place with **zero network
work**, so a retry budget is meaningless for them; worse, every premature
fork.1 boot re-failed the same parked rows and inflated their `ErrorCount`,
so a global cap would silently exclude exactly the entries this tool exists
to save. Capped B rows are listed with decision `skip-max-errors` (counted
under `skipped=` in the census) instead of being silently dropped.

## Classification and reset

- **Class A** — NZB meta decodes with `Status=completed` and a generation
  matching the entry (or either blank).
  - **A-action** (`LastError` shows the mount-files timeout): reset to
    `State=downloading`, `Status=downloaded`, `IsDownloading=true` — the
    exact triple boot pass-1 needs to emit a ResumeAction.
  - **A-queued** (all other A): reset to `State=downloading`,
    `Status=queued`, `IsDownloading=false`, `Progress=0` — boot pass-2
    resumes from the completed metadata without any network access.
- **Class B** — meta missing, still `parsing`, or durably `failed` (the
  archive-processing cohort ran `markAsFailed` despite having no content
  verdict), but the raw NZB XML survives (`entry.Magnet` path, `meta.Path`,
  or a `usenet/nzbs/<id>.*.source` / `.queued` artifact). Same reset as
  A-queued; boot pass-2 re-parses the XML and overwrites the stale meta.
- **Class A2 ("un-flip")** — meta durably `failed` and no XML survives, but
  the meta's `Files` still carry the full parsed segment map from an
  earlier **completed** lifecycle: `markAsFailed` only flips `Status` and
  `FailMessage`, it never clears `Files`. Requires `!IsBad`, no permanently
  failed (`IsDeleted`) file, every file with at least one segment, a
  positive size, and the class-A generation adopt-or-match rule. `-apply`
  restores the meta to `completed` (clearing `FailMessage`) through
  `usenet.RestoreCompleted` — a guarded write under the per-NZB lifecycle
  lock that re-validates all of the above — then stages the entry with the
  A-action triple so the symlink action re-runs from the intact segment
  map. Zero network, zero re-download.
  Near-misses stay class C with a precise reason:
  `skip-meta-failed-bad-or-deleted` (genuine content verdict),
  `skip-meta-failed-empty-files` (files present but segmentless or
  zero-size), `skip-meta-failed-no-xml` (truly empty `Files` AND no XML).
  Note on `skip-meta-failed-no-xml`: the 2026-07-19 cohort lands here
  because the incident's rebuild re-parse persisted the quick-parse NZB —
  which always carries an **empty** file table — over the durable meta
  *before* `markAsFailed` ran, and then deleted the re-staged source on
  failure. The verdict is faithful to the disk: for those rows nothing
  survives to un-flip offline, and the recourse is an arr-side re-grab.
  (The engine no longer re-parses over completed metadata —
  `rebuildQueuedNZBJob` resumes pre-fence completed metas — so this shape
  cannot be produced again.)
- **Class C** — none of the above (also: generation mismatch or
  undecodable meta). Reported only, never mutated. Recourse: re-grab from
  the arr side.

All writes go through the storage package's guarded mutation APIs
(`MutateQueuedIfPresent` / `MutateEntryIfPresent`), so per-key locks are
held and CAS generation/revision tokens advance exactly as the daemon's
own writes would. The selector is re-checked inside the mutation callback,
which also makes the tool idempotent: revived rows no longer match, so a
second run finds nothing.

Never touched: `InfoHash`, `NZBGeneration`, `Providers`, `Files`,
`Magnet`, `SavePath`, `Action`, `CallbackURL`, and the error audit trail
(`LastError`, `LastErrorTime`, `ErrorCount`). If the main entry store also
holds the hash in `State=error`, it receives the same cosmetic reset.

## Output

One TSV line per candidate on stdout
(`hash  name(60)  class  meta-status  xml-present  decision`), where
decision is `would-revive-as-<class>` / `would-unflip` (dry-run),
`revived-as-<class>` / `unflipped` (apply; `+main` when the main-store row
was reset too), or `skip-<reason>`. A `# census:` summary line (including
`A2-unflip=N`) closes the report.

stdout carries **only** the `# hash...` header, TSV rows, and trailing `#`
census/comment lines — safe to redirect straight into a file or pipe.
Every banner, warning, and dependency log line (config loading, storage
init) goes to stderr: the tool points the process-global `os.Stdout` at
stderr while the stores are open, so writers that grab `os.Stdout` at
construction time (zerolog console writers, `fmt.Printf` in config
loading) can never leak into the TSV stream.

Exit codes: `0` success, `1` the state dir looks wrong (no `db/`) or an
operational failure, `2` zero candidates matched (distinct so operator
scripts can tell "already done" from "worked").
