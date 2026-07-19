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
| `-max-errors N` | `3` | skip entries with `ErrorCount > N` |
| `-tag <string>` | `revived-20260719` | tag appended to revived entries' Tags |

## What it selects

Queue-store entries with protocol `nzb`, `State=error`, `Bad=false`,
`ErrorCount <= max-errors`, `LastErrorTime` inside `[from, to]`, and a
`LastError` containing one of the incident signatures:

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
- **Class C** — neither (also: generation mismatch or undecodable meta).
  Reported only, never mutated. Recourse: re-grab from the arr side.

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
decision is `would-revive-as-<class>` (dry-run), `revived-as-<class>`
(apply; `+main` when the main-store row was reset too), or
`skip-<reason>`. A `# census:` summary line closes the report. The storage
layer prints its own startup log lines to stdout as well; TSV consumers
should keep only lines starting with a hash or drop `#`/log lines.

Exit codes: `0` success, `1` the state dir looks wrong (no `db/`) or an
operational failure, `2` zero candidates matched (distinct so operator
scripts can tell "already done" from "worked").
