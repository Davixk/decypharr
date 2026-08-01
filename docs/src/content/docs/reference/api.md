---
title: API Reference
description: Every route registered by this build, with request/response shapes and destructive-action markers.
---

Written by reading `pkg/server/routes.go`, `pkg/server/server.go`, `pkg/server/api.go` and
`pkg/server/webhook.go`. This documents **what the code does**, not what would be reasonable.
There is no OpenAPI/spec endpoint on this build; `routes.go` is the authority.

## Base path

Every path below is relative to `url_base` (config `url_base`, env `URL_BASE`; default `/`).
With `url_base: /decypharr/`, `GET /api/repair/status` becomes `GET /decypharr/api/repair/status`.
Default listen address is `bind_address:port` (defaults `0.0.0.0:8282`).

Global middleware: `Recoverer`, `StripSlashes`, `RedirectSlashes` — trailing slashes are stripped,
so `/api/browse` and `/api/browse/` are the same route.

## Auth

| Surface | Scheme |
|---|---|
| `/api/*`, web pages, `/logs`, `/debug/*` | `Authorization: Bearer <token>` **or** `Authorization: Token <token>`, **or** the `auth-session` cookie |
| `/webhooks/*` | The same API token, via `Authorization: Bearer\|Token <token>`, the `X-API-Token` header, or a `?token=` / `?apikey=` query parameter (for senders that can only append to the URL). No session cookie. |
| `/webdav/*` | HTTP Basic — **only enforced when `use_auth` AND `enable_webdav_auth` are both true** |
| `/api/v2/*` (qBittorrent emu) | Basic auth / `SID` cookie carrying an **\*arr host + API key**, not the decypharr API token |
| `/sabnzbd/api` | `?ma_username=<arr host>&ma_password=<arr key>` query params |

Get the token from `GET /api/config` → `api_token`, or the Settings → Auth page.

:::caution[Auth can be globally off]
Every auth path returns early when `use_auth == false`. If auth was skipped in the setup wizard
(`POST /skip-auth`) or disabled via `POST /api/update-auth` with an empty username *and* password,
**every** endpoint below — including all destructive ones — is open to anyone who can reach the port.
:::

**Auth failure shape differs by path.** Requests whose path starts with `/api/`, and all
`/webhooks/*` requests, get `401 {"error": "...", "status": 401}`. Everything else (`/logs`,
`/debug/*`, web pages) gets a `303` redirect to `/login` — so `curl -L` on `/debug/logs` silently
returns an HTML login page with status `200`, not an error.

**Setup gate.** Until `SetupComplete()` validates, all paths *except* `/setup`, `/api/setup*`,
`/api/config*`, `/assets*`, `/images*`, `/version` return `503` (JSON for `/api/`, else `303` to
`/setup`). (`setupRedirectMiddleware` also exempts `/api/login` and `/api/logout`, which are not
registered routes on this build — dead entries.) `/webhooks/*` is mounted outside that middleware,
so it is not setup-gated — but it *is* authenticated.

## Request bodies

**Bodies are optional only where marked OPTIONAL below; every other body-taking endpoint requires
one** (they decode straight from `r.Body`, so an absent body yields `EOF` → `400`).

Only three endpoints accept an absent body, via `decodeOptionalJSONBody`:
`POST /api/repair/run`, `POST /api/repair/fix`, `POST /api/repair/health/{name}/check`
(and no other). For those, an absent / empty / whitespace-only body means "apply defaults" —
see each row for what the defaults *do*. A body that is **present but malformed** (e.g. `{`)
is rejected with `400`; it is never silently treated as absent.

---

## Safety

These endpoints can delete data, mutate an \*arr, or launch work that does:

| Endpoint | What it can destroy |
|---|---|
| `POST /api/repair/run` | Full sweep. With no body, uses the configured REPAIR/PRUNE/RE-GRAB knobs — destructive if `prune` or `regrab` is on in config. |
| `POST /api/repair/fix` | With no body / no `names`, acts on **every** entry currently marked broken, using the configured knobs. |
| `POST /api/repair/recheck/media` | `fix:true` or an `actions` selection can PRUNE/RE-GRAB the resolved media. Bypasses the per-run deletion cap. |
| `POST /api/repair/health/{name}/check` | `?fix=true` or an `actions` selection can PRUNE/RE-GRAB that entry. Bypasses the per-run deletion cap. |
| `POST /webhooks/tautulli` | Authenticated. Runs a targeted media recheck; `fix:true` maps to the configured knobs, so it can PRUNE/RE-GRAB **that media**. It cannot launch a library-wide sweep. |
| `DELETE /api/torrents/{category}/{hash}` | Removes the queue entry; with `?removeFromDebrid=true` also deletes provider placements + files. |
| `DELETE /api/torrents?hashes=…` | Same, in bulk. |
| `DELETE /api/browse/torrents/{id}` | **Always** deletes provider placements + files (the flag is hardcoded `true`; there is no opt-out). |
| `DELETE /api/browse/torrents/batch` | Same, in bulk. |
| `DELETE /webdav/{group}/{torrent}` · `MOVE /webdav/...` | Deletes the entry (placements + files) / moves it. |
| `POST /api/mount/cache/purge` | Purges the whole DFS mount cache. |
| `POST /api/mount/cache/cleanup` | Evicts expired DFS cache entries. |
| `PATCH /api/config` · `PATCH /api/repair/config` | Rewrites config on disk. **Merges recursively** — a key absent from the body keeps its current value at every nesting level. Submitted arrays/maps still replace wholesale. |
| `PUT /api/config` · `PUT /api/repair/config` | Rewrites config on disk with the submitted document. **Everything you omit is cleared** — including `repair.max_deletions_per_run`, `stop_schedule`, `prune` and `regrab`. See the note under "Config and auth management" for why that cannot make a config *more* destructive. |
| `POST /api/update-auth` | Empty username + password **disables authentication entirely**. |
| `POST /api/refresh-token` | Invalidates the current API token immediately. |
| `POST /api/setup/complete` · `POST /skip-auth` | Writes config + restarts (pre-setup only; `403` after setup). |
| `POST /api/repair/clear-state` · `DELETE /api/repair/runs` | Deletes health/run **metadata** only — no files, \*arrs, or placements touched. |
| `POST /api/v2/torrents/delete` | qBittorrent-emulation delete. |
| `GET /api/v2/app/shutdown` | Shuts the process down. |

Everything not listed is read-only or non-destructive.

---

## Repair — the action model

Read this before using `/api/repair/*`.

Four components. **CHECK** (enumerate + probe + record health) always runs and is not gated.
The other three are **independent and compose** — they are *not* mutually exclusive, and there is
no master on/off gate:

| Component | Effect | \*arr? | Destructive |
|---|---|---|---|
| `repair` | Re-acquire the dead item from the same provider, falling back to other configured debrids. | no | **No** |
| `prune` | Delete decypharr-side **only**: provider placements + symlink/download folder + db entry. **Never calls the \*arr — and do not expect the \*arr to notice.** Its file record still points at the now-dangling symlink, and `MediaFileTableCleanupService` compares paths without stat-ing them, so the record is kept and nothing is re-searched. PRUNE is the component that leaves the \*arr alone. | no | **Yes** |
| `arr_delete` | Through the \*arr: delete the file record. **That is all it does by itself** — `arr_search` and `arr_blocklist` are separate, default-off sub-actions, both subordinate to this one. The only \*arr-coupled component. Formerly `regrab`, still accepted as an alias. | yes | **Yes** |
| ↳ `arr_search` | Sub-action of `arr_delete`: ask the \*arr for a replacement after the delete. Off by default; does nothing on its own. | yes | no |
| ↳ `arr_blocklist` | Sub-action of `arr_delete`: mark the grab failed, which bans the release **globally and permanently**. Off by default; does nothing on its own. Reserve for bad releases, not missing bytes. | yes | no |

Config knobs (`repair.repair` / `repair.arr_delete` / `repair.prune`, in the order a sweep runs
them): `repair` is a `*bool` that **defaults to `true` when unset**; `arr_delete` and `prune`
default to `false`, as do the two sub-actions. So out of the box a sweep detects and re-acquires,
and deletes nothing.

:::note[`repair.auto_repair` is DEPRECATED and ignored]
The old `auto_repair` **config** field no longer gates anything — the resolver never reads it. It is
retained only so pre-split configs still round-trip through JSON. Use `repair` / `prune` / `regrab`.
(The `auto_repair` *body/query* field on `POST /api/repair/run` is a different thing: it is still
honored there as a back-compat shim for old clients, as described below.)
:::

### How a request picks components

Two endpoint families, two spellings of the same idea.

**`POST /api/repair/run`** builds the selection from the `repair` / `prune` / `regrab` keys
(JSON body or query param), each a tri-state (absent = unset):

- **Any one of the three present** → explicit override; **the two you omitted are forced to `false`**.
  `{"prune": true}` means *prune only, with REPAIR turned off for this run*.
- **All three absent** → fall back to the configured knobs.
- All three present and `false` → **CHECK-only** (probe and record, act on nothing).
- Deprecated `auto_repair` body/query flag: `false` → CHECK-only; `true` → same as omitting
  (configured knobs). Ignored if any of the three are present.

**`POST /api/repair/fix`, `POST /api/repair/recheck/media`, `POST /api/repair/health/{name}/check`**
use an `actions` object plus a legacy `fix` flag:

- `actions` naming **≥1 true** component → exactly those components (single-component works, e.g. PRUNE-only).
- `actions` **absent** → falls through to the `fix` flag:
  - `fix` true → **the configured knobs** (not force-all). On `/api/repair/fix` this branch always applies.
  - `fix` false → CHECK-only.
- `actions` **present with all three `false`** → an explicit "no components":
  - on `recheck/media` and `health/{name}/check` → **CHECK-only**, regardless of `fix` / `?fix=true`;
  - on `/api/repair/fix` → **`400 "no repair action selected: enable REPAIR, PRUNE, or RE-GRAB"`**,
    since that endpoint has nothing to do without a component (it never re-probes).

:::tip[An all-false `actions` means "do nothing", everywhere]
`{"actions":{"repair":false,"prune":false,"regrab":false}}` is honored as written: CHECK-only on the
recheck endpoints, a `400` on `/api/repair/fix`. It is **not** promoted to the configured knobs — the
same JSON on `POST /api/repair/run` has always yielded CHECK-only, and the `actions`-style endpoints
now agree. Only an **absent** `actions` object falls back to the configured knobs.
:::

### Concurrency and caps

- One repair run at a time. `run`, `fix`, `recheck/media` and `clear-state` return **`409`**
  (`"repair already running (run …)"`) while one is active. `POST /api/repair/health/{name}/check`
  does **not** take the singleton lock — it only rejects if *that entry* is currently being probed.
- Destructive components consume a per-run deletion budget: `repair.max_deletions_per_run`
  (`0`/unset → **100**, negative → unlimited). Applies to sweeps and to `/api/repair/fix`.
  **Not** applied to `recheck/media` or `health/{name}/check` — both run with an unlimited budget.
- `POST /api/repair/run` works **even when `repair.enabled` is `false`** in config. The `enabled`
  check only blocks the *scheduled* trigger.

---

## Setup and auth (public)

| Method | Path | Does | Body | Safety |
|---|---|---|---|---|
| GET | `/version` | Build/version info. Public even pre-setup. | — | SAFE |
| GET | `/setup` | Setup wizard HTML. `303` to `/` once setup is complete. | — | SAFE |
| POST | `/api/setup/complete` | One-shot wizard submit: writes auth, debrid, usenet, download folder, mount config; then restarts. `403` if setup already complete. | **REQUIRED** — `{auth:{username,password,skip_auth}, debrid:{provider,api_key,skip_debrid}, usenet:{host,port,username,password,max_connections,reader_connections,ssl,skip_usenet}, download:{download_folder}, mount:{mount_type,mount_path,cache_dir,rclone_buffer_size}}`. Requires ≥1 of debrid/usenet configured and a non-empty `download_folder`. `provider` ∈ realdebrid, alldebrid, debridlink, torbox, premiumize. `mount_type` ∈ `dfs`, `rclone`. | **DESTRUCTIVE** (rewrites config, restarts) |
| GET/POST | `/login` | GET renders HTML. POST takes `{"username","password"}` JSON, sets the `auth-session` cookie, `303` to `/`; `401` on bad creds. | REQUIRED (POST) | SAFE |
| GET/POST | `/register` | GET renders HTML. POST takes **form** fields `username`, `password`, `confirmPassword`; writes credentials and logs in. | REQUIRED (POST, form) | SAFE-ish (sets credentials) |
| POST | `/skip-auth` | Sets `use_auth=false` and saves. `403` once setup is complete. | — | **DESTRUCTIVE** (disables auth) |
| GET | `/assets/*`, `/images/*` | Embedded static files. | — | SAFE |

## Web pages (auth required, HTML)

`GET /` · `/browse` · `/download` · `/repair` · `/stats` · `/settings` — server-rendered pages.
Not useful from curl; unauthenticated requests `303` to `/login`.

## Config and auth management

| Method | Path | Does | Body | Response / Safety |
|---|---|---|---|---|
| GET | `/api/config` | Full `config.Config` plus `api_token` and `auth_username`. Exempt from the setup gate. | — | Config JSON. SAFE (**leaks the API token and all provider keys**) |
| PATCH | `/api/config` | **Partial update.** A key absent from the submitted JSON keeps its current value at *every* nesting level, not just the top (`PreserveMissingSections`). So `{"repair":{"enabled":true}}` changes only `repair.enabled` and leaves `max_deletions_per_run`, `stop_schedule`, `prune`, `regrab` — and the `repair` tri-state — exactly as stored. Explicitly submitted values still overwrite, including empty/zero/false ones (`"download_folder": ""`, `"repair":{"prune":false}`). **Arrays and maps are not element-merged**: a submitted `debrids` / `arrs` / `usenet.providers` / `repair.arrs` / `custom_folders` is the caller's complete list and **replaces wholesale** (`"debrids": []` still clears); an omitted one is preserved. A submitted `null` for a section clears it. | REQUIRED — a partial `config.Config` object | `{"status":"success","restarted":bool}`. **DESTRUCTIVE** (writes config) |
| PUT | `/api/config` | **Full replacement.** The submitted document *is* the new config: **every key you omit reverts to its zero value**, then the normal save-time defaults are applied (so e.g. an omitted `download_folder` becomes `<config dir>/downloads`, an omitted `repair.source` becomes `arr`). Omitting `debrids` clears every provider. Submitted values behave exactly as under PATCH. | REQUIRED — a complete `config.Config` object | Same as PATCH. **DESTRUCTIVE** (writes config, clears omissions) |
| POST | `/api/refresh-token` | Generates a new 256-bit API token and saves it. | — | `{"token","message"}` — note the key is **`token`**, not `api_token`. **DESTRUCTIVE** (old token stops working immediately) |
| POST | `/api/update-auth` | Set or clear credentials. Empty `username` **and** `password` → sets `use_auth=false` and clears the stored credentials. Otherwise both are required and `password` must equal `confirm_password`. | REQUIRED — `{"username":string,"password":string,"confirm_password":string}` | `{"message"}`. **DESTRUCTIVE** |

Both verbs share one implementation — they differ *only* in whether the merge step runs — so the
rest is identical for each: `auth`, `use_auth` and `enable_webdav_auth` are always preserved from
the live config (a PUT cannot disable authentication by omission), \*arrs missing
`name`/`host`/`token` are dropped, an invalid \*arr host is a `400`, and the server restarts only
if bind/debrid/usenet/mount changed, else applies live. A missing or malformed body is a `400` on
both — it is never treated as an empty document.

:::caution[What `PUT` does to the repair safety settings]
`repair.max_deletions_per_run`, `stop_schedule`, `prune` and `regrab` have **no save-time default**,
so a `PUT` that omits them clears them outright: `0`, `""`, `false`, `false`. **A `PUT` can drop a
deletion cap you deliberately tightened.** What it cannot do is make the configuration *more*
destructive, because each of those zero values is the conservative one — `max_deletions_per_run: 0`
resolves to the default cap of **100** (unlimited is `-1`), `prune`/`regrab` `false` delete nothing,
and an absent `repair` tri-state resolves to `true` (re-acquire, non-destructive). `stop_schedule: ""`
only means the sweep runs to completion.

If you want to change one setting without restating the rest, use **`PATCH`**. That is what the
Settings page does.
:::

```bash
# PATCH — nested partial update: only repair.enabled changes.
# max_deletions_per_run, stop_schedule, prune, regrab and the repair tri-state
# keep their stored values.
curl -X PATCH -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"repair":{"enabled":true}}' \
  http://localhost:8282/api/config

# Lists replace, they are not element-merged: this leaves ONE debrid configured.
curl -X PATCH -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"debrids":[{"name":"rd","provider":"realdebrid","api_key":"KEY"}]}' \
  http://localhost:8282/api/config

# PUT — full replacement. This is NOT "set the log level": it also clears
# debrids, arrs, the repair block and everything else you did not send.
curl -X PUT -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"log_level":"debug"}' \
  http://localhost:8282/api/config
```

## \*arrs and imports

| Method | Path | Does | Body | Response / Safety |
|---|---|---|---|---|
| GET | `/api/arrs` | All known \*arrs (config + auto-detected). | — | Array of `{name,host,token,type,cleanup,skip_repair,download_uncached,selected_debrid,fallback_on_failure,source}` — **includes \*arr API keys**. SAFE |
| POST | `/api/add` | Queue torrents and/or NZBs for import. | **REQUIRED, `multipart/form-data` only** — a JSON or urlencoded body returns `400` (`ParseMultipartForm` → `ErrNotMultipart`). Fields: `urls` (newline-separated magnets/URLs), `nzbURLs` (newline-separated), file parts `files` (.torrent) and `nzbFiles` (.nzb), plus `arr`, `action`, `debrid`, `callbackUrl`, `downloadFolder` (defaults to config `download_folder`), `skipMultiSeason`, `downloadUncached`, `rmTrackerUrls` (all booleans as the literal string `"true"`). An unknown `arr` name creates a throwaway \*arr rather than erroring. | Array of import records `{id,name,status,error,debrid,action,…}`; always `200` even when individual items have `"status":"error"`. SAFE (adds data) |

```bash
# Correct: multipart. A JSON body is rejected with 400.
curl -X POST -H "Authorization: Bearer TOKEN" \
  -F "urls=magnet:?xt=urn:btih:..." \
  -F "arr=sonarr" -F "action=symlink" \
  http://localhost:8282/api/add

curl -X POST -H "Authorization: Bearer TOKEN" \
  -F "files=@episode.torrent" -F "arr=sonarr" \
  http://localhost:8282/api/add
```

## Torrents (queue)

| Method | Path | Does | Query | Response / Safety |
|---|---|---|---|---|
| GET | `/api/torrents` | Paginated queue listing. | `page` (≥1, default 1), `limit` (1–100, default 20; out-of-range silently → 20), `search` (name + infohash substring), `category`, `state`, `sort_by` ∈ `name,size,added_on,progress,category,state` (default `added_on`), `sort_order` ∈ `asc,desc` (default `desc`) | `{torrents[],total,page,limit,total_pages,has_prev,has_next,categories[]}`. SAFE |
| DELETE | `/api/torrents/{category}/{hash}` | Deletes one queue entry by hash. **`{category}` is read but never used by the handler** — deletion is by hash alone, across all categories. | `removeFromDebrid=true` → also removes provider placements and files | `200`, empty body. **DESTRUCTIVE** |
| DELETE | `/api/torrents` | Bulk delete. `400` if `hashes` is missing/empty (does **not** fall back to "all"). | `hashes=a,b,c` (**required**), `removeFromDebrid=true` | `200`, empty body. **DESTRUCTIVE** |

```bash
curl -H "Authorization: Bearer TOKEN" \
  'http://localhost:8282/api/torrents?page=1&limit=50&state=downloading&sort_by=added_on&sort_order=desc'

# Bulk delete. There is no ?category=/?hash= form on this route.
curl -X DELETE -H "Authorization: Bearer TOKEN" \
  'http://localhost:8282/api/torrents?hashes=abc123,def456&removeFromDebrid=true'
```

## Browse (hierarchical file browser)

Shared query params on the three listing endpoints: `page` (default 1), `limit` (1–100, default 50),
`sort_by` ∈ `name,size,mod_time,active_debrid` (default `name`; unknown values fall back to `name`),
`sort_order` ∈ `asc,desc` (default `asc`). Directories always sort above files. `search` (substring,
case-insensitive) applies to the group and torrent-file levels only.

| Method | Path | Does | Response / Safety |
|---|---|---|---|
| GET | `/api/browse` | Root groups (`__all__`, `__bad__`, custom folders). | `BrowseResponse` — `{entries[],total,page,limit,total_pages,current_dir,parent_dir}`; each entry `{name,path,size,mod_time,is_dir,info_hash,can_delete,active_debrid}`. SAFE |
| GET | `/api/browse/{group}` | Torrents in a group. `404` if the group is unknown. | `BrowseResponse`. SAFE |
| GET | `/api/browse/{group}/{torrent}` | Files in a torrent. `404` if unknown. | `BrowseResponse`. SAFE |
| GET | `/api/browse/{group}/{subgroup}/{torrent}` | Same handler, three-segment form. Only `{group}` and `{torrent}` are used; `{subgroup}` affects nothing but the returned `path` strings. | `BrowseResponse`. SAFE |
| GET | `/api/browse/download/{torrent}/{file}` | Download a file. Torrents → `302` redirect to the debrid link (plus `X-Accel-Redirect`); NZBs → streamed inline. | File bytes / redirect. SAFE |
| DELETE | `/api/browse/torrents/{id}` | Delete a torrent by **infohash**. Calls `DeleteEntry(id, true)` — provider placements and files are **always** removed; there is no `removeFromDebrid` opt-out here. | `{"success":true,"message":…}`. **DESTRUCTIVE** |
| DELETE | `/api/browse/torrents/batch` | Bulk version. `400` if `ids` is empty (no act-on-all fallback). | REQUIRED body `{"ids":["<infohash>",…]}` → `{"success":true,"message":…,"count":n}`. **DESTRUCTIVE** |

## Repair

| Method | Path | Does | Body | Response / Safety |
|---|---|---|---|---|
| GET | `/api/repair/config` | Current `repair` config block. | — | `RepairConfig`. SAFE |
| PATCH | `/api/repair/config` | **Partial update.** Merges the submitted object into the current repair config, saves, then reschedules. A key absent from the body keeps its current value; an explicitly submitted value — including `0`, `""` or `false` — overwrites (`PreserveMissingFields`, the same mechanism `PATCH /api/config` uses). The `repair` tri-state is preserved exactly: absent keeps the stored pointer, `null` included. | **REQUIRED** — any subset of `RepairConfig`: `{enabled,source,schedule,stop_schedule,workers,nntp_connection_percent,strategy,recheck_interval,arrs[],skip_nzb_repair,repair,prune,regrab,max_deletions_per_run,auto_repair}` | Saved `RepairConfig`. **DESTRUCTIVE** (writes config) |
| PUT | `/api/repair/config` | **Full replacement.** The submitted object *is* the new repair config: **every key you omit is cleared** — `max_deletions_per_run` → `0`, `stop_schedule` → `""`, `prune`/`regrab` → `false`, `repair` → unset, `arrs` → empty. Fields that have a save-time default (`source`, `workers`, `strategy`, `recheck_interval`, `nntp_connection_percent`) fall back to it rather than to the stored value. Validation still runs on the result, so a `PUT` of `{"enabled":true}` is a **`400`** — the replacement carries no schedule. | **REQUIRED** — a complete `RepairConfig` | Saved `RepairConfig`. **DESTRUCTIVE** (writes config, clears omissions) |
| GET | `/api/repair/status` | `{enabled,next_run_at,active_run,last_run,health_counts}`. Returns an empty object if the service is unavailable (never `503`). | — | SAFE |
| POST | `/api/repair/run` | Start a sweep now. Runs even when `repair.enabled` is false. | **OPTIONAL** — `{"ignore_last_checked":bool,"force":bool,"repair":bool,"prune":bool,"regrab":bool,"auto_repair":bool,"unrestrict_link":bool,"protocol":"all"\|"torrent"\|"nzb"}`. **Absent body ⇒ probe only what is due, using the configured REPAIR/PRUNE/RE-GRAB knobs.** Query params of the same names override the body (`1/true/yes/on` and `0/false/no/off`); `force` is an alias for `ignore_last_checked`. `protocol` also accepts `both` (≡ `all`); anything else → `400`. Omitting `protocol` uses `torrent` when `skip_nzb_repair` is set, else `all`. | `{"run_id":"…"}`; `409` if a run is active; `503` if the service is unavailable. **DESTRUCTIVE** if PRUNE/RE-GRAB resolve on |
| POST | `/api/repair/stop` | Cancels the active run and flips its record to `cancelled`. | — | `200`; `400` if no run is active. SAFE |
| POST | `/api/repair/fix` | Acts on entries **already** marked broken, **without re-probing**. `400` `"no repair action selected"` for an all-false `actions` object or when the resolved components are all off; `400` `"no fixable broken entries"` when nothing matches. | **OPTIONAL** — `{"names":["<entry name>",…],"actions":{"repair":bool,"prune":bool,"regrab":bool}}`. **Absent body, or `names` empty/omitted ⇒ acts on EVERY entry with status `broken`.** Absent `actions` ⇒ the configured knobs; all-false `actions` ⇒ `400`. `names` match `EntryHealth.entry_name` exactly (trimmed). | `RepairRun` record (work continues in background). **DESTRUCTIVE** |
| POST | `/api/repair/recheck/media` | Re-probe every entry an \*arr media id resolves to, then act. Empty `arr` → the first eligible \*arr that resolves entries wins. Bounded by the media id, so the deletion cap does not apply. | **REQUIRED** — `{"arr":string,"media_id":string,"fix":bool,"actions":{"repair":bool,"prune":bool,"regrab":bool}}`. `media_id` required. An all-false `actions` forces CHECK-only even with `fix:true`. | `RepairRun`; on error `{"error","run"}` with `400`, or `409` if a run is active. **DESTRUCTIVE** when `fix`/`actions` select PRUNE or RE-GRAB |
| POST | `/api/repair/clear-state` | Deletes persisted entry-health records for the given statuses. Touches **no** files, \*arrs, or placements. | **REQUIRED** — `{"statuses":["broken",…]}`; ≥1 required; valid values `healthy`, `broken`, `repairing`, `stale`, `unknown`, `unsupported` (case-insensitive). | `{"statuses":[…],"cleared":n}`; `409` if a run is active. Metadata-destructive only |
| GET | `/api/repair/runs` | Run history (last 100 retained). | — | Array of `RepairRun`. SAFE |
| GET | `/api/repair/runs/{id}` | One run. `404` if unknown. | — | `RepairRun`. SAFE |
| DELETE | `/api/repair/runs` | Clears the entire run history. No confirmation, no filter. | — | `200`. Metadata-destructive |
| GET | `/api/repair/health` | All entry-health records, sorted by entry name. | — | Array of `EntryHealth`. SAFE |
| GET | `/api/repair/health/{name}` | One entry's health, incl. `broken_files`. `{name}` is URL-unescaped. `404` if unknown. | — | `EntryHealth`. SAFE |
| POST | `/api/repair/health/{name}/check` | Re-probe one entry, then act if it probes broken. Does not take the singleton run lock; `400` if that entry is already being probed. Unlimited deletion budget. | **OPTIONAL** — `{"actions":{"repair":bool,"prune":bool,"regrab":bool}}`. **Absent body and no `?fix=true` ⇒ CHECK-only.** `?fix=true` with absent `actions` ⇒ the configured knobs; an all-false `actions` forces CHECK-only even with `?fix=true`. | `EntryHealth` ack with `status:"repairing"` (work runs in background). **DESTRUCTIVE** when `actions`/`?fix=true` select PRUNE or RE-GRAB |

Validation is identical for both verbs and runs on the RESULTING document: when the resulting
`enabled` is true, `schedule` must be present and parseable and `recheck_interval` parseable, and
`source` must be `arr` or `managed`; `nntp_connection_percent` must be 0–100 always. An empty or
malformed body is a `400` on both, and a rejected request never mutates the stored config.

```bash
# PATCH — partial update: only the cap changes; schedule, stop_schedule,
# prune/regrab and the repair tri-state keep their stored values.
curl -X PATCH -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"max_deletions_per_run":25}' \
  http://localhost:8282/api/repair/config

# PUT — full replacement. This does NOT just set the cap: it also clears
# stop_schedule, prune, regrab, arrs and the repair tri-state, and turns the
# sweep off (enabled was not sent).
curl -X PUT -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"max_deletions_per_run":25}' \
  http://localhost:8282/api/repair/config

# Prune only, this run only (REPAIR is off because a component was specified).
curl -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"prune":true}' http://localhost:8282/api/repair/run

# Explicit "check, act on nothing" for one entry.
curl -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"actions":{"repair":false,"prune":false,"regrab":false}}' \
  'http://localhost:8282/api/repair/health/My.Show.S01/check?fix=true'
```

`?status=<value>` on `GET /api/repair/health` filters by exact status string; an unknown value
silently returns an empty list rather than erroring.

`RepairRun` shape: `{id,trigger,status,stage,started_at,updated_at,completed_at,error,cancel_reason,source,stats}`.
`source` encodes the resolved components, e.g. `fix-broken:all:prune+regrab` or `managed:check-only`.

`stats` keys, grouped by the component they belong to:

| Key | Counts |
|---|---|
| `candidates` · `skipped_fresh` · `probed` | Entries selected / skipped as too recently checked / actually probed. |
| `healthy` · `broken` · `unknown` | CHECK verdicts. |
| `reacquired` | REPAIR: dead entries successfully re-acquired. |
| `repair_failed` | REPAIR: re-acquire attempts that **errored**. Arr-side failures are **not** counted here — they have their own `arr_delete_failed`. |
| `repair_skipped_unsupported` | REPAIR **declined**: the entry's protocol cannot be re-acquired (nzb — see below). |
| `pruned` | PRUNE: entries deleted decypharr-side. |
| `prune_skipped_not_eligible` | PRUNE **declined**: only *some* files in the entry are broken, so deleting the whole entry would be wrong. |
| `arr_deleted` | ARR-DELETE: entries whose \*arr file record was deleted. Search and blocklist are separate opt-ins and may or may not have run. |
| `arr_delete_failed` | ARR-DELETE: arr-side failures. |
| `arr_skipped_no_link` | ARR-DELETE **declined**: the entry has no resolved arr link to route through. |
| `deletions` · `deletion_cap_skipped` | Destructive-deletion slots consumed this run (PRUNE + ARR-DELETE) / broken entries left un-deleted because `max_deletions_per_run` was exhausted. |

:::note[These three counters were renamed]
`regrabbed` / `regrab_failed` / `regrab_skipped_no_arr_link` are now
`arr_deleted` / `arr_delete_failed` / `arr_skipped_no_link`. Anything parsing run
records by the old key names reads zero, not an error.
:::

The three `*_skipped_*` counters exist to separate a component that **declined on principle**
from one that **silently broke**: `reacquired: 0` alone reads as "REPAIR is broken", when the
truth may be `repair_skipped_unsupported: 40` — REPAIR correctly refusing every nzb entry.
The matching per-entry reason lands on the health record as `EntryHealth.action_skips`
(`{"repair"|"prune"|"regrab": "<reason>"}`), and a failed REPAIR attempt records
`EntryHealth.last_repair_error`.

:::caution[nzb entries cannot be REPAIRed]
REPAIR (re-acquire) applies to **torrent entries only** — `Entry.CanBeFixed()` is
`return e.IsTorrent()`. Re-acquisition for usenet is not sound: re-parsing the staged NZB asks
the same providers for the same message-ids, so it cannot resurrect a missing or payload-less
article. A dead nzb entry is actionable only via **ARR-DELETE** (delete the \*arr's file record,
optionally with `arr_search` to ask for a replacement) or **PRUNE** (delete decypharr-side —
which recovers nothing, it only stops decypharr serving a dead entry).

Running a REPAIR-only configuration against an nzb-heavy library therefore fixes **nothing**.
Watch `repair_skipped_unsupported` — that is the count of entries REPAIR declined for this
reason. Set `prune` and/or `regrab` if you want dead nzb entries acted on.
:::

## Mount cache

| Method | Path | Does | Safety |
|---|---|---|---|
| POST | `/api/mount/cache/cleanup` | Runs DFS cache cleanup (evict expired). `503` if the mount is not ready, `400` if the mount is not DFS. | **DESTRUCTIVE** (cache only) |
| POST | `/api/mount/cache/purge` | Purges the entire DFS cache. Same preconditions. | **DESTRUCTIVE** (cache only) |

Both return `{"status":"success","cache":{…}}`. No body.

## Debug

Auth-protected but **not** under `/api/`, so an unauthenticated call `303`-redirects to `/login`
instead of returning `401`.

| Method | Path | Does | Safety |
|---|---|---|---|
| GET | `/debug/stats` | Full stats snapshot: `{system,debrids,mount,usenet,active_streams,storage,queue,arrs,repair}`. Refreshed every 5s. | SAFE |
| GET | `/debug/logs` | Streams `decypharr.log` as `text/plain`. | SAFE |
| GET | `/logs` | Deprecated alias of `/debug/logs`. | SAFE |
| GET | `/debug/logs/rclone` | Streams `rclone.log`. `500` if absent. | SAFE |
| GET | `/debug/ingests` | Ingest records across all providers. | SAFE |
| GET | `/debug/ingests/{debrid}` | Ingest records for one provider. | SAFE |
| POST | `/debug/speedtest` | Runs a provider speed test. **REQUIRED** body `{"protocol":"nntp"\|"debrid","provider":string}`; both required. Returns `{provider,protocol,speed_mbps,latency_ms,bytes_read,tested_at}`. | SAFE (consumes bandwidth) |

## Webhooks

| Method | Path | Does | Body | Safety |
|---|---|---|---|---|
| POST | `/webhooks/tautulli` | **Authenticated** (see the Auth table — API token via header or `?token=`). `topic` must equal `"tautulli"` or `400`. Requires a media id (`media_id`, else `tmdb_id`, else `tvdb_id`) and runs a targeted media recheck with `fix` mapped to the configured knobs. **A payload with no media id is a `400`** — it never falls back to a library-wide sweep. | **REQUIRED** — `{"topic":"tautulli","arr":string,"media_id":string,"tvdb_id":string,"tmdb_id":string,"fix":bool}` | **DESTRUCTIVE** (bounded to the resolved media) |

```bash
curl -X POST -H "Content-Type: application/json" \
  'http://localhost:8282/webhooks/tautulli?token=YOUR_API_TOKEN' \
  -d '{"topic":"tautulli","arr":"Sonarr","tvdb_id":"{thetvdb_id}","fix":true}'
```

In Tautulli, configure a Webhook notification agent pointing at that URL. If the agent can send
custom headers, prefer `Authorization: Bearer YOUR_API_TOKEN` or `X-API-Token: YOUR_API_TOKEN`
over the query parameter, so the token stays out of proxy logs.

## WebDAV

Mounted at `/webdav` and **omitted entirely from the router when `disable_webdav` is true**
(`404`). Returns `503` with `Retry-After: 5` until the manager is ready. Auth is Basic and only
enforced when `use_auth` **and** `enable_webdav_auth` are both true — enabling `use_auth` alone
leaves WebDAV open.

Paths: `/webdav/`, `/webdav/{group}`, `/webdav/{group}/{torrent}`, `/webdav/{group}/{torrent}/{file}`,
`/webdav/stream/{group}/{torrent}/{file}` (same handler as the file route).

All five accept the same method set:

| Method | Does | Safety |
|---|---|---|
| `PROPFIND` | Directory/file listing → `207 Multi-Status` XML. `Depth: 0` omits children. Children that resolve to `410 Gone` are dropped from the listing rather than surfaced. | SAFE |
| `GET` | Download. `400` on a directory. | SAFE |
| `HEAD` | Metadata headers only. | SAFE |
| `OPTIONS` | Advertises `DAV: 1, 2`. | SAFE |
| `COPY` | Copy to `Destination` header. `Overwrite: F` to fail on conflict (default `T`). `412` if the destination exists, `409` on missing parent / unsupported / active source. | SAFE-ish |
| `MOVE` | Copy + delete source. | **DESTRUCTIVE** |
| `DELETE` | Deletes the entry (provider placements + files) or removes a file from a torrent. `204` on success. | **DESTRUCTIVE** |

`PUT`, `POST`, `MKCOL`, `PROPPATCH`, `LOCK` and `UNLOCK` are advertised in the `Allow`/`DAV`
response headers but hit the `default` branch and return `405 Method Not Allowed` — the handler
switch implements only the seven methods above. The mount is effectively read-plus-delete.

## Compatibility APIs

Mounted separately from `/api/*` and authenticated with **\*arr credentials**, not the decypharr
API token. Listed for completeness; the \*arrs drive these, not operators.

**qBittorrent emulation** — `/api/v2`:
`POST /auth/login` · `GET|POST /torrents/info` · `POST /torrents/add` · **`POST /torrents/delete`
(DESTRUCTIVE)** · `GET|POST /torrents/categories` · `POST /torrents/createCategory` ·
`POST /torrents/setCategory` · `POST /torrents/addTags` · `POST /torrents/removeTags` ·
`POST /torrents/createTags` · `GET|POST /torrents/tags` · `GET|POST /torrents/pause` ·
`GET|POST /torrents/resume` · `GET|POST /torrents/recheck` · `GET|POST /torrents/properties` ·
`GET|POST /torrents/files` · `GET /app/version` · `GET /app/webapiVersion` · `GET /app/preferences` ·
`GET /app/buildInfo` · **`GET /app/shutdown` (DESTRUCTIVE — stops the process)**.

**SABnzbd emulation** — `GET|POST /sabnzbd/api`, dispatched by the `?mode=` query parameter.

## Error shapes

`/api/*` and `/webhooks/*` auth failures return JSON: `{"error": "...", "status": 401}`. Almost
every other error path uses `http.Error`, i.e. **`text/plain`**, not JSON — do not assume a JSON
body on `400`/`404`/`500` from these handlers.

There is no application-level rate limiting; only the per-provider debrid limits configured in
`config.json` apply.

## Client examples

```python
import requests

TOKEN = "your_api_token"
BASE_URL = "http://localhost:8282"
headers = {"Authorization": f"Bearer {TOKEN}"}

# List torrents (server-side paging/filtering).
r = requests.get(f"{BASE_URL}/api/torrents",
                 headers=headers,
                 params={"page": 1, "limit": 50, "sort_by": "added_on"})
torrents = r.json()["torrents"]

# Add a magnet - multipart form, NOT json=.
r = requests.post(f"{BASE_URL}/api/add",
                  headers=headers,
                  files={"urls": (None, "magnet:?xt=urn:btih:..."),
                         "arr": (None, "sonarr")})

# Raise the destructive-action cap without touching any other repair setting.
# PATCH, not PUT: a PUT here would clear stop_schedule, prune, regrab and the
# rest of the block along the way.
r = requests.patch(f"{BASE_URL}/api/repair/config",
                   headers=headers,
                   json={"max_deletions_per_run": 25})
```

```javascript
const TOKEN = 'your_api_token';
const BASE_URL = 'http://localhost:8282';
const headers = { Authorization: `Bearer ${TOKEN}` };

// List torrents
fetch(`${BASE_URL}/api/torrents?page=1&limit=50`, { headers })
  .then(r => r.json())
  .then(data => console.log(data.torrents));

// Add a magnet - multipart, not JSON.
const form = new FormData();
form.append('urls', 'magnet:?xt=urn:btih:...');
form.append('arr', 'sonarr');
fetch(`${BASE_URL}/api/add`, { method: 'POST', headers, body: form });
```

## Traps worth memorizing

1. **A malformed body is rejected, but an absent one is not.** `curl -d '{'` → `400`; `curl -X POST`
   with no body on `/api/repair/run` or `/api/repair/fix` → a real run with the configured knobs.
2. **`POST /api/repair/fix` with no body acts on every broken entry** using the configured knobs.
   An all-false `actions` is a `400`, not a silent full run.
3. **`{"prune": true}` on `/api/repair/run` turns REPAIR off** for that run — the omitted components
   default to `false` once any one is specified.
4. **`DELETE /api/browse/torrents/{id}` always deletes from the provider** — unlike
   `/api/torrents/...`, there is no `removeFromDebrid` flag to withhold.
5. **`{category}` in `DELETE /api/torrents/{category}/{hash}` is ignored.** The hash alone selects
   the entry.
6. **`POST /api/repair/run` ignores `repair.enabled: false`.**
7. **`recheck/media` and `health/{name}/check` bypass the per-run deletion cap.**
8. **`use_auth: false` disables auth for the whole server**, destructive endpoints included —
   `/webhooks/*` along with everything else.
9. **`repair.auto_repair` in config is dead.** It is ignored by the resolver; only
   `repair`/`prune`/`regrab` decide what a run does.
10. **`POST /api/refresh-token` returns `token`, not `api_token`** — and the old token stops working
    the moment it returns.
11. **`PUT` on either config endpoint means REPLACE, and it means it.** `PUT /api/repair/config`
    with `{"max_deletions_per_run":25}` does not "just set the cap" — it clears `stop_schedule`,
    `prune`, `regrab`, `arrs`, the `repair` tri-state and `enabled` along with it. Use `PATCH` for
    a one-field change. There is **no `POST /api/config`**; the config endpoints answer `GET`,
    `PATCH` and `PUT` only.
