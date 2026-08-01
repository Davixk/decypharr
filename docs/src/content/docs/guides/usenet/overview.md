---
title: Usenet Configuration
description: Direct NNTP streaming configuration.
---

Decypharr supports direct NNTP streaming from Usenet providers - no additional download client required.

## How It Works

Decypharr connects directly to NNTP servers to:

1. Parse NZB files for segment information
2. Stream segments on-demand for playback
3. Download and assemble complete files

## Provider Configuration

### Add Provider

```json
{
  "usenet": {
    "providers": [
      {
        "host": "news.provider.com",
        "port": 563,
        "username": "your_username",
        "password": "your_password",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      }
    ]
  }
}
```

### Multiple Providers

Decypharr can use multiple providers with priority and failover:

```json
{
  "usenet": {
    "providers": [
      {
        "host": "primary.news.com",
        "port": 563,
        "username": "user1",
        "password": "pass1",
        "backbone": "UsenetExpress",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      },
      {
        "host": "backup.news.com",
        "port": 563,
        "username": "user2",
        "password": "pass2",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 10,
        "priority": 2
      }
    ]
  }
}
```

Lower `priority` = higher preference.

`backbone` is optional. Set it when two providers share the same article spool so Decypharr can skip same-backbone providers after `423/430 article not found` responses.

## Performance Tuning

### Connection Limits

```json
{
  "usenet": {
    "max_connections": 15,
    "processing_max_connections": 15
  }
}
```

- `max_connections`: Per-file streaming connection limit
- `processing_max_connections`: Per-file parsing and NZB download connection limit
- Provider `max_connections`: Per-provider limit

**Example:**

- Global: `15`
- Provider A: `20`
- Provider B: `10`

→ Up to 15 connections per file, split between providers based on priority

### Read-Ahead Buffer

```json
{
  "usenet": {
    "read_ahead": "16MB"
  }
}
```

Prefetch buffer for smoother playback. Higher = smoother but more memory.

### Processing Limits

```json
{
  "max_active_downloads": 5,
  "usenet": {"processing_timeout": "10m"}
}
```

- `max_active_downloads`: concurrent post-download work (mount waits, symlink
  creation). Local I/O only &mdash; **not** a limit on NZB imports
- `processing_timeout`: Mark as bad if processing exceeds this

NZB processing concurrency is `floor(total provider connections /
usenet.processing_max_connections)`, not either job knob. NZB imports consume no
provider download slot, so they are not subject to provider admission at all.

### Stream Read Timeout

```json
{
  "usenet": {"read_timeout": "30s"}
}
```

`read_timeout` is a no-progress deadline. If a stream delivers no bytes while
waiting for a reader slot, provider connection, or article body, Decypharr
cancels the read and releases its resources instead of leaving a stuck WebDAV
request. Article-not-found failures are persisted so later requests fail fast.

### Availability Checking

```json
{
  "usenet": {
    "availability_sample_percent": 10,
    "import_availability_sample_percent": 1
  }
}
```

Use `availability_sample_percent` for repair checks and
`import_availability_sample_percent` for the availability gate when adding an NZB.

- `100`: Check all segments (slow but accurate)
- `10`: Check 10% (fast but may miss issues)
- `1`: Quick import check (default)

## Disk Buffer

```json
{
  "usenet": {
    "disk_buffer_path": "/cache/usenet/streams"
  }
}
```

Streams use disk buffer for assembly. Ensure sufficient disk space.

## Repair

```json
{
  "usenet": {
    "skip_repair": false
  }
}
```

## Arr Integration

Arrs send NZB files to Decypharr via the Sabnzbd API endpoint:

See [Sabnzbd Integration](./sabnzbd/) for details.

## Troubleshooting

### Connection Failures

- Verify host/port/SSL settings
- Test manually: `telnet news.provider.com 563`
- Check provider status

### Slow Streaming

1. Increase `max_connections` per provider
2. Increase global `max_connections`
3. Increase `read_ahead` buffer

### Processing Timeouts

- Increase `processing_timeout` for large files
- Reduce `availability_sample_percent` for faster checks
- To raise NZB processing concurrency, raise the providers' `max_connections` or
  **lower** `usenet.processing_max_connections` &mdash; the gate is
  `floor(pool / processing_max_connections)`, so raising that value *reduces* how
  many NZBs process at once. `max_active_downloads` does not affect it.

### Incomplete Downloads

- Enable `skip_repair: false` for PAR2 repair
- Check provider retention (old files may be incomplete)
- Try backup provider if available

## Example Configuration

Full Usenet config with optimal settings:

```json
{
  "max_active_downloads": 5,
  "usenet": {
    "providers": [
      {
        "host": "us.news.provider.com",
        "port": 563,
        "username": "user",
        "password": "pass",
        "ssl": true,
        "max_connections": 30,
        "priority": 1
      }
    ],
    "max_connections": 15,
    "processing_max_connections": 15,
    "read_ahead": "32MB",
    "read_timeout": "30s",
    "processing_timeout": "15m",
    "availability_sample_percent": 5,
    "disk_buffer_path": "/cache/usenet",
    "skip_repair": false
  }
}
```
