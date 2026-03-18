# Quality Runbook

This project uses strict quality gates to prevent silent data drift.

## Quick Local Check

Run this from `server/`:

```bash
npm run eval:ci:strict
```

To run with the exact CI threshold profile locally (recommended):

```bash
npm run eval:ci:strict:local
```

To verify that the profile itself is sane and internally consistent:

```bash
npm run eval:profile:check
```

If all commands pass, current quality is healthy.

## What Each Command Means

- `eval:ci:strict`: full strict pipeline used by CI (enrich, trend, atlas/parser/credit-match/reason-quality/studio-evidence/spotify strict, coverage, thresholds, summaries).
- `eval:ci:strict:local`: same strict pipeline with CI-equivalent env thresholds preloaded.
- `eval:profile:check`: validates profile keys and ensures non-main thresholds are not stricter than main.
- `eval:profile:status`: emits profile fingerprint artifacts (`profile-status.json/.md`) for traceability.
- `eval:profile:drift`: compares current profile fingerprint with previous baseline.
- `eval:baseline:persist`: updates local baseline files (`last-enrich-evidence.json`, `last-profile-status.json`, `last-reason-quality.json`).
- `eval:auto-backfill`: network-dependent MusicBrainz auto-backfill smoke harness (runs only when `ENABLE_NETWORK_EVAL=true`).
- `eval:credit-match`: strict local harness for diacritic and title-variant credit matching behavior.
- `eval:reason-quality`: strict local harness for anti-repetition and reason variation quality.
- `eval:truth-membership`: network-dependent MusicBrainz membership truth harness (runs only when `ENABLE_NETWORK_EVAL=true`).
- `eval:truth-credit`: network-dependent Discogs truth credit harness (runs only when `ENABLE_NETWORK_EVAL=true`).
- `eval:routing`: deterministic prompt-routing policy harness.
- `eval:routing:status`: emits routing observability summary from `routing-status.json`.

### Routing policy env overrides

You can override route mode per intent without code changes:

- `ROUTE_CREDIT_MODE`
- `ROUTE_STUDIO_MODE`
- `ROUTE_VENUE_MODE`
- `ROUTE_EQUIPMENT_MODE`
- `ROUTE_ARTIST_DISCOVERY_MODE`
- `ROUTE_ABSTRACT_MOOD_MODE`
- `ROUTE_UNKNOWN_MODE`
- `ROUTE_DEFAULT_MODE` (fallback for all intents)

Allowed values: `truth-first`, `hybrid`, `gemini-first`.

### Truth backfill endpoints (dev)

- `POST /api/evidence/backfill-credit`: MusicBrainz credit evidence backfill.
- `POST /api/evidence/backfill-credit-truth`: Discogs truth-credit backfill.

### Spotify save diagnostics (dev/prod)

Endpoint: `POST /api/spotify/save-playlist/:id`

On partial or failed add-tracks calls, the response includes structured diagnostics to explain what happened.

- `failedChunkCategory`: high-level bucket for failure cause.
  - `request_timeout`: local request timed out before Spotify responded.
  - `network_error`: fetch/network issue that was not a timeout.
  - `rate_limited`: Spotify returned `429`.
  - `spotify_server_error`: Spotify returned `5xx`.
  - `spotify_client_error`: Spotify returned `4xx` (non-429).
  - `unknown_error`: fallback when no better classification is available.
- `failedChunkTransient`: whether the failing status/error was treated as retryable.
- `failedChunkIndex`, `totalChunks`, `failedChunkAttempt`, `failedChunkStatus`, `failedChunkTimeoutMs`, `failedChunkError`: low-level details for the exact failing chunk request.
- `addTracksChunkStats`: aggregate retry metrics for the save operation.
  - `totalChunks`, `totalAttempts`, `retriedChunks`
  - `retryDelayTotalMs`, `retryDelayAverageMs`, `retryDelayMaxMs`
  - `retryAfterRetries` (Spotify `Retry-After`) and `backoffRetries` (local exponential backoff)
  - `requestTimeoutMs`, `maxAttempts`, `baseRetryDelayMs`, `maxRetryDelayMs`

Quick interpretation guide:

- High `rate_limited` + high `retryAfterRetries`: Spotify throttling; consider fewer concurrent saves or longer retry windows.
- High `request_timeout` + low `retryAfterRetries`: network latency/instability or timeout too low for current conditions.
- High `spotify_server_error`: transient Spotify instability.
- `retryDelayMaxMs` much larger than `retryDelayAverageMs`: a few severe spikes drove most wait time.
- `totalAttempts` close to `totalChunks * maxAttempts`: retry budget is nearly exhausted.

Retry tuning knobs (`server/.env`):

- `SPOTIFY_ADD_TRACKS_TIMEOUT_MS` (min 5000)
- `SPOTIFY_ADD_TRACKS_MAX_ATTEMPTS` (clamped to 1..5)
- `SPOTIFY_ADD_TRACKS_BASE_RETRY_DELAY_MS` (min 100)
- `SPOTIFY_ADD_TRACKS_MAX_RETRY_DELAY_MS` (min 500)

### Spotify match diagnostics (v1)

`POST /api/spotify/save-playlist/:id` now includes search-quality diagnostics even on successful saves:

- `searchScoreBands`: coarse distribution for search-selected matches.
  - `strong` (`score >= 6`)
  - `good` (`score >= 3`)
  - `weak` (`score >= 1`)
  - `uncertain` (`score <= 0`)
  - `unknown` (no numeric score captured)
- `searchScoreSummary`: aggregate quality snapshot.
  - `scoredCount`, `average`, `min`, `max`

Quick interpretation:

- High `strong + good`: search ranking quality is healthy.
- Rising `weak + uncertain`: likely drift in title/artist matching quality; review recent ranking heuristics.
- High `unknown`: scoring path may be bypassed by non-search sources or missing score propagation.

### Spotify ranking heuristics (v1)

Current ranking intentionally prefers canonical versions unless prompts explicitly request variants.

- Primary artist matches are boosted over featured-only matches.
- Crowded featured-only entries get a slight penalty.
- Variant types are downranked by default unless requested:
  - karaoke / instrumental / a cappella
  - tribute / cover
  - sped up / slowed / nightcore / reverb / 8d
  - remix / edit / rework / mashup / flip
- On score ties, non-variant candidates are preferred before popularity/year tie-breakers.

This keeps default user requests closer to standard/canonical recordings while preserving expected behavior for explicit variant prompts.

## If A Check Fails

1. Run `npm run eval:coverage` to inspect current counts.
   - Check `studio alias groups` specifically; it should stay `(none)`.
2. Run `npm run eval:reason-quality:strict` to verify reason variation quality.
   - Expect `min_unique` to stay high and `max_dup` to stay low.
3. If evidence is low, run `npm run enrich:evidence` again and re-check.
4. If trend fails after intentional changes, update baseline with:

```bash
npm run eval:trend:rebaseline
```

5. Re-run the full quick check with `npm run eval:ci:strict`.

6. If this run becomes your new local baseline, persist it:

```bash
npm run eval:baseline:persist
```

## Key Artifacts

Generated in `server/eval-artifacts/`:

- `enrich-evidence.json` / `.md`: latest evidence snapshot.
- `reason-quality.json` / `.md`: latest reason variation quality snapshot.
- `trend-summary.md`: delta vs previous baseline.
- `pr-summary-brief.md`: short human summary used in PR comments.
- `quality-status.json` / `.md`: single status snapshot for dashboards.
- `profile-status.json` / `.md`: CI profile key count and SHA256 fingerprint.
- `routing-status.json` / `.md`: runtime routing counters (calls/success/fallback/backfill by mode/source).
- `routing-status-summary.md`: compact routing observability summary for CI output.
