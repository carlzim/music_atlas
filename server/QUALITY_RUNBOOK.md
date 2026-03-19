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

Canonical-first variant policy (current):

- Candidates are bucketed and selected in this order:
  1. `canonical`
  2. `fallback_live`
  3. `avoid`
- `soundtrack` and `remix` variants are treated as `avoid` unless prompt intent explicitly allows them.
- `live` variants are treated as `fallback_live` for normal prompts, and promoted when prompt/venue intent is explicitly live-focused.
- `remaster`/`mix` are generally allowed as canonical-safe variants (to avoid dropping valid modern official releases).

### Artist parsing and collaborator matching (v1)

Recent matching work adds safer handling for truncated artist names and collaboration text.

- Generated playlist parsing now separates collaboration metadata:
  - `artist`: primary artist (used for matching)
  - `featured_artists`: parsed guest artist list
  - `artist_display`: original collaboration display string
- Collaboration parsing supports common English + Swedish forms:
  - `feat`, `featuring`, `ft`
  - `duett med`, `with`, `med`
  - delimiter splits including `&`, `and`, `och`, `,`, `x`, `+` (guarded)
- Spotify matching uses layered artist strategies:
  - exact primary-artist match
  - safe primary prefix fallback (for truncated names)
  - guarded alias expansion for known short forms
  - fallback candidate/diagnostic searches using featured and display artist forms
- Tie-breaks now prefer stronger artist match modes when score is equal:
  - `exact` > `prefix` > `alias` > `other`

Additional search resilience for this class of failures:

- Song title query fallbacks for shortened variants (before `-`, `/`, `:`, `|`).
- ASCII/diacritic-stripped fallback queries for song titles.
- Artist-key normalization strips ensemble suffixes (e.g. `Orkester`, `Band`, `Trio`) in a guarded way.

### Artist/collab diagnostics fields

`POST /api/spotify/save-playlist/:id` includes counters to validate artist parsing improvements in real playlists:

- `artistNormalizationStats.tracksWithFeaturedArtists`
- `artistNormalizationStats.tracksWithArtistDisplay`
- `artistNormalizationStats.featuredArtistFallbackAttempts`
- `artistNormalizationStats.featuredArtistFallbackMatches`
- `artistNormalizationStats.featuredArtistDiagnosticFallbackAttempts`
- `artistNormalizationStats.featuredArtistDiagnosticFallbackMatches`
- `artistNormalizationStats.displayArtistFallbackAttempts`
- `artistNormalizationStats.displayArtistFallbackMatches`
- `artistNormalizationStats.searchExactArtistMatches`
- `artistNormalizationStats.searchPrefixArtistMatches`
- `artistNormalizationStats.searchAliasArtistMatches`
- `artistNormalizationStats.searchOtherArtistMatches`

Quick interpretation:

- High `searchExactArtistMatches`: healthy canonical artist quality.
- High `searchPrefixArtistMatches` or `searchAliasArtistMatches`: generation still emits truncated names; guardrails are rescuing matches.
- High featured/display fallback attempts with low matches: collaboration text is being parsed, but query forms still need tuning.

### Credit playlist curation modes (producer/engineer/arranger)

Credit prompts now use a prompt-driven curation mode to improve prominence and representativeness after factual retrieval.

- Modes:
  - `balanced` (default)
  - `essential` (triggered by intent like `best`, `greatest`, `most iconic`, `signature`, `essential`)
  - `deep_cuts` (triggered by intent like `unknown`, `obscure`, `underrated`, `deep cuts`, `hidden gems`, `b-sides`)
- Conflict rule: if both essential and deep-cut intents appear, `deep_cuts` wins.

Pipeline for credit prompts:

1. Retrieval (verified truth/evidence candidates)
2. Ranking (component scoring)
3. Composition (mode-aware balance by artist/decade)

Scoring components used in ranking diagnostics:

- `relevance_to_query`
- `prominence_score`
- `artist_canonical_score`
- `entity_signature_score`
- `diversity_adjustment`

Returned diagnostics in `truth.curation`:

- `mode`, `inferred_from_prompt`
- `top_score_sample` (top candidate score breakdown)
- `composition` summary (`selected_tracks`, `selected_track_target`, `selected_track_target_met`, `selected_track_gap`, `selected_track_coverage`, `selection_retention_gap`, `selection_retention_coverage`, `target_total_count`, `target_met_count`, `target_met_coverage`, `target_met_reasons`, `target_miss_count`, `target_miss_reasons`, `target_consistency_ok`, `target_size_met`, `target_retention_met`, `target_artist_met`, `target_decade_met`, `unique_artists`, `unique_artist_target`, `unique_artist_target_met`, `unique_artist_target_gap`, `unique_artist_target_coverage`, `unique_decades`, `unique_decade_target`, `unique_decade_target_met`, `unique_decade_target_gap`, `unique_decade_target_coverage`, `max_tracks_per_artist`)

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
