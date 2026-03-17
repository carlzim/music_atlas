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
