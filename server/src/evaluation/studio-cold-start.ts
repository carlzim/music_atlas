import Database from 'better-sqlite3';
import { backfillStudioFromDiscogs } from '../services/evidence-backfill-studio.js';
import { getTracksByRecordingStudioEvidence } from '../services/db.js';
import { isDiscogsConfigured } from '../services/discogs.js';
import { buildStudioCanonicalKey } from '../services/normalize.js';
import { resolveStudioIdentityFromPrompt } from '../services/studio-identity.js';

interface StudioColdStartResult {
  id: string;
  pass: boolean;
  details: string;
}

function countTrustedStudioTracks(acceptedStudios: string[]): number {
  const keys = new Set<string>();
  for (const studio of acceptedStudios) {
    const rows = getTracksByRecordingStudioEvidence(studio, 500, true);
    for (const row of rows) {
      const artist = String(row.artist || '').trim().toLowerCase();
      const title = String(row.title || '').trim().toLowerCase();
      if (!artist || !title) continue;
      keys.add(`${artist}::${title}`);
    }
  }
  return keys.size;
}

function purgeSystemSeedEvidence(db: Database.Database, acceptedStudios: string[], acceptedPromptNames: string[]): void {
  const canonicalStudios = Array.from(
    new Set(
      acceptedStudios
        .map((value) => buildStudioCanonicalKey(value))
        .filter((value) => value.length > 0)
    )
  );

  if (canonicalStudios.length > 0) {
    const placeholders = canonicalStudios.map(() => '?').join(', ');
    db.prepare(`
      DELETE FROM recording_studio_evidence
      WHERE source_playlist_id IN (
        SELECT id FROM playlists WHERE prompt LIKE '[system] studio evidence backfill from %'
      )
        AND COALESCE(studio_name_canonical, lower(trim(studio_name))) IN (${placeholders})
    `).run(...canonicalStudios);

    db.prepare(`
      DELETE FROM recording_studio_album_evidence
      WHERE source_playlist_id IN (
        SELECT id FROM playlists WHERE prompt LIKE '[system] studio evidence backfill from %'
      )
        AND COALESCE(studio_name_canonical, lower(trim(studio_name))) IN (${placeholders})
    `).run(...canonicalStudios);
  }

  const promptNeedles = Array.from(
    new Set(
      acceptedPromptNames
        .map((value) => value.trim().toLowerCase())
        .filter((value) => value.length > 0)
    )
  );
  if (promptNeedles.length === 0) return;

  const promptClause = promptNeedles.map(() => 'lower(prompt) LIKE ?').join(' OR ');
  const promptParams = promptNeedles.map((value) => `%${value}%`);
  db.prepare(`
    DELETE FROM playlists
    WHERE prompt LIKE '[system] studio evidence backfill from %'
      AND (${promptClause})
  `).run(...promptParams);
}

async function runStudioColdStartCase(): Promise<StudioColdStartResult> {
  const id = 'studio_cold_start_seed_recovery';
  const requestedPrompt = process.env.STUDIO_COLD_START_PROMPT || 'Best recordings made in Muscle Shoals studio';
  const resolved = resolveStudioIdentityFromPrompt(requestedPrompt);
  if (!resolved) {
    return {
      id,
      pass: false,
      details: `failed_to_resolve_identity prompt="${requestedPrompt}"`,
    };
  }

  const acceptedStudios = Array.from(new Set(resolved.acceptedStudioNames));
  const before = countTrustedStudioTracks(acceptedStudios);

  const preflight = await backfillStudioFromDiscogs({
    studioName: resolved.primaryName,
    prompt: requestedPrompt,
    limit: 400,
  });

  if (!isDiscogsConfigured() && preflight.imported === 0 && preflight.insertedEvidence === 0 && preflight.skippedReason === 'no_rows') {
    return {
      id,
      pass: true,
      details: `skipped_no_discogs_and_empty_musicbrainz identity=${resolved.key} studio="${resolved.primaryName}"`,
    };
  }

  const db = new Database('playlists.db');
  try {
    purgeSystemSeedEvidence(db, acceptedStudios, [resolved.primaryName, ...acceptedStudios]);
  } finally {
    db.close();
  }

  const afterPurge = countTrustedStudioTracks(acceptedStudios);
  const backfill = await backfillStudioFromDiscogs({
    studioName: resolved.primaryName,
    prompt: requestedPrompt,
    limit: 400,
  });
  const afterBackfill = countTrustedStudioTracks(acceptedStudios);

  const pass = afterBackfill >= 8;
  return {
    id,
    pass,
    details: [
      `identity=${resolved.key}`,
      `studio="${resolved.primaryName}"`,
      `before=${before}`,
      `after_purge=${afterPurge}`,
      `after_backfill=${afterBackfill}`,
      `inserted=${backfill.insertedEvidence}`,
      `source=${backfill.source}`,
      `mb_place=${backfill.musicBrainzPlaceId || 'none'}`,
    ].join(' '),
  };
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  const networkEnabled = process.env.ENABLE_NETWORK_EVAL === 'true';
  if (!networkEnabled) {
    console.log('[eval:studio-cold-start] skipped (set ENABLE_NETWORK_EVAL=true to run network eval)');
    return;
  }

  const results = [await runStudioColdStartCase()];
  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;

  console.log('[eval:studio-cold-start] Studio cold-start harness');
  console.log(`[eval:studio-cold-start] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:studio-cold-start] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error('[eval:studio-cold-start] failed:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
