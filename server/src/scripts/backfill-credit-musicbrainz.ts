import { pathToFileURL } from 'url';
import { backfillCreditFromMusicBrainz } from '../services/evidence-backfill.js';

function getArgValue(flag: string): string {
  const index = process.argv.indexOf(flag);
  if (index < 0) return '';
  return String(process.argv[index + 1] || '').trim();
}

function parseLimit(): number {
  const raw = getArgValue('--limit');
  const parsed = Number(raw || '150');
  if (!Number.isFinite(parsed)) return 150;
  return Math.max(1, Math.min(1000, Math.floor(parsed)));
}

export async function runMusicBrainzCreditBackfill(): Promise<void> {
  const name = getArgValue('--name');
  const role = getArgValue('--role').toLowerCase();
  const limit = parseLimit();

  const result = await backfillCreditFromMusicBrainz({ name, role, limit });

  console.log('[backfill:credit:mb] done');
  console.log(`[backfill:credit:mb] name=${result.name}`);
  console.log(`[backfill:credit:mb] role=${result.role}`);
  console.log(`[backfill:credit:mb] mbid=${result.mbid}`);
  console.log(`[backfill:credit:mb] mb_candidates=${result.mbCandidates}`);
  console.log(`[backfill:credit:mb] source_playlist_id=${result.sourcePlaylistId}`);
  console.log(`[backfill:credit:mb] inserted_recordings=${result.insertedRecordings}`);
  console.log(`[backfill:credit:mb] inserted_evidence=${result.insertedEvidence}`);
  console.log(`[backfill:credit:mb] skipped_existing_evidence=${result.skippedExistingEvidence}`);
  console.log(`[backfill:credit:mb] skipped_invalid=${result.skippedInvalid}`);
}

const entryPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : '';
if (entryPath && import.meta.url === entryPath) {
  runMusicBrainzCreditBackfill().catch((error) => {
    console.error('[backfill:credit:mb] failed:', error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
