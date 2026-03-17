import 'dotenv/config';
import { pathToFileURL } from 'url';
import { backfillTruthCreditsFromDiscogs } from '../services/truth-credit-layer.js';

function getArgValue(flag: string): string {
  const index = process.argv.indexOf(flag);
  if (index < 0) return '';
  return String(process.argv[index + 1] || '').trim();
}

function parseLimit(): number {
  const raw = getArgValue('--limit');
  const parsed = Number(raw || '20');
  if (!Number.isFinite(parsed)) return 20;
  return Math.max(1, Math.min(50, Math.floor(parsed)));
}

export async function runDiscogsCreditBackfill(): Promise<void> {
  const name = getArgValue('--name');
  const role = getArgValue('--role').toLowerCase();
  const query = getArgValue('--query') || `${name} ${role}`;
  const limit = parseLimit();

  const result = await backfillTruthCreditsFromDiscogs({
    creditName: name,
    creditRole: role,
    query,
    limit,
  });

  console.log('[backfill:credit:discogs] done');
  console.log(`[backfill:credit:discogs] name=${name}`);
  console.log(`[backfill:credit:discogs] role=${role}`);
  console.log(`[backfill:credit:discogs] query=${query}`);
  console.log(`[backfill:credit:discogs] attempted=${result.attempted}`);
  console.log(`[backfill:credit:discogs] imported=${result.imported}`);
  if (result.skippedReason) {
    console.log(`[backfill:credit:discogs] skipped_reason=${result.skippedReason}`);
  }
}

const entryPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : '';
if (entryPath && import.meta.url === entryPath) {
  runDiscogsCreditBackfill().catch((error) => {
    console.error('[backfill:credit:discogs] failed:', error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
