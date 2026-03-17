import { backfillCreditFromMusicBrainz } from '../services/evidence-backfill.js';
import { getRecordingCreditEvidenceCount } from '../services/db.js';

interface AutoBackfillCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

async function runProducerBackfillCase(): Promise<AutoBackfillCaseResult> {
  const id = 'auto_backfill_musicbrainz_producer';
  const name = 'Tony Visconti';
  const role = 'producer';

  const before = getRecordingCreditEvidenceCount(name, role);
  const result = await backfillCreditFromMusicBrainz({ name, role, limit: 20 });
  const after = getRecordingCreditEvidenceCount(name, role);

  const pass = after >= before && result.mbCandidates > 0;
  return {
    id,
    pass,
    details: `before=${before} after=${after} inserted=${result.insertedEvidence} candidates=${result.mbCandidates}`,
  };
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  const networkEnabled = process.env.ENABLE_NETWORK_EVAL === 'true';

  if (!networkEnabled) {
    console.log('[eval:auto-backfill] skipped (set ENABLE_NETWORK_EVAL=true to run network eval)');
    return;
  }

  const results: AutoBackfillCaseResult[] = [
    await runProducerBackfillCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;
  console.log('[eval:auto-backfill] Auto-backfill harness');
  console.log(`[eval:auto-backfill] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:auto-backfill] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error('[eval:auto-backfill] failed:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
