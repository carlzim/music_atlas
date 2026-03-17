import 'dotenv/config';
import { backfillTruthCreditsFromDiscogs, getTruthCreditCandidates } from '../services/truth-credit-layer.js';

interface TruthCreditCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

async function runTruthCreditCase(params: {
  id: string;
  creditName: string;
  creditRole: string;
  query: string;
  limit?: number;
  expectedAttempted?: boolean;
  expectedSkippedReason?: string;
  expectedImported?: number;
}): Promise<TruthCreditCaseResult> {
  const result = await backfillTruthCreditsFromDiscogs({
    creditName: params.creditName,
    creditRole: params.creditRole,
    query: params.query,
    limit: params.limit ?? 10,
  });

  if (!result.attempted && result.skippedReason === 'missing_discogs_token') {
    return {
      id: params.id,
      pass: true,
      details: 'skipped_missing_discogs_token',
    };
  }

  if (params.expectedSkippedReason) {
    const attemptedMatches = typeof params.expectedAttempted === 'boolean'
      ? result.attempted === params.expectedAttempted
      : true;
    const importedMatches = typeof params.expectedImported === 'number'
      ? result.imported === params.expectedImported
      : true;
    const pass = attemptedMatches
      && importedMatches
      && result.skippedReason === params.expectedSkippedReason;
    return {
      id: params.id,
      pass,
      details: `attempted=${result.attempted} imported=${result.imported} skipped=${result.skippedReason || ''} expected_attempted=${typeof params.expectedAttempted === 'boolean' ? params.expectedAttempted : ''} expected_imported=${typeof params.expectedImported === 'number' ? params.expectedImported : ''} expected_skip=${params.expectedSkippedReason}`,
    };
  }

  const candidates = getTruthCreditCandidates(params.creditName, params.creditRole, 20);
  const pass = result.attempted && (result.imported > 0 || candidates.length > 0);
  return {
    id: params.id,
    pass,
    details: `attempted=${result.attempted} imported=${result.imported} skipped=${result.skippedReason || ''} candidates=${candidates.length}`,
  };
}

async function runProducerCase(): Promise<TruthCreditCaseResult> {
  return runTruthCreditCase({
    id: 'truth_credit_discogs_producer',
    creditName: 'Brian Eno',
    creditRole: 'producer',
    query: 'Brian Eno producer',
    limit: 10,
  });
}

async function runEngineerCase(): Promise<TruthCreditCaseResult> {
  return runTruthCreditCase({
    id: 'truth_credit_discogs_engineer',
    creditName: 'Alan Moulder',
    creditRole: 'engineer',
    query: 'Alan Moulder engineer',
    limit: 12,
  });
}

async function runArrangerCase(): Promise<TruthCreditCaseResult> {
  return runTruthCreditCase({
    id: 'truth_credit_discogs_arranger',
    creditName: 'Claus Ogerman',
    creditRole: 'arranger',
    query: 'Claus Ogerman arranger',
    limit: 12,
  });
}

async function runUnsupportedRoleCase(): Promise<TruthCreditCaseResult> {
  return runTruthCreditCase({
    id: 'truth_credit_discogs_unsupported_role',
    creditName: 'Peter Saville',
    creditRole: 'design_studio',
    query: 'Peter Saville design studio',
    expectedAttempted: false,
    expectedImported: 0,
    expectedSkippedReason: 'unsupported_role',
  });
}

async function runNoRowsCase(): Promise<TruthCreditCaseResult> {
  return runTruthCreditCase({
    id: 'truth_credit_discogs_no_rows',
    creditName: 'NoRowsCandidate XQZV 7F13D6',
    creditRole: 'producer',
    query: 'NoRowsCandidate XQZV 7F13D6 producer',
    limit: 5,
    expectedAttempted: true,
    expectedImported: 0,
    expectedSkippedReason: 'no_rows',
  });
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  const networkEnabled = process.env.ENABLE_NETWORK_EVAL === 'true';

  if (!networkEnabled) {
    console.log('[eval:truth-credit] skipped (set ENABLE_NETWORK_EVAL=true to run network eval)');
    return;
  }

  const results: TruthCreditCaseResult[] = [
    await runProducerCase(),
    await runEngineerCase(),
    await runArrangerCase(),
    await runUnsupportedRoleCase(),
    await runNoRowsCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;
  console.log('[eval:truth-credit] Truth credit harness');
  console.log(`[eval:truth-credit] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:truth-credit] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error('[eval:truth-credit] failed:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
