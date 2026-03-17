import 'dotenv/config';
import { getBandMembers } from '../services/db.js';
import { syncTruthMembershipForBandName } from '../services/truth-layer.js';

interface TruthMembershipCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

async function runOasisMembershipCase(): Promise<TruthMembershipCaseResult> {
  const id = 'truth_membership_oasis_members';
  const sync = await syncTruthMembershipForBandName('Oasis');
  const members = getBandMembers('Oasis').map((value) => value.toLowerCase());
  const hasNoel = members.some((value) => value.includes('noel gallagher'));
  const hasLiam = members.some((value) => value.includes('liam gallagher'));
  const pass = hasNoel && hasLiam;
  return {
    id,
    pass,
    details: `imported=${sync.imported} members=${members.length} has_noel=${hasNoel} has_liam=${hasLiam}`,
  };
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  const networkEnabled = process.env.ENABLE_NETWORK_EVAL === 'true';

  if (!networkEnabled) {
    console.log('[eval:truth-membership] skipped (set ENABLE_NETWORK_EVAL=true to run network eval)');
    return;
  }

  const results: TruthMembershipCaseResult[] = [
    await runOasisMembershipCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;

  console.log('[eval:truth-membership] Truth membership harness');
  console.log(`[eval:truth-membership] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:truth-membership] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error('[eval:truth-membership] failed:', error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
