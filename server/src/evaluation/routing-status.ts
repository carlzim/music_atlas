import fs from 'fs';
import path from 'path';

interface RoutingStatus {
  generatedAt?: string;
  totals?: {
    calls?: number;
    success?: number;
    cachedSuccess?: number;
    fallback?: number;
    backfillAttempts?: number;
    backfillUsed?: number;
  };
  byIntent?: Record<string, number>;
  byMode?: Record<string, number>;
  byReasonCode?: Record<string, number>;
  backfillBySource?: Record<string, { attempts?: number; used?: number }>;
}

function readJson<T>(filePath: string): T | null {
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function num(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const inputPath = path.join(artifactsDir, 'routing-status.json');
  const outputPath = path.join(artifactsDir, 'routing-status-summary.md');
  const payload = readJson<RoutingStatus>(inputPath);

  const calls = num(payload?.totals?.calls);
  const success = num(payload?.totals?.success);
  const cachedSuccess = num(payload?.totals?.cachedSuccess);
  const fallback = num(payload?.totals?.fallback);
  const backfillAttempts = num(payload?.totals?.backfillAttempts);
  const backfillUsed = num(payload?.totals?.backfillUsed);
  const creditCalls = num(payload?.byIntent?.credit);
  const truthFirst = num(payload?.byMode?.['truth-first']);
  const designCreditReasons =
    num(payload?.byReasonCode?.credit_role_detected);

  const markdown = [
    '# Routing Status Summary',
    '',
    `Generated: ${payload?.generatedAt || new Date().toISOString()}`,
    `calls=${calls} success=${success} cached_success=${cachedSuccess} fallback=${fallback} backfill_attempts=${backfillAttempts} backfill_used=${backfillUsed}`,
    `credit_calls=${creditCalls} truth_first_calls=${truthFirst} credit_reason_hits=${designCreditReasons}`,
    '',
  ].join('\n');

  fs.writeFileSync(outputPath, markdown);
  console.log('[eval:routing:status] generated');
  console.log(`[eval:routing:status] input_path=${inputPath}`);
  console.log(`[eval:routing:status] output_path=${outputPath}`);
  console.log(`[eval:routing:status] one_line calls=${calls} success=${success} cached_success=${cachedSuccess} fallback=${fallback} backfill_attempts=${backfillAttempts} backfill_used=${backfillUsed} credit_calls=${creditCalls} truth_first_calls=${truthFirst} credit_reason_hits=${designCreditReasons}`);
}

run();
