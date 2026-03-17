import { fetchDiscogsReleaseCreditsByQuery, isDiscogsConfigured } from './discogs.js';
import {
  completeTruthImportRun,
  createTruthImportRun,
  getTracksByTruthCreditClaim,
  upsertTruthCreditClaim,
  upsertTruthEntity,
} from './db.js';

export interface TruthCreditBackfillResult {
  attempted: boolean;
  imported: number;
  skippedReason?: string;
}

export const SUPPORTED_TRUTH_CREDIT_ROLES = new Set(['producer', 'engineer', 'arranger', 'art_director', 'photographer', 'cover_designer']);

const truthCreditCooldown = new Map<string, number>();
const TRUTH_CREDIT_COOLDOWN_MS = Math.max(
  0,
  Number.isFinite(Number(process.env.TRUTH_CREDIT_COOLDOWN_MS || '600000'))
    ? Number(process.env.TRUTH_CREDIT_COOLDOWN_MS || '600000')
    : 600000
);

function normalizeCooldownKey(name: string, role: string): string {
  return `${role.trim().toLowerCase()}::${name.trim().toLowerCase().replace(/\s+/g, ' ')}`;
}

function shouldBackfillNow(name: string, role: string): { allowed: boolean; reason?: string } {
  const key = normalizeCooldownKey(name, role);
  const now = Date.now();
  const last = truthCreditCooldown.get(key) || 0;
  if (last > 0 && now - last < TRUTH_CREDIT_COOLDOWN_MS) {
    return { allowed: false, reason: 'cooldown_active' };
  }
  truthCreditCooldown.set(key, now);
  return { allowed: true };
}

export async function backfillTruthCreditsFromDiscogs(params: {
  creditName: string;
  creditRole: string;
  query: string;
  limit?: number;
  force?: boolean;
}): Promise<TruthCreditBackfillResult> {
  const creditName = params.creditName.trim();
  const creditRole = params.creditRole.trim().toLowerCase();
  const query = params.query.trim();
  const limit = Math.max(1, Math.min(150, Math.floor(params.limit ?? 20)));

  if (!creditName || !creditRole || !query) {
    return { attempted: false, imported: 0, skippedReason: 'invalid_input' };
  }
  if (!SUPPORTED_TRUTH_CREDIT_ROLES.has(creditRole)) {
    return { attempted: false, imported: 0, skippedReason: 'unsupported_role' };
  }
  if (!isDiscogsConfigured()) {
    return { attempted: false, imported: 0, skippedReason: 'missing_discogs_token' };
  }

  if (!params.force) {
    const window = shouldBackfillNow(creditName, creditRole);
    if (!window.allowed) {
      return { attempted: false, imported: 0, skippedReason: window.reason || 'cooldown_active' };
    }
  }

  const importRunId = createTruthImportRun('discogs', 'credit', `${creditRole}:${creditName}`);
  try {
    const rows = await fetchDiscogsReleaseCreditsByQuery(query, creditName, creditRole, limit);
    if (rows.length === 0) {
      completeTruthImportRun(importRunId, 'success', { imported: 0, skipped: 'no_rows' });
      return { attempted: true, imported: 0, skippedReason: 'no_rows' };
    }

    const creditEntity = upsertTruthEntity('person', creditName);
    let imported = 0;
    for (const row of rows) {
      const ok = upsertTruthCreditClaim({
        artist: row.artist,
        title: row.title,
        creditName: row.creditName,
        creditRole: row.creditRole,
        source: 'discogs',
        sourceRef: row.sourceRef,
        confidence: 90,
        creditEntityId: creditEntity?.id ?? null,
      });
      if (ok) imported += 1;
    }

    completeTruthImportRun(importRunId, 'success', { imported, rows: rows.length });
    return { attempted: true, imported };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const skippedReason = /\(401\)|\(403\)/.test(message)
      ? 'discogs_auth_failed'
      : /\(429\)/.test(message)
        ? 'discogs_rate_limited'
        : 'request_failed';
    completeTruthImportRun(importRunId, 'error', {
      error: message,
    });
    return { attempted: true, imported: 0, skippedReason };
  }
}

export function getTruthCreditCandidates(name: string, role: string, limit = 50): Array<{ artist: string; title: string; source: string; confidence: number }> {
  return getTracksByTruthCreditClaim(name, role, limit);
}
