import fs from 'fs';
import path from 'path';
import type { PromptIntent, RouteDecision, RouteMode } from './prompt-routing.js';

type RouteSource = 'discogs' | 'musicbrainz';

interface RoutingStatus {
  generatedAt: string;
  totals: {
    calls: number;
    success: number;
    cachedSuccess: number;
    fallback: number;
    backfillAttempts: number;
    backfillUsed: number;
  };
  byIntent: Record<PromptIntent, number>;
  byMode: Record<RouteMode, number>;
  byReasonCode: Record<RouteDecision['reasonCode'], number>;
  fallbackReasons: Record<string, number>;
  backfillBySource: Record<RouteSource, { attempts: number; used: number }>;
}

const ARTIFACTS_DIR = path.resolve(process.cwd(), 'eval-artifacts');
const JSON_PATH = path.join(ARTIFACTS_DIR, 'routing-status.json');
const MD_PATH = path.join(ARTIFACTS_DIR, 'routing-status.md');

function createEmptyStatus(): RoutingStatus {
  return {
    generatedAt: new Date().toISOString(),
    totals: {
      calls: 0,
      success: 0,
      cachedSuccess: 0,
      fallback: 0,
      backfillAttempts: 0,
      backfillUsed: 0,
    },
    byIntent: {
      credit: 0,
      studio: 0,
      venue: 0,
      equipment: 0,
      'artist-discovery': 0,
      'abstract-mood': 0,
      unknown: 0,
    },
    byMode: {
      'truth-first': 0,
      hybrid: 0,
      'gemini-first': 0,
    },
    byReasonCode: {
      credit_role_detected: 0,
      studio_cue_detected: 0,
      venue_cue_detected: 0,
      equipment_cue_detected: 0,
      artist_discovery_cue_detected: 0,
      abstract_mood_cue_detected: 0,
      empty_prompt: 0,
      no_factual_intent_detected: 0,
    },
    fallbackReasons: {},
    backfillBySource: {
      discogs: { attempts: 0, used: 0 },
      musicbrainz: { attempts: 0, used: 0 },
    },
  };
}

function readStatus(): RoutingStatus {
  try {
    const raw = fs.readFileSync(JSON_PATH, 'utf-8');
    const parsed = JSON.parse(raw) as Partial<RoutingStatus>;
    const base = createEmptyStatus();
    return {
      generatedAt: typeof parsed.generatedAt === 'string' ? parsed.generatedAt : base.generatedAt,
      totals: {
        calls: Number(parsed.totals?.calls || 0),
        success: Number(parsed.totals?.success || 0),
        cachedSuccess: Number(parsed.totals?.cachedSuccess || 0),
        fallback: Number(parsed.totals?.fallback || 0),
        backfillAttempts: Number(parsed.totals?.backfillAttempts || 0),
        backfillUsed: Number(parsed.totals?.backfillUsed || 0),
      },
      byIntent: {
        ...base.byIntent,
        ...(parsed.byIntent || {}),
      },
      byMode: {
        ...base.byMode,
        ...(parsed.byMode || {}),
      },
      byReasonCode: {
        ...base.byReasonCode,
        ...(parsed.byReasonCode || {}),
      },
      fallbackReasons: {
        ...(parsed.fallbackReasons || {}),
      },
      backfillBySource: {
        discogs: {
          attempts: Number(parsed.backfillBySource?.discogs?.attempts || 0),
          used: Number(parsed.backfillBySource?.discogs?.used || 0),
        },
        musicbrainz: {
          attempts: Number(parsed.backfillBySource?.musicbrainz?.attempts || 0),
          used: Number(parsed.backfillBySource?.musicbrainz?.used || 0),
        },
      },
    };
  } catch {
    return createEmptyStatus();
  }
}

function writeStatus(status: RoutingStatus): void {
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
  status.generatedAt = new Date().toISOString();
  fs.writeFileSync(JSON_PATH, JSON.stringify(status, null, 2));
  fs.writeFileSync(
    MD_PATH,
    [
      '# Routing Status',
      '',
      `Generated: ${status.generatedAt}`,
      `calls=${status.totals.calls} success=${status.totals.success} cached_success=${status.totals.cachedSuccess} fallback=${status.totals.fallback} backfill_attempts=${status.totals.backfillAttempts} backfill_used=${status.totals.backfillUsed}`,
      `modes truth_first=${status.byMode['truth-first']} hybrid=${status.byMode.hybrid} gemini_first=${status.byMode['gemini-first']}`,
      `reasons credit_role=${status.byReasonCode.credit_role_detected} studio_cue=${status.byReasonCode.studio_cue_detected} venue_cue=${status.byReasonCode.venue_cue_detected} equipment_cue=${status.byReasonCode.equipment_cue_detected} discovery_cue=${status.byReasonCode.artist_discovery_cue_detected} abstract_mood=${status.byReasonCode.abstract_mood_cue_detected} empty=${status.byReasonCode.empty_prompt} no_factual=${status.byReasonCode.no_factual_intent_detected}`,
      `intents credit=${status.byIntent.credit} studio=${status.byIntent.studio} venue=${status.byIntent.venue} equipment=${status.byIntent.equipment} artist_discovery=${status.byIntent['artist-discovery']} abstract_mood=${status.byIntent['abstract-mood']} unknown=${status.byIntent.unknown}`,
      `backfill_sources discogs_attempts=${status.backfillBySource.discogs.attempts} discogs_used=${status.backfillBySource.discogs.used} musicbrainz_attempts=${status.backfillBySource.musicbrainz.attempts} musicbrainz_used=${status.backfillBySource.musicbrainz.used}`,
      '',
    ].join('\n')
  );
}

function mutate(mutator: (status: RoutingStatus) => void): void {
  try {
    const status = readStatus();
    mutator(status);
    writeStatus(status);
  } catch {
    // Observability is best-effort and must never break request flow.
  }
}

export function recordRoutingCall(decision: RouteDecision): void {
  mutate((status) => {
    status.totals.calls += 1;
    status.byIntent[decision.intent] = (status.byIntent[decision.intent] || 0) + 1;
    status.byMode[decision.mode] = (status.byMode[decision.mode] || 0) + 1;
    status.byReasonCode[decision.reasonCode] = (status.byReasonCode[decision.reasonCode] || 0) + 1;
  });
}

export function recordRoutingSuccess(cached: boolean): void {
  mutate((status) => {
    status.totals.success += 1;
    if (cached) status.totals.cachedSuccess += 1;
  });
}

export function recordRoutingFallback(reason: string): void {
  const normalizedReason = String(reason || '').trim().toLowerCase() || 'unknown';
  mutate((status) => {
    status.totals.fallback += 1;
    status.fallbackReasons[normalizedReason] = (status.fallbackReasons[normalizedReason] || 0) + 1;
  });
}

export function recordRoutingBackfill(source: RouteSource, used: boolean): void {
  mutate((status) => {
    status.totals.backfillAttempts += 1;
    status.backfillBySource[source].attempts += 1;
    if (used) {
      status.totals.backfillUsed += 1;
      status.backfillBySource[source].used += 1;
    }
  });
}
