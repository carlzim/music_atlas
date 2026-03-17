export type RouteMode = 'truth-first' | 'hybrid' | 'gemini-first';

export type PromptIntent =
  | 'credit'
  | 'studio'
  | 'venue'
  | 'equipment'
  | 'artist-discovery'
  | 'abstract-mood'
  | 'unknown';

export type CreditRole =
  | 'producer'
  | 'engineer'
  | 'arranger'
  | 'cover_designer'
  | 'art_director'
  | 'photographer';

export interface CreditIntentDetails {
  role: CreditRole;
  name?: string;
}

export interface RouteDecision {
  intent: PromptIntent;
  mode: RouteMode;
  confidence: 'high' | 'medium' | 'low';
  reason: string;
  reasonCode:
    | 'credit_role_detected'
    | 'studio_cue_detected'
    | 'venue_cue_detected'
    | 'equipment_cue_detected'
    | 'artist_discovery_cue_detected'
    | 'abstract_mood_cue_detected'
    | 'empty_prompt'
    | 'no_factual_intent_detected';
  credit?: CreditIntentDetails;
}

function parseRouteMode(value: string | undefined): RouteMode | null {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'truth-first' || normalized === 'hybrid' || normalized === 'gemini-first') {
    return normalized;
  }
  return null;
}

function getIntentModeOverride(intent: PromptIntent): { mode: RouteMode; key: string } | null {
  const keyByIntent: Record<PromptIntent, string> = {
    credit: 'ROUTE_CREDIT_MODE',
    studio: 'ROUTE_STUDIO_MODE',
    venue: 'ROUTE_VENUE_MODE',
    equipment: 'ROUTE_EQUIPMENT_MODE',
    'artist-discovery': 'ROUTE_ARTIST_DISCOVERY_MODE',
    'abstract-mood': 'ROUTE_ABSTRACT_MOOD_MODE',
    unknown: 'ROUTE_UNKNOWN_MODE',
  };

  const key = keyByIntent[intent];
  const fromIntent = parseRouteMode(process.env[key]);
  if (fromIntent) return { mode: fromIntent, key };

  const fromDefault = parseRouteMode(process.env.ROUTE_DEFAULT_MODE);
  if (fromDefault) return { mode: fromDefault, key: 'ROUTE_DEFAULT_MODE' };

  return null;
}

function applyModeOverride(decision: RouteDecision): RouteDecision {
  const override = getIntentModeOverride(decision.intent);
  if (!override || override.mode === decision.mode) return decision;
  return {
    ...decision,
    mode: override.mode,
    reason: `${decision.reason}; env override ${override.key}=${override.mode}`,
  };
}

const CREDIT_PATTERNS: Array<{ role: CreditRole; pattern: RegExp }> = [
  { role: 'cover_designer', pattern: /\bsleeve\s+design\s+by\b/i },
  { role: 'cover_designer', pattern: /\bcover\s+design\s+by\b/i },
  { role: 'cover_designer', pattern: /\bcover\s+art\s+by\b/i },
  { role: 'cover_designer', pattern: /\bdesigned\s+by\b/i },
  { role: 'cover_designer', pattern: /\bcover\s+designer\b/i },
  { role: 'art_director', pattern: /\bart\s+direction\s+by\b/i },
  { role: 'art_director', pattern: /\bart\s+director\b/i },
  { role: 'photographer', pattern: /\bphotography\s+by\b/i },
  { role: 'photographer', pattern: /\bphoto\s+by\b/i },
  { role: 'photographer', pattern: /\bphotographer\b/i },
  { role: 'producer', pattern: /\bproduced\s+by\b/i },
  { role: 'producer', pattern: /\bproducer\b/i },
  { role: 'engineer', pattern: /\bengineered\s+by\b/i },
  { role: 'engineer', pattern: /\bengineering\s+by\b/i },
  { role: 'engineer', pattern: /\bengineer\b/i },
  { role: 'arranger', pattern: /\barranged\s+by\b/i },
  { role: 'arranger', pattern: /\barranger\b/i },
  { role: 'arranger', pattern: /\barrangement\s+by\b/i },
];

function normalizePrompt(prompt: string): string {
  return String(prompt || '').replace(/\s+/g, ' ').trim();
}

function hasStudioCue(prompt: string): boolean {
  return /\bstudio\b|\bstudios\b|\brecorded\s+at\b|\brecorded\s+in\b|\btracked\s+at\b|\btracked\s+in\b|\bcut\s+at\b|\bcut\s+in\b|\bmade\s+at\b|\bmade\s+in\b/i.test(prompt);
}

function hasVenueCue(prompt: string): boolean {
  return /\blive\s+at\b|\blive\s+from\b|\blive\s+in\b|\bvenue\b|\bconcert\b|\bclub\b|\bhall\b|\btheatre\b|\btheater\b|\barena\b|\bfestival\b|\bcbgb\b/i.test(prompt);
}

function hasEquipmentCue(prompt: string): boolean {
  return /\bequipment\b|\bgear\b|\binstrument\b|\bsynth\b|\bsynthesizer\b|\bdrum\s+machine\b|\bpedal\b|\bconsole\b|\bmic\b|\bmicrophone\b|\bmellotron\b|\btr-?808\b|\bminimoog\b|\bneve\b|\bneumann\b/i.test(prompt);
}

function hasArtistDiscoveryCue(prompt: string): boolean {
  return /\bdiscover\b|\bunderrated\b|\bhidden\s+gems\b|\bdeep\s+cuts\b|\bartists?\s+like\b|\bsimilar\s+artists?\b|\bnew\s+artists?\b|\brecommend\b|\bexplore\b/i.test(prompt);
}

function hasAbstractMoodCue(prompt: string): boolean {
  return /\bmood\b|\bvibe\b|\batmospheric\b|\bchill\b|\bmelancholic\b|\brainy\b|\bnight\b|\bdreamy\b|\bnostalgic\b|\bhappy\b|\bsad\b|\benergy\b|\bworkout\b|\bfocus\b/i.test(prompt);
}

function extractCreditName(prompt: string): string | undefined {
  const match = prompt.match(/\b(?:produced|engineered|arranged|designed)\s+by\s+([^,.!?;]+)/i)
    || prompt.match(/\b(?:cover\s+art|cover\s+design|sleeve\s+design|art\s+direction|photography|photo)\s+by\s+([^,.!?;]+)/i)
    || prompt.match(/\b(?:producer|engineer|arranger|cover\s+designer|art\s+director|photographer)\s*[:\-]?\s*([^,.!?;]+)/i)
    || prompt.match(/\bwith\s+([^,.!?;]+?)\s+as\s+an?\s+(?:producer|engineer|arranger|cover\s+designer|art\s+director|photographer)\b/i);
  if (!match) return undefined;
  const cleaned = (match[1] || '').replace(/^the\s+/i, '').trim();
  if (!cleaned) return undefined;
  return cleaned;
}

export function detectCreditIntent(prompt: string): CreditIntentDetails | null {
  const normalized = normalizePrompt(prompt);
  if (!normalized) return null;

  for (const item of CREDIT_PATTERNS) {
    if (!item.pattern.test(normalized)) continue;
    return {
      role: item.role,
      name: extractCreditName(normalized),
    };
  }

  return null;
}

export function classifyPromptIntent(prompt: string): PromptIntent {
  const normalized = normalizePrompt(prompt);
  if (!normalized) return 'unknown';

  if (detectCreditIntent(normalized)) return 'credit';
  if (hasStudioCue(normalized)) return 'studio';
  if (hasVenueCue(normalized)) return 'venue';
  if (hasEquipmentCue(normalized)) return 'equipment';
  if (hasArtistDiscoveryCue(normalized)) return 'artist-discovery';
  if (hasAbstractMoodCue(normalized)) return 'abstract-mood';
  return 'unknown';
}

export function resolvePromptRoute(prompt: string): RouteDecision {
  const normalized = normalizePrompt(prompt);
  if (!normalized) {
    return applyModeOverride({
      intent: 'unknown',
      mode: 'gemini-first',
      confidence: 'low',
      reason: 'empty prompt',
      reasonCode: 'empty_prompt',
    });
  }

  const credit = detectCreditIntent(normalized);
  if (credit) {
    return applyModeOverride({
      intent: 'credit',
      mode: 'truth-first',
      confidence: 'high',
      reason: `credit role detected: ${credit.role}`,
      reasonCode: 'credit_role_detected',
      credit,
    });
  }

  if (hasStudioCue(normalized)) {
    return applyModeOverride({
      intent: 'studio',
      mode: 'hybrid',
      confidence: 'high',
      reason: 'studio cue detected',
      reasonCode: 'studio_cue_detected',
    });
  }

  if (hasVenueCue(normalized)) {
    return applyModeOverride({
      intent: 'venue',
      mode: 'hybrid',
      confidence: 'high',
      reason: 'venue cue detected',
      reasonCode: 'venue_cue_detected',
    });
  }

  if (hasEquipmentCue(normalized)) {
    return applyModeOverride({
      intent: 'equipment',
      mode: 'hybrid',
      confidence: 'medium',
      reason: 'equipment cue detected',
      reasonCode: 'equipment_cue_detected',
    });
  }

  if (hasArtistDiscoveryCue(normalized)) {
    return applyModeOverride({
      intent: 'artist-discovery',
      mode: 'gemini-first',
      confidence: 'medium',
      reason: 'artist discovery cue detected',
      reasonCode: 'artist_discovery_cue_detected',
    });
  }

  if (hasAbstractMoodCue(normalized)) {
    return applyModeOverride({
      intent: 'abstract-mood',
      mode: 'gemini-first',
      confidence: 'high',
      reason: 'abstract mood cue detected',
      reasonCode: 'abstract_mood_cue_detected',
    });
  }

  return applyModeOverride({
    intent: 'unknown',
    mode: 'gemini-first',
    confidence: 'low',
    reason: 'no factual intent detected',
    reasonCode: 'no_factual_intent_detected',
  });
}
