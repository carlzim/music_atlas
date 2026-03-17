import { CI_STRICT_PROFILE } from './ci-profile.js';

type Profile = Record<string, string>;

function readInt(profile: Profile, key: string): number {
  const raw = profile[key];
  if (typeof raw !== 'string' || raw.trim().length === 0) {
    throw new Error(`Missing profile key: ${key}`);
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error(`Profile key must be integer: ${key}=${raw}`);
  }
  return value;
}

function assertNonMainFloorNotStricter(profile: Profile, baseKey: string): void {
  const nonMainKey = `${baseKey}_NON_MAIN`;
  if (!(baseKey in profile) || !(nonMainKey in profile)) return;

  const mainValue = readInt(profile, baseKey);
  const nonMainValue = readInt(profile, nonMainKey);
  if (nonMainValue > mainValue) {
    throw new Error(`Non-main floor stricter than main: ${nonMainKey}=${nonMainValue} > ${baseKey}=${mainValue}`);
  }
}

function assertNonMainCeilingNotStricter(profile: Profile, baseKey: string): void {
  const nonMainKey = `${baseKey}_NON_MAIN`;
  if (!(baseKey in profile) || !(nonMainKey in profile)) return;

  const mainValue = readInt(profile, baseKey);
  const nonMainValue = readInt(profile, nonMainKey);
  if (nonMainValue < mainValue) {
    throw new Error(`Non-main ceiling stricter than main: ${nonMainKey}=${nonMainValue} < ${baseKey}=${mainValue}`);
  }
}

function run(): void {
  const profile = CI_STRICT_PROFILE;

  const requiredKeys = [
    'ENRICH_PREVIOUS_REPORT_PATH',
    'REASON_QUALITY_PREVIOUS_REPORT_PATH',
    'MAX_TREND_DROP_MEMBERSHIP',
    'MAX_TREND_DROP_CREDIT',
    'MAX_TREND_DROP_STUDIO',
    'MAX_TREND_DROP_RECORDINGS',
    'MAX_TREND_DROP_PLAYLISTS',
    'MAX_TREND_DROP_USER_PLAYLISTS',
    'MAX_TREND_RISE_USER_PLAYLISTS',
    'MAX_TREND_RISE_SYSTEM_PLAYLISTS',
    'MAX_TREND_RISE_REASON_MAX_DUP',
    'MAX_TREND_DROP_REASON_MIN_UNIQUE',
    'MIN_ARTIST_MEMBERSHIP_EVIDENCE',
    'MIN_RECORDING_CREDIT_EVIDENCE',
    'MIN_RECORDING_STUDIO_EVIDENCE',
    'MIN_RECORDING_EQUIPMENT_EVIDENCE',
    'MIN_ATLAS_EDGES',
    'MIN_ATLAS_NODE_STATS',
    'MIN_DISTINCT_CREDIT_NAMES',
    'MIN_DISTINCT_STUDIO_NAMES',
    'MIN_DISTINCT_MEMBERSHIP_BANDS',
    'MIN_DISTINCT_MEMBERSHIP_PEOPLE',
    'MIN_DISTINCT_EQUIPMENT_NAMES',
    'MIN_DISTINCT_EQUIPMENT_CATEGORIES',
    'MIN_RECORDINGS_WITH_CREDIT_EVIDENCE',
    'MIN_RECORDINGS_WITH_STUDIO_EVIDENCE',
    'MIN_RECORDINGS_WITH_EQUIPMENT_EVIDENCE',
    'MIN_SYSTEM_PLAYLISTS',
    'MAX_SYSTEM_PLAYLISTS',
    'MAX_MEMBERSHIP_MISSING_CANONICAL',
    'MAX_MEMBERSHIP_CANONICAL_COLLISIONS',
    'MAX_CREDIT_CANONICAL_COLLISIONS',
    'MAX_STUDIO_CANONICAL_COLLISIONS',
    'MAX_STUDIO_ALIAS_GROUPS',
    'MIN_REASON_MIN_UNIQUE',
    'MAX_REASON_MAX_DUP',
  ];

  for (const key of requiredKeys) {
    if (!(key in profile)) {
      throw new Error(`Missing required profile key: ${key}`);
    }
  }

  const floorKeys = [
    'MIN_ARTIST_MEMBERSHIP_EVIDENCE',
    'MIN_RECORDING_CREDIT_EVIDENCE',
    'MIN_RECORDING_STUDIO_EVIDENCE',
    'MIN_RECORDING_EQUIPMENT_EVIDENCE',
    'MIN_ATLAS_EDGES',
    'MIN_ATLAS_NODE_STATS',
    'MIN_DISTINCT_CREDIT_NAMES',
    'MIN_DISTINCT_STUDIO_NAMES',
    'MIN_DISTINCT_MEMBERSHIP_BANDS',
    'MIN_DISTINCT_MEMBERSHIP_PEOPLE',
    'MIN_DISTINCT_EQUIPMENT_NAMES',
    'MIN_DISTINCT_EQUIPMENT_CATEGORIES',
    'MIN_RECORDINGS_WITH_CREDIT_EVIDENCE',
    'MIN_RECORDINGS_WITH_STUDIO_EVIDENCE',
    'MIN_RECORDINGS_WITH_EQUIPMENT_EVIDENCE',
    'MIN_SYSTEM_PLAYLISTS',
    'MIN_REASON_MIN_UNIQUE',
  ];

  const ceilingKeys = [
    'MAX_SYSTEM_PLAYLISTS',
    'MAX_TREND_DROP_MEMBERSHIP',
    'MAX_TREND_DROP_CREDIT',
    'MAX_TREND_DROP_STUDIO',
    'MAX_TREND_DROP_RECORDINGS',
    'MAX_TREND_DROP_PLAYLISTS',
    'MAX_TREND_DROP_USER_PLAYLISTS',
    'MAX_TREND_RISE_USER_PLAYLISTS',
    'MAX_TREND_RISE_SYSTEM_PLAYLISTS',
    'MAX_TREND_RISE_REASON_MAX_DUP',
    'MAX_TREND_DROP_REASON_MIN_UNIQUE',
    'MAX_MEMBERSHIP_MISSING_CANONICAL',
    'MAX_MEMBERSHIP_CANONICAL_COLLISIONS',
    'MAX_CREDIT_CANONICAL_COLLISIONS',
    'MAX_STUDIO_CANONICAL_COLLISIONS',
    'MAX_STUDIO_ALIAS_GROUPS',
    'MAX_REASON_MAX_DUP',
  ];

  for (const key of [...floorKeys, ...ceilingKeys]) {
    readInt(profile, key);
  }

  for (const key of floorKeys) {
    assertNonMainFloorNotStricter(profile, key);
  }

  for (const key of ceilingKeys) {
    assertNonMainCeilingNotStricter(profile, key);
  }

  console.log('[eval:profile:check] PASS profile is valid');
  console.log(`[eval:profile:check] keys=${Object.keys(profile).length}`);
}

try {
  run();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[eval:profile:check] FAIL ${message}`);
  process.exitCode = 1;
}
