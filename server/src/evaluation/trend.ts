import fs from 'fs';
import path from 'path';

interface EnrichReport {
  generatedAt: string;
  evidenceCounts?: {
    playlists?: number;
    systemPlaylists?: number;
    userPlaylists?: number;
    recordings?: number;
    recordingStudioEvidence?: number;
    recordingCreditEvidence?: number;
    artistMembershipEvidence?: number;
  };
}

interface ReasonQualityReport {
  generatedAt?: string;
  status?: string;
  minUnique?: number;
  maxDup?: number;
  failed?: number;
}

function readJson<T>(filePath: string): T | null {
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    const parsed = JSON.parse(raw) as T;
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function num(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function readNumberEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(0, Math.floor(value));
}

function isMainlineBranch(branchName: string): boolean {
  const normalized = branchName.trim().toLowerCase();
  return normalized === 'main' || normalized === 'master';
}

function resolveThreshold(baseName: string, fallback: number, isMainline: boolean): number {
  if (isMainline) {
    return readNumberEnv(baseName, fallback);
  }
  return readNumberEnv(`${baseName}_NON_MAIN`, readNumberEnv(baseName, fallback));
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const currentPath = path.resolve(process.cwd(), 'eval-artifacts', 'enrich-evidence.json');
  const previousPath = process.env.ENRICH_PREVIOUS_REPORT_PATH
    ? path.resolve(process.cwd(), process.env.ENRICH_PREVIOUS_REPORT_PATH)
    : path.resolve(process.cwd(), 'eval-artifacts', 'last-enrich-evidence.json');
  const reasonQualityCurrentPath = path.resolve(process.cwd(), 'eval-artifacts', 'reason-quality.json');
  const reasonQualityPreviousPath = process.env.REASON_QUALITY_PREVIOUS_REPORT_PATH
    ? path.resolve(process.cwd(), process.env.REASON_QUALITY_PREVIOUS_REPORT_PATH)
    : path.resolve(process.cwd(), 'eval-artifacts', 'last-reason-quality.json');

  const current = readJson<EnrichReport>(currentPath);
  if (!current || !current.evidenceCounts) {
    console.error(`[eval:trend] Missing current enrich report: ${currentPath}`);
    process.exitCode = 1;
    return;
  }

  const previous = readJson<EnrichReport>(previousPath);
  if (!previous || !previous.evidenceCounts) {
    console.log(`[eval:trend] SKIP no previous enrich report at ${previousPath}`);
    return;
  }

  const branchName = process.env.GITHUB_REF_NAME || 'local';
  const mainline = isMainlineBranch(branchName);
  const maxMembershipDrop = resolveThreshold('MAX_TREND_DROP_MEMBERSHIP', 0, mainline);
  const maxCreditDrop = resolveThreshold('MAX_TREND_DROP_CREDIT', 0, mainline);
  const maxStudioDrop = resolveThreshold('MAX_TREND_DROP_STUDIO', 0, mainline);
  const maxRecordingsDrop = resolveThreshold('MAX_TREND_DROP_RECORDINGS', 0, mainline);
  const maxPlaylistsDrop = resolveThreshold('MAX_TREND_DROP_PLAYLISTS', 0, mainline);
  const maxUserPlaylistsDrop = resolveThreshold('MAX_TREND_DROP_USER_PLAYLISTS', 0, mainline);
  const maxUserPlaylistsRise = resolveThreshold('MAX_TREND_RISE_USER_PLAYLISTS', 999999, mainline);
  const maxSystemPlaylistRise = resolveThreshold('MAX_TREND_RISE_SYSTEM_PLAYLISTS', 0, mainline);
  const maxReasonMaxDupRise = resolveThreshold('MAX_TREND_RISE_REASON_MAX_DUP', 0, mainline);
  const maxReasonMinUniqueDrop = resolveThreshold('MAX_TREND_DROP_REASON_MIN_UNIQUE', 0, mainline);
  const hasSystemPlaylistsCurrent = typeof current.evidenceCounts.systemPlaylists === 'number';
  const hasSystemPlaylistsPrevious = typeof previous.evidenceCounts.systemPlaylists === 'number';
  const canCompareSystemPlaylists = hasSystemPlaylistsCurrent && hasSystemPlaylistsPrevious;
  const hasUserPlaylistsCurrent = typeof current.evidenceCounts.userPlaylists === 'number';
  const hasUserPlaylistsPrevious = typeof previous.evidenceCounts.userPlaylists === 'number';
  const canCompareUserPlaylists = hasUserPlaylistsCurrent && hasUserPlaylistsPrevious;

  const membershipDelta = num(current.evidenceCounts.artistMembershipEvidence) - num(previous.evidenceCounts.artistMembershipEvidence);
  const creditDelta = num(current.evidenceCounts.recordingCreditEvidence) - num(previous.evidenceCounts.recordingCreditEvidence);
  const studioDelta = num(current.evidenceCounts.recordingStudioEvidence) - num(previous.evidenceCounts.recordingStudioEvidence);
  const recordingsDelta = num(current.evidenceCounts.recordings) - num(previous.evidenceCounts.recordings);
  const playlistsDelta = num(current.evidenceCounts.playlists) - num(previous.evidenceCounts.playlists);
  const userPlaylistsDelta = canCompareUserPlaylists
    ? num(current.evidenceCounts.userPlaylists) - num(previous.evidenceCounts.userPlaylists)
    : 0;
  const systemPlaylistsDelta = canCompareSystemPlaylists
    ? num(current.evidenceCounts.systemPlaylists) - num(previous.evidenceCounts.systemPlaylists)
    : 0;

  const reasonQualityCurrent = readJson<ReasonQualityReport>(reasonQualityCurrentPath);
  const reasonQualityPrevious = readJson<ReasonQualityReport>(reasonQualityPreviousPath);
  const canCompareReasonQuality = Boolean(reasonQualityCurrent && reasonQualityPrevious);
  const reasonMaxDupDelta = canCompareReasonQuality
    ? num(reasonQualityCurrent?.maxDup) - num(reasonQualityPrevious?.maxDup)
    : 0;
  const reasonMinUniqueDelta = canCompareReasonQuality
    ? num(reasonQualityCurrent?.minUnique) - num(reasonQualityPrevious?.minUnique)
    : 0;

  console.log('[eval:trend] Evidence trend comparison');
  console.log(`[eval:trend] branch=${branchName} mode=${mainline ? 'mainline' : 'feature'}`);
  console.log(`[eval:trend] previous=${previous.generatedAt || 'unknown'} current=${current.generatedAt || 'unknown'}`);
  console.log(`[eval:trend] artist_membership_evidence_delta=${membershipDelta}`);
  console.log(`[eval:trend] recording_credit_evidence_delta=${creditDelta}`);
  console.log(`[eval:trend] recording_studio_evidence_delta=${studioDelta}`);
  console.log(`[eval:trend] recordings_delta=${recordingsDelta}`);
  console.log(`[eval:trend] playlists_delta=${playlistsDelta}`);
  console.log(`[eval:trend] user_playlists_delta=${canCompareUserPlaylists ? String(userPlaylistsDelta) : 'n/a'}`);
  console.log(`[eval:trend] system_playlists_delta=${canCompareSystemPlaylists ? String(systemPlaylistsDelta) : 'n/a'}`);
  console.log(`[eval:trend] reason_max_dup_delta=${canCompareReasonQuality ? String(reasonMaxDupDelta) : 'n/a'}`);
  console.log(`[eval:trend] reason_min_unique_delta=${canCompareReasonQuality ? String(reasonMinUniqueDelta) : 'n/a'}`);
  console.log(`[eval:trend] summary membership=${membershipDelta} credit=${creditDelta} studio=${studioDelta} recordings=${recordingsDelta} playlists=${playlistsDelta} user_playlists=${canCompareUserPlaylists ? String(userPlaylistsDelta) : 'n/a'} system_playlists=${canCompareSystemPlaylists ? String(systemPlaylistsDelta) : 'n/a'} reason_max_dup=${canCompareReasonQuality ? String(reasonMaxDupDelta) : 'n/a'} reason_min_unique=${canCompareReasonQuality ? String(reasonMinUniqueDelta) : 'n/a'}`);

  const failures: string[] = [];
  if (membershipDelta < -maxMembershipDrop) {
    failures.push(`artist_membership_evidence delta ${membershipDelta} < -${maxMembershipDrop}`);
  }
  if (creditDelta < -maxCreditDrop) {
    failures.push(`recording_credit_evidence delta ${creditDelta} < -${maxCreditDrop}`);
  }
  if (studioDelta < -maxStudioDrop) {
    failures.push(`recording_studio_evidence delta ${studioDelta} < -${maxStudioDrop}`);
  }
  if (recordingsDelta < -maxRecordingsDrop) {
    failures.push(`recordings delta ${recordingsDelta} < -${maxRecordingsDrop}`);
  }
  if (playlistsDelta < -maxPlaylistsDrop) {
    failures.push(`playlists delta ${playlistsDelta} < -${maxPlaylistsDrop}`);
  }
  if (canCompareUserPlaylists && userPlaylistsDelta < -maxUserPlaylistsDrop) {
    failures.push(`user_playlists delta ${userPlaylistsDelta} < -${maxUserPlaylistsDrop}`);
  }
  if (canCompareUserPlaylists && userPlaylistsDelta > maxUserPlaylistsRise) {
    failures.push(`user_playlists delta ${userPlaylistsDelta} > ${maxUserPlaylistsRise}`);
  }
  if (canCompareSystemPlaylists && systemPlaylistsDelta > maxSystemPlaylistRise) {
    failures.push(`system_playlists delta ${systemPlaylistsDelta} > ${maxSystemPlaylistRise}`);
  }
  if (canCompareReasonQuality && reasonMaxDupDelta > maxReasonMaxDupRise) {
    failures.push(`reason_max_dup delta ${reasonMaxDupDelta} > ${maxReasonMaxDupRise}`);
  }
  if (canCompareReasonQuality && reasonMinUniqueDelta < -maxReasonMinUniqueDrop) {
    failures.push(`reason_min_unique delta ${reasonMinUniqueDelta} < -${maxReasonMinUniqueDrop}`);
  }

  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });
  const summaryPath = path.join(artifactsDir, 'trend-summary.md');
  const markdown = [
    '# Evidence Trend Summary',
    '',
    `Branch: ${branchName}`,
    `Mode: ${mainline ? 'mainline' : 'feature'}`,
    '',
    `Previous report: ${previous.generatedAt || 'unknown'}`,
    `Current report: ${current.generatedAt || 'unknown'}`,
    '',
    '## Deltas',
    '',
    `- artist_membership_evidence: ${membershipDelta}`,
    `- recording_credit_evidence: ${creditDelta}`,
    `- recording_studio_evidence: ${studioDelta}`,
    `- recordings: ${recordingsDelta}`,
    `- playlists: ${playlistsDelta}`,
    `- user_playlists: ${canCompareUserPlaylists ? String(userPlaylistsDelta) : 'n/a'}`,
    `- system_playlists: ${canCompareSystemPlaylists ? String(systemPlaylistsDelta) : 'n/a'}`,
    `- reason_max_dup: ${canCompareReasonQuality ? String(reasonMaxDupDelta) : 'n/a'}`,
    `- reason_min_unique: ${canCompareReasonQuality ? String(reasonMinUniqueDelta) : 'n/a'}`,
    '',
    '## Thresholds',
    '',
    `- MAX_TREND_DROP_MEMBERSHIP: ${maxMembershipDrop}`,
    `- MAX_TREND_DROP_CREDIT: ${maxCreditDrop}`,
    `- MAX_TREND_DROP_STUDIO: ${maxStudioDrop}`,
    `- MAX_TREND_DROP_RECORDINGS: ${maxRecordingsDrop}`,
    `- MAX_TREND_DROP_PLAYLISTS: ${maxPlaylistsDrop}`,
    `- MAX_TREND_DROP_USER_PLAYLISTS: ${maxUserPlaylistsDrop}`,
    `- MAX_TREND_RISE_USER_PLAYLISTS: ${maxUserPlaylistsRise}`,
    `- MAX_TREND_RISE_SYSTEM_PLAYLISTS: ${maxSystemPlaylistRise}`,
    `- MAX_TREND_RISE_REASON_MAX_DUP: ${maxReasonMaxDupRise}`,
    `- MAX_TREND_DROP_REASON_MIN_UNIQUE: ${maxReasonMinUniqueDrop}`,
    '',
    failures.length > 0 ? '## Status\n\nFAIL' : '## Status\n\nPASS',
    '',
  ].join('\n');
  fs.writeFileSync(summaryPath, markdown);
  console.log(`[eval:trend] summary_path=${summaryPath}`);

  if (strict && failures.length > 0) {
    for (const failure of failures) {
      console.error(`[eval:trend] FAIL ${failure}`);
    }
    process.exitCode = 1;
    return;
  }

  if (failures.length > 0) {
    for (const failure of failures) {
      console.warn(`[eval:trend] WARN ${failure}`);
    }
  }

  console.log('[eval:trend] PASS trend checks');
}

run();
