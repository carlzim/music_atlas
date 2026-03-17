import fs from 'fs';
import path from 'path';

interface EnrichReport {
  generatedAt?: string;
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
  status?: string;
  minUnique?: number;
  maxDup?: number;
  failed?: number;
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

function readText(filePath: string): string {
  try {
    return fs.readFileSync(filePath, 'utf-8');
  } catch {
    return '';
  }
}

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const enrichPath = path.join(artifactsDir, 'enrich-evidence.json');
  const previousPath = path.join(artifactsDir, 'last-enrich-evidence.json');
  const trendPath = path.join(artifactsDir, 'trend-summary.md');
  const outputPath = path.join(artifactsDir, 'pr-summary.md');
  const briefOutputPath = path.join(artifactsDir, 'pr-summary-brief.md');

  const enrich = readJson<EnrichReport>(enrichPath);
  const previous = readJson<EnrichReport>(previousPath);
  const reasonQuality = readJson<ReasonQualityReport>(path.join(artifactsDir, 'reason-quality.json'));
  const trendSummary = readText(trendPath);

  const playlists = enrich?.evidenceCounts?.playlists ?? 0;
  const recordings = enrich?.evidenceCounts?.recordings ?? 0;
  const systemPlaylists = enrich?.evidenceCounts?.systemPlaylists ?? 0;
  const userPlaylists = enrich?.evidenceCounts?.userPlaylists ?? 0;
  const studioEvidence = enrich?.evidenceCounts?.recordingStudioEvidence ?? 0;
  const creditEvidence = enrich?.evidenceCounts?.recordingCreditEvidence ?? 0;
  const membershipEvidence = enrich?.evidenceCounts?.artistMembershipEvidence ?? 0;

  const previousPlaylists = num(previous?.evidenceCounts?.playlists);
  const previousRecordings = num(previous?.evidenceCounts?.recordings);
  const previousSystemPlaylists = num(previous?.evidenceCounts?.systemPlaylists);
  const previousUserPlaylists = num(previous?.evidenceCounts?.userPlaylists);
  const previousStudioEvidence = num(previous?.evidenceCounts?.recordingStudioEvidence);
  const previousCreditEvidence = num(previous?.evidenceCounts?.recordingCreditEvidence);
  const previousMembershipEvidence = num(previous?.evidenceCounts?.artistMembershipEvidence);

  const hasPrevious = Boolean(previous?.evidenceCounts);
  const deltaPlaylists = playlists - previousPlaylists;
  const deltaRecordings = recordings - previousRecordings;
  const deltaSystemPlaylists = systemPlaylists - previousSystemPlaylists;
  const deltaUserPlaylists = userPlaylists - previousUserPlaylists;
  const deltaStudioEvidence = studioEvidence - previousStudioEvidence;
  const deltaCreditEvidence = creditEvidence - previousCreditEvidence;
  const deltaMembershipEvidence = membershipEvidence - previousMembershipEvidence;

  const hasRegression = hasPrevious
    && (
      deltaPlaylists < 0
      || deltaRecordings < 0
      || deltaStudioEvidence < 0
      || deltaCreditEvidence < 0
      || deltaMembershipEvidence < 0
      || deltaSystemPlaylists > 0
      || deltaUserPlaylists < 0
    );
  const reasonQualityFailed = num(reasonQuality?.failed) > 0 || String(reasonQuality?.status || '').toUpperCase() === 'FAIL';
  const status = hasRegression || reasonQualityFailed ? 'ATTN' : 'PASS';

  const deltaText = (value: number): string => {
    if (value > 0) return `+${value}`;
    return String(value);
  };

  const markdown = [
    '# Quality Snapshot',
    '',
    `Generated: ${enrich?.generatedAt || new Date().toISOString()}`,
    '',
    '## Key Counts',
    '',
    `- playlists: ${playlists}`,
    `- system_playlists: ${systemPlaylists}`,
    `- user_playlists: ${userPlaylists}`,
    `- recordings: ${recordings}`,
    `- recording_studio_evidence: ${studioEvidence}`,
    `- recording_credit_evidence: ${creditEvidence}`,
    `- artist_membership_evidence: ${membershipEvidence}`,
    `- reason_quality_status: ${String(reasonQuality?.status || 'n/a')}`,
    `- reason_quality_min_unique: ${num(reasonQuality?.minUnique)}`,
    `- reason_quality_max_dup: ${num(reasonQuality?.maxDup)}`,
    '',
    '## Delta vs Previous',
    '',
    hasPrevious ? `- playlists: ${deltaText(deltaPlaylists)}` : '- playlists: n/a',
    hasPrevious ? `- recordings: ${deltaText(deltaRecordings)}` : '- recordings: n/a',
    hasPrevious ? `- system_playlists: ${deltaText(deltaSystemPlaylists)}` : '- system_playlists: n/a',
    hasPrevious ? `- user_playlists: ${deltaText(deltaUserPlaylists)}` : '- user_playlists: n/a',
    hasPrevious ? `- recording_studio_evidence: ${deltaText(deltaStudioEvidence)}` : '- recording_studio_evidence: n/a',
    hasPrevious ? `- recording_credit_evidence: ${deltaText(deltaCreditEvidence)}` : '- recording_credit_evidence: n/a',
    hasPrevious ? `- artist_membership_evidence: ${deltaText(deltaMembershipEvidence)}` : '- artist_membership_evidence: n/a',
    '',
    '## Trend',
    '',
    trendSummary.trim().length > 0 ? trendSummary : '_No trend summary available._',
    '',
  ].join('\n');

  const briefMarkdown = [
    '# PR Quality Brief',
    '',
    `Status: ${status}`,
    `Counts: playlists=${playlists}, system_playlists=${systemPlaylists}, user_playlists=${userPlaylists}, recordings=${recordings}, studio_evidence=${studioEvidence}, credit_evidence=${creditEvidence}, membership_evidence=${membershipEvidence}`,
    `Reason quality: status=${String(reasonQuality?.status || 'n/a')}, min_unique=${num(reasonQuality?.minUnique)}, max_dup=${num(reasonQuality?.maxDup)}, failed=${num(reasonQuality?.failed)}`,
    hasPrevious
      ? `Delta: playlists=${deltaText(deltaPlaylists)}, system_playlists=${deltaText(deltaSystemPlaylists)}, user_playlists=${deltaText(deltaUserPlaylists)}, recordings=${deltaText(deltaRecordings)}, studio=${deltaText(deltaStudioEvidence)}, credit=${deltaText(deltaCreditEvidence)}, membership=${deltaText(deltaMembershipEvidence)}`
      : 'Delta: n/a (no previous report)',
    '',
  ].join('\n');

  fs.writeFileSync(outputPath, markdown);
  fs.writeFileSync(briefOutputPath, briefMarkdown);

  console.log('[eval:pr-summary] generated');
  console.log(`[eval:pr-summary] output_path=${outputPath}`);
  console.log(`[eval:pr-summary] brief_output_path=${briefOutputPath}`);
  console.log(`[eval:pr-summary] one_line status=${status} playlists=${playlists} system_playlists=${systemPlaylists} user_playlists=${userPlaylists} recordings=${recordings} studio=${studioEvidence} credit=${creditEvidence} membership=${membershipEvidence}`);
}

run();
