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
    recordingEquipmentEvidence?: number;
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

function readText(filePath: string): string {
  try {
    return fs.readFileSync(filePath, 'utf-8');
  } catch {
    return '';
  }
}

function num(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function run(): void {
  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });

  const enrich = readJson<EnrichReport>(path.join(artifactsDir, 'enrich-evidence.json'));
  const reasonQuality = readJson<ReasonQualityReport>(path.join(artifactsDir, 'reason-quality.json'));
  const trend = readText(path.join(artifactsDir, 'trend-summary.md'));
  const brief = readText(path.join(artifactsDir, 'pr-summary-brief.md'));

  const reasonQualityFailed = num(reasonQuality?.failed) > 0 || String(reasonQuality?.status || '').toUpperCase() === 'FAIL';
  const status = /\bATTN\b|\bFAIL\b/i.test(`${trend}\n${brief}`) || reasonQualityFailed ? 'ATTN' : 'PASS';
  const badge = status === 'PASS' ? '🟢 PASS' : '🟠 ATTN';

  const counts = {
    playlists: num(enrich?.evidenceCounts?.playlists),
    systemPlaylists: num(enrich?.evidenceCounts?.systemPlaylists),
    userPlaylists: num(enrich?.evidenceCounts?.userPlaylists),
    recordings: num(enrich?.evidenceCounts?.recordings),
    studioEvidence: num(enrich?.evidenceCounts?.recordingStudioEvidence),
    creditEvidence: num(enrich?.evidenceCounts?.recordingCreditEvidence),
    equipmentEvidence: num(enrich?.evidenceCounts?.recordingEquipmentEvidence),
    membershipEvidence: num(enrich?.evidenceCounts?.artistMembershipEvidence),
  };

  const payload = {
    generatedAt: new Date().toISOString(),
    sourceGeneratedAt: enrich?.generatedAt || null,
    status,
    badge,
    counts,
    reasonQuality: {
      status: String(reasonQuality?.status || ''),
      minUnique: num(reasonQuality?.minUnique),
      maxDup: num(reasonQuality?.maxDup),
      failed: num(reasonQuality?.failed),
    },
  };

  const jsonPath = path.join(artifactsDir, 'quality-status.json');
  const mdPath = path.join(artifactsDir, 'quality-status.md');

  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 2));
  fs.writeFileSync(
    mdPath,
    [
      '# Quality Status',
      '',
      `Status: ${badge}`,
      `Generated: ${payload.generatedAt}`,
      '',
      `Counts: playlists=${counts.playlists}, system_playlists=${counts.systemPlaylists}, user_playlists=${counts.userPlaylists}, recordings=${counts.recordings}, studio=${counts.studioEvidence}, credit=${counts.creditEvidence}, equipment=${counts.equipmentEvidence}, membership=${counts.membershipEvidence}`,
      `Reason quality: status=${payload.reasonQuality.status || 'n/a'}, min_unique=${payload.reasonQuality.minUnique}, max_dup=${payload.reasonQuality.maxDup}, failed=${payload.reasonQuality.failed}`,
      '',
    ].join('\n')
  );

  console.log('[eval:status] generated');
  console.log(`[eval:status] json_path=${jsonPath}`);
  console.log(`[eval:status] markdown_path=${mdPath}`);
  console.log(`[eval:status] one_line status=${status} playlists=${counts.playlists} system_playlists=${counts.systemPlaylists} user_playlists=${counts.userPlaylists} recordings=${counts.recordings} studio=${counts.studioEvidence} credit=${counts.creditEvidence} equipment=${counts.equipmentEvidence} membership=${counts.membershipEvidence} reason_max_dup=${payload.reasonQuality.maxDup} reason_min_unique=${payload.reasonQuality.minUnique}`);
}

run();
