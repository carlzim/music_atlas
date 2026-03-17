import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

interface CountRow {
  count: number | null;
}

interface ReasonQualityReport {
  minUnique?: number;
  maxDup?: number;
}

function readNumberEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (typeof raw !== 'string' || raw.trim().length === 0) return fallback;
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

function readCount(db: Database.Database, table: string): number {
  const row = db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readSystemPlaylistCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM playlists
    WHERE prompt LIKE '[system] %'
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctCreditNameCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT COALESCE(credit_name_canonical, lower(trim(credit_name))) AS key
      FROM recording_credit_evidence
      WHERE COALESCE(credit_name_canonical, lower(trim(credit_name))) IS NOT NULL
        AND trim(COALESCE(credit_name_canonical, lower(trim(credit_name)))) != ''
    ) credits
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctStudioNameCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT COALESCE(studio_name_canonical, lower(trim(studio_name))) AS key
      FROM recording_studio_evidence
      WHERE COALESCE(studio_name_canonical, lower(trim(studio_name))) IS NOT NULL
        AND trim(COALESCE(studio_name_canonical, lower(trim(studio_name)))) != ''
    ) studios
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctMembershipBandCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT COALESCE(band_name_canonical, lower(trim(band_name))) AS key
      FROM artist_membership_evidence
      WHERE COALESCE(band_name_canonical, lower(trim(band_name))) IS NOT NULL
        AND trim(COALESCE(band_name_canonical, lower(trim(band_name)))) != ''
    ) bands
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctMembershipPersonCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT COALESCE(person_name_canonical, lower(trim(person_name))) AS key
      FROM artist_membership_evidence
      WHERE COALESCE(person_name_canonical, lower(trim(person_name))) IS NOT NULL
        AND trim(COALESCE(person_name_canonical, lower(trim(person_name)))) != ''
    ) people
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctRecordingCoverageCount(db: Database.Database, table: string): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT recording_id
      FROM ${table}
      WHERE recording_id IS NOT NULL
    ) coverage
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctEquipmentNameCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT lower(trim(equipment_name)) AS key
      FROM recording_equipment_evidence
      WHERE equipment_name IS NOT NULL
        AND trim(equipment_name) != ''
    ) equipment
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function readDistinctEquipmentCategoryCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT DISTINCT lower(trim(equipment_category)) AS key
      FROM recording_equipment_evidence
      WHERE equipment_category IS NOT NULL
        AND trim(equipment_category) != ''
    ) categories
  `).get() as CountRow;
  return typeof row?.count === 'number' ? row.count : 0;
}

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string | null }>;
  return rows.some((row) => row.name === column);
}

function normalizeStudioAliasKey(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/^the\s+/, '')
    .replace(/\brecording\s+studios?\b/g, 'studio')
    .replace(/\bstudios\b/g, 'studio')
    .replace(/\s+/g, ' ')
    .trim();
}

function readStudioAliasGroupCount(db: Database.Database): number {
  const rows = db.prepare(`
    SELECT studio_name AS name
    FROM recording_studio_evidence
    WHERE studio_name IS NOT NULL
      AND trim(studio_name) != ''
  `).all() as Array<{ name: string | null }>;

  const groups = new Map<string, Set<string>>();
  for (const row of rows) {
    const name = typeof row.name === 'string' ? row.name.trim() : '';
    if (!name) continue;

    const aliasKey = normalizeStudioAliasKey(name);
    if (!aliasKey) continue;

    const variants = groups.get(aliasKey) ?? new Set<string>();
    variants.add(name.toLowerCase());
    groups.set(aliasKey, variants);
  }

  let collisionGroups = 0;
  for (const variants of groups.values()) {
    if (variants.size > 1) {
      collisionGroups += 1;
    }
  }

  return collisionGroups;
}

function readReasonQualityMetrics(): { minUnique: number; maxDup: number } {
  const reportPath = path.resolve(process.cwd(), 'eval-artifacts', 'reason-quality.json');
  try {
    const raw = fs.readFileSync(reportPath, 'utf-8');
    const parsed = JSON.parse(raw) as ReasonQualityReport;
    const minUnique = typeof parsed.minUnique === 'number' && Number.isFinite(parsed.minUnique)
      ? Math.max(0, Math.floor(parsed.minUnique))
      : Number.NEGATIVE_INFINITY;
    const maxDup = typeof parsed.maxDup === 'number' && Number.isFinite(parsed.maxDup)
      ? Math.max(0, Math.floor(parsed.maxDup))
      : Number.POSITIVE_INFINITY;
    return { minUnique, maxDup };
  } catch {
    return { minUnique: Number.NEGATIVE_INFINITY, maxDup: Number.POSITIVE_INFINITY };
  }
}

function readMissingMembershipCanonicalCount(db: Database.Database): number {
  const hasBandCanonical = hasColumn(db, 'artist_membership_evidence', 'band_name_canonical');
  const hasPersonCanonical = hasColumn(db, 'artist_membership_evidence', 'person_name_canonical');
  if (!hasBandCanonical || !hasPersonCanonical) {
    return Number.POSITIVE_INFINITY;
  }

  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM artist_membership_evidence
    WHERE band_name_canonical IS NULL
      OR trim(band_name_canonical) = ''
      OR person_name_canonical IS NULL
      OR trim(person_name_canonical) = ''
  `).get() as CountRow;

  return typeof row?.count === 'number' ? row.count : 0;
}

function readMembershipCanonicalCollisions(db: Database.Database): number {
  const hasBandCanonical = hasColumn(db, 'artist_membership_evidence', 'band_name_canonical');
  const hasPersonCanonical = hasColumn(db, 'artist_membership_evidence', 'person_name_canonical');
  if (!hasBandCanonical || !hasPersonCanonical) return Number.POSITIVE_INFINITY;

  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT 1
      FROM artist_membership_evidence
      GROUP BY band_name_canonical, person_name_canonical
      HAVING COUNT(DISTINCT lower(trim(band_name)) || '::' || lower(trim(person_name))) > 1
    ) collisions
  `).get() as CountRow;

  return typeof row?.count === 'number' ? row.count : 0;
}

function readCreditCanonicalCollisions(db: Database.Database): number {
  const hasCreditCanonical = hasColumn(db, 'recording_credit_evidence', 'credit_name_canonical');
  if (!hasCreditCanonical) return Number.POSITIVE_INFINITY;

  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT 1
      FROM recording_credit_evidence
      GROUP BY credit_name_canonical
      HAVING COUNT(DISTINCT lower(trim(credit_name))) > 1
    ) collisions
  `).get() as CountRow;

  return typeof row?.count === 'number' ? row.count : 0;
}

function readStudioCanonicalCollisions(db: Database.Database): number {
  const hasStudioCanonical = hasColumn(db, 'recording_studio_evidence', 'studio_name_canonical');
  if (!hasStudioCanonical) return Number.POSITIVE_INFINITY;

  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM (
      SELECT 1
      FROM recording_studio_evidence
      GROUP BY studio_name_canonical
      HAVING COUNT(DISTINCT lower(trim(studio_name))) > 1
    ) collisions
  `).get() as CountRow;

  return typeof row?.count === 'number' ? row.count : 0;
}

function run(): void {
  const db = new Database('playlists.db');
  const branchName = process.env.GITHUB_REF_NAME || 'local';
  const mainline = isMainlineBranch(branchName);

  const minMembershipEvidence = resolveThreshold('MIN_ARTIST_MEMBERSHIP_EVIDENCE', 20, mainline);
  const minCreditEvidence = resolveThreshold('MIN_RECORDING_CREDIT_EVIDENCE', 0, mainline);
  const minStudioEvidence = resolveThreshold('MIN_RECORDING_STUDIO_EVIDENCE', 0, mainline);
  const minEquipmentEvidence = resolveThreshold('MIN_RECORDING_EQUIPMENT_EVIDENCE', 0, mainline);
  const minAtlasEdges = resolveThreshold('MIN_ATLAS_EDGES', 0, mainline);
  const minAtlasNodeStats = resolveThreshold('MIN_ATLAS_NODE_STATS', 0, mainline);
  const minSystemPlaylists = resolveThreshold('MIN_SYSTEM_PLAYLISTS', 0, mainline);
  const maxSystemPlaylists = resolveThreshold('MAX_SYSTEM_PLAYLISTS', 999999, mainline);
  const minDistinctCreditNames = resolveThreshold('MIN_DISTINCT_CREDIT_NAMES', 0, mainline);
  const minDistinctStudioNames = resolveThreshold('MIN_DISTINCT_STUDIO_NAMES', 0, mainline);
  const minDistinctMembershipBands = resolveThreshold('MIN_DISTINCT_MEMBERSHIP_BANDS', 0, mainline);
  const minDistinctMembershipPeople = resolveThreshold('MIN_DISTINCT_MEMBERSHIP_PEOPLE', 0, mainline);
  const minDistinctEquipmentNames = resolveThreshold('MIN_DISTINCT_EQUIPMENT_NAMES', 0, mainline);
  const minDistinctEquipmentCategories = resolveThreshold('MIN_DISTINCT_EQUIPMENT_CATEGORIES', 0, mainline);
  const minRecordingsWithCreditEvidence = resolveThreshold('MIN_RECORDINGS_WITH_CREDIT_EVIDENCE', 0, mainline);
  const minRecordingsWithStudioEvidence = resolveThreshold('MIN_RECORDINGS_WITH_STUDIO_EVIDENCE', 0, mainline);
  const minRecordingsWithEquipmentEvidence = resolveThreshold('MIN_RECORDINGS_WITH_EQUIPMENT_EVIDENCE', 0, mainline);
  const maxMissingMembershipCanonical = resolveThreshold('MAX_MEMBERSHIP_MISSING_CANONICAL', 0, mainline);
  const maxMembershipCanonicalCollisions = resolveThreshold('MAX_MEMBERSHIP_CANONICAL_COLLISIONS', 0, mainline);
  const maxCreditCanonicalCollisions = resolveThreshold('MAX_CREDIT_CANONICAL_COLLISIONS', 0, mainline);
  const maxStudioCanonicalCollisions = resolveThreshold('MAX_STUDIO_CANONICAL_COLLISIONS', 0, mainline);
  const maxStudioAliasGroups = resolveThreshold('MAX_STUDIO_ALIAS_GROUPS', 0, mainline);
  const minReasonMinUnique = resolveThreshold('MIN_REASON_MIN_UNIQUE', 0, mainline);
  const maxReasonMaxDup = resolveThreshold('MAX_REASON_MAX_DUP', 999999, mainline);

  const membershipEvidence = readCount(db, 'artist_membership_evidence');
  const creditEvidence = readCount(db, 'recording_credit_evidence');
  const studioEvidence = readCount(db, 'recording_studio_evidence');
  const equipmentEvidence = readCount(db, 'recording_equipment_evidence');
  const atlasEdges = readCount(db, 'atlas_edges');
  const atlasNodeStats = readCount(db, 'atlas_node_stats');
  const systemPlaylists = readSystemPlaylistCount(db);
  const distinctCreditNames = readDistinctCreditNameCount(db);
  const distinctStudioNames = readDistinctStudioNameCount(db);
  const distinctMembershipBands = readDistinctMembershipBandCount(db);
  const distinctMembershipPeople = readDistinctMembershipPersonCount(db);
  const distinctEquipmentNames = readDistinctEquipmentNameCount(db);
  const distinctEquipmentCategories = readDistinctEquipmentCategoryCount(db);
  const recordingsWithCreditEvidence = readDistinctRecordingCoverageCount(db, 'recording_credit_evidence');
  const recordingsWithStudioEvidence = readDistinctRecordingCoverageCount(db, 'recording_studio_evidence');
  const recordingsWithEquipmentEvidence = readDistinctRecordingCoverageCount(db, 'recording_equipment_evidence');
  const missingMembershipCanonical = readMissingMembershipCanonicalCount(db);
  const membershipCanonicalCollisions = readMembershipCanonicalCollisions(db);
  const creditCanonicalCollisions = readCreditCanonicalCollisions(db);
  const studioCanonicalCollisions = readStudioCanonicalCollisions(db);
  const studioAliasGroups = readStudioAliasGroupCount(db);
  const reasonQuality = readReasonQualityMetrics();

  const failures: string[] = [];

  if (membershipEvidence < minMembershipEvidence) {
    failures.push(`artist_membership_evidence ${membershipEvidence} < ${minMembershipEvidence}`);
  }

  if (creditEvidence < minCreditEvidence) {
    failures.push(`recording_credit_evidence ${creditEvidence} < ${minCreditEvidence}`);
  }

  if (studioEvidence < minStudioEvidence) {
    failures.push(`recording_studio_evidence ${studioEvidence} < ${minStudioEvidence}`);
  }

  if (equipmentEvidence < minEquipmentEvidence) {
    failures.push(`recording_equipment_evidence ${equipmentEvidence} < ${minEquipmentEvidence}`);
  }

  if (atlasEdges < minAtlasEdges) {
    failures.push(`atlas_edges ${atlasEdges} < ${minAtlasEdges}`);
  }

  if (atlasNodeStats < minAtlasNodeStats) {
    failures.push(`atlas_node_stats ${atlasNodeStats} < ${minAtlasNodeStats}`);
  }

  if (distinctCreditNames < minDistinctCreditNames) {
    failures.push(`distinct_credit_names ${distinctCreditNames} < ${minDistinctCreditNames}`);
  }

  if (distinctStudioNames < minDistinctStudioNames) {
    failures.push(`distinct_studio_names ${distinctStudioNames} < ${minDistinctStudioNames}`);
  }

  if (distinctMembershipBands < minDistinctMembershipBands) {
    failures.push(`distinct_membership_bands ${distinctMembershipBands} < ${minDistinctMembershipBands}`);
  }

  if (distinctMembershipPeople < minDistinctMembershipPeople) {
    failures.push(`distinct_membership_people ${distinctMembershipPeople} < ${minDistinctMembershipPeople}`);
  }

  if (distinctEquipmentNames < minDistinctEquipmentNames) {
    failures.push(`distinct_equipment_names ${distinctEquipmentNames} < ${minDistinctEquipmentNames}`);
  }

  if (distinctEquipmentCategories < minDistinctEquipmentCategories) {
    failures.push(`distinct_equipment_categories ${distinctEquipmentCategories} < ${minDistinctEquipmentCategories}`);
  }

  if (recordingsWithCreditEvidence < minRecordingsWithCreditEvidence) {
    failures.push(`recordings_with_credit_evidence ${recordingsWithCreditEvidence} < ${minRecordingsWithCreditEvidence}`);
  }

  if (recordingsWithStudioEvidence < minRecordingsWithStudioEvidence) {
    failures.push(`recordings_with_studio_evidence ${recordingsWithStudioEvidence} < ${minRecordingsWithStudioEvidence}`);
  }

  if (recordingsWithEquipmentEvidence < minRecordingsWithEquipmentEvidence) {
    failures.push(`recordings_with_equipment_evidence ${recordingsWithEquipmentEvidence} < ${minRecordingsWithEquipmentEvidence}`);
  }

  if (systemPlaylists < minSystemPlaylists) {
    failures.push(`system_playlists ${systemPlaylists} < ${minSystemPlaylists}`);
  }

  if (maxSystemPlaylists >= minSystemPlaylists && systemPlaylists > maxSystemPlaylists) {
    failures.push(`system_playlists ${systemPlaylists} > ${maxSystemPlaylists}`);
  }

  if (missingMembershipCanonical > maxMissingMembershipCanonical) {
    failures.push(`membership_missing_canonical ${missingMembershipCanonical} > ${maxMissingMembershipCanonical}`);
  }

  if (membershipCanonicalCollisions > maxMembershipCanonicalCollisions) {
    failures.push(`membership_canonical_collisions ${membershipCanonicalCollisions} > ${maxMembershipCanonicalCollisions}`);
  }

  if (creditCanonicalCollisions > maxCreditCanonicalCollisions) {
    failures.push(`credit_canonical_collisions ${creditCanonicalCollisions} > ${maxCreditCanonicalCollisions}`);
  }

  if (studioCanonicalCollisions > maxStudioCanonicalCollisions) {
    failures.push(`studio_canonical_collisions ${studioCanonicalCollisions} > ${maxStudioCanonicalCollisions}`);
  }

  if (studioAliasGroups > maxStudioAliasGroups) {
    failures.push(`studio_alias_groups ${studioAliasGroups} > ${maxStudioAliasGroups}`);
  }

  if (reasonQuality.minUnique < minReasonMinUnique) {
    failures.push(`reason_min_unique ${reasonQuality.minUnique} < ${minReasonMinUnique}`);
  }

  if (reasonQuality.maxDup > maxReasonMaxDup) {
    failures.push(`reason_max_dup ${reasonQuality.maxDup} > ${maxReasonMaxDup}`);
  }

  console.log('[eval:thresholds] Quality thresholds');
  console.log(`[eval:thresholds] branch=${branchName} mode=${mainline ? 'mainline' : 'feature'}`);
  console.log(`[eval:thresholds] artist_membership_evidence=${membershipEvidence} (min=${minMembershipEvidence})`);
  console.log(`[eval:thresholds] recording_credit_evidence=${creditEvidence} (min=${minCreditEvidence})`);
  console.log(`[eval:thresholds] recording_studio_evidence=${studioEvidence} (min=${minStudioEvidence})`);
  console.log(`[eval:thresholds] recording_equipment_evidence=${equipmentEvidence} (min=${minEquipmentEvidence})`);
  console.log(`[eval:thresholds] atlas_edges=${atlasEdges} (min=${minAtlasEdges})`);
  console.log(`[eval:thresholds] atlas_node_stats=${atlasNodeStats} (min=${minAtlasNodeStats})`);
  console.log(`[eval:thresholds] distinct_credit_names=${distinctCreditNames} (min=${minDistinctCreditNames})`);
  console.log(`[eval:thresholds] distinct_studio_names=${distinctStudioNames} (min=${minDistinctStudioNames})`);
  console.log(`[eval:thresholds] distinct_membership_bands=${distinctMembershipBands} (min=${minDistinctMembershipBands})`);
  console.log(`[eval:thresholds] distinct_membership_people=${distinctMembershipPeople} (min=${minDistinctMembershipPeople})`);
  console.log(`[eval:thresholds] distinct_equipment_names=${distinctEquipmentNames} (min=${minDistinctEquipmentNames})`);
  console.log(`[eval:thresholds] distinct_equipment_categories=${distinctEquipmentCategories} (min=${minDistinctEquipmentCategories})`);
  console.log(`[eval:thresholds] recordings_with_credit_evidence=${recordingsWithCreditEvidence} (min=${minRecordingsWithCreditEvidence})`);
  console.log(`[eval:thresholds] recordings_with_studio_evidence=${recordingsWithStudioEvidence} (min=${minRecordingsWithStudioEvidence})`);
  console.log(`[eval:thresholds] recordings_with_equipment_evidence=${recordingsWithEquipmentEvidence} (min=${minRecordingsWithEquipmentEvidence})`);
  console.log(`[eval:thresholds] system_playlists=${systemPlaylists} (min=${minSystemPlaylists}, max=${maxSystemPlaylists})`);
  console.log(`[eval:thresholds] membership_missing_canonical=${missingMembershipCanonical} (max=${maxMissingMembershipCanonical})`);
  console.log(`[eval:thresholds] membership_canonical_collisions=${membershipCanonicalCollisions} (max=${maxMembershipCanonicalCollisions})`);
  console.log(`[eval:thresholds] credit_canonical_collisions=${creditCanonicalCollisions} (max=${maxCreditCanonicalCollisions})`);
  console.log(`[eval:thresholds] studio_canonical_collisions=${studioCanonicalCollisions} (max=${maxStudioCanonicalCollisions})`);
  console.log(`[eval:thresholds] studio_alias_groups=${studioAliasGroups} (max=${maxStudioAliasGroups})`);
  console.log(`[eval:thresholds] reason_min_unique=${reasonQuality.minUnique} (min=${minReasonMinUnique})`);
  console.log(`[eval:thresholds] reason_max_dup=${reasonQuality.maxDup} (max=${maxReasonMaxDup})`);

  if (failures.length > 0) {
    for (const failure of failures) {
      console.error(`[eval:thresholds] FAIL ${failure}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log('[eval:thresholds] PASS all thresholds satisfied');
}

run();
