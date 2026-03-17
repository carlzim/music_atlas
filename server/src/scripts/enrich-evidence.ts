import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import {
  buildArtistCanonicalKey,
  buildCreditCanonicalKey,
  buildPersonCanonicalKey,
  buildStudioCanonicalKey,
} from '../services/normalize.js';
import { runMembershipBackfill } from './backfill-membership.js';
import { runCreditBackfill } from './backfill-credit.js';

interface IdNameRow {
  id: number;
  name: string | null;
}

interface IdMembershipRow {
  id: number;
  band_name: string | null;
  person_name: string | null;
}

interface EnrichReport {
  generatedAt: string;
  studioCanonicalUpdated: number;
  creditCanonicalUpdated: number;
  membershipCanonicalUpdated: number;
  dedupedStudioRows: number;
  dedupedCreditRows: number;
  dedupedMembershipRows: number;
  evidenceCounts: {
    playlists: number;
    systemPlaylists: number;
    userPlaylists: number;
    recordings: number;
    recordingEquipmentEvidence: number;
    recordingStudioEvidence: number;
    recordingCreditEvidence: number;
    artistMembershipEvidence: number;
  };
}

function buildMarkdownSummary(report: EnrichReport): string {
  return [
    '# Enrich Evidence Report',
    '',
    `Generated at: ${report.generatedAt}`,
    '',
    '## Canonical Backfill Updates',
    '',
    `- studio_canonical_updated: ${report.studioCanonicalUpdated}`,
    `- credit_canonical_updated: ${report.creditCanonicalUpdated}`,
    `- membership_canonical_updated: ${report.membershipCanonicalUpdated}`,
    '',
    '## Deduped Rows',
    '',
    `- deduped_studio_rows: ${report.dedupedStudioRows}`,
    `- deduped_credit_rows: ${report.dedupedCreditRows}`,
    `- deduped_membership_rows: ${report.dedupedMembershipRows}`,
    '',
    '## Evidence Counts Snapshot',
    '',
    `- playlists: ${report.evidenceCounts.playlists}`,
    `- system_playlists: ${report.evidenceCounts.systemPlaylists}`,
    `- user_playlists: ${report.evidenceCounts.userPlaylists}`,
    `- recordings: ${report.evidenceCounts.recordings}`,
    `- recording_equipment_evidence: ${report.evidenceCounts.recordingEquipmentEvidence}`,
    `- recording_studio_evidence: ${report.evidenceCounts.recordingStudioEvidence}`,
    `- recording_credit_evidence: ${report.evidenceCounts.recordingCreditEvidence}`,
    `- artist_membership_evidence: ${report.evidenceCounts.artistMembershipEvidence}`,
    '',
  ].join('\n');
}

function readTableCount(db: Database.Database, table: string): number {
  const row = db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as { count: number | null };
  return typeof row?.count === 'number' ? row.count : 0;
}

function readSystemPlaylistCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM playlists
    WHERE prompt LIKE '[system] %'
  `).get() as { count: number | null };
  return typeof row?.count === 'number' ? row.count : 0;
}

function readUserPlaylistCount(db: Database.Database): number {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM playlists
    WHERE prompt NOT LIKE '[system] %'
  `).get() as { count: number | null };
  return typeof row?.count === 'number' ? row.count : 0;
}

function hasColumn(db: Database.Database, table: string, column: string): boolean {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string | null }>;
  return rows.some((row) => row.name === column);
}

function ensureSchema(db: Database.Database): void {
  if (!hasColumn(db, 'recording_studio_evidence', 'studio_name_canonical')) {
    db.exec('ALTER TABLE recording_studio_evidence ADD COLUMN studio_name_canonical TEXT');
  }
  if (!hasColumn(db, 'recording_credit_evidence', 'credit_name_canonical')) {
    db.exec('ALTER TABLE recording_credit_evidence ADD COLUMN credit_name_canonical TEXT');
  }
  if (!hasColumn(db, 'artist_membership_evidence', 'band_name_canonical')) {
    db.exec('ALTER TABLE artist_membership_evidence ADD COLUMN band_name_canonical TEXT');
  }
  if (!hasColumn(db, 'artist_membership_evidence', 'person_name_canonical')) {
    db.exec('ALTER TABLE artist_membership_evidence ADD COLUMN person_name_canonical TEXT');
  }
}

function backfillStudioCanonical(db: Database.Database): number {
  const rows = db.prepare(`
    SELECT id, studio_name AS name
    FROM recording_studio_evidence
    WHERE studio_name IS NOT NULL
  `).all() as IdNameRow[];

  const update = db.prepare('UPDATE recording_studio_evidence SET studio_name_canonical = ? WHERE id = ?');
  let updated = 0;
  for (const row of rows) {
    const canonical = buildStudioCanonicalKey(row.name || '');
    if (!canonical) continue;
    update.run(canonical, row.id);
    updated += 1;
  }
  return updated;
}

function backfillCreditCanonical(db: Database.Database): number {
  const rows = db.prepare(`
    SELECT id, credit_name AS name
    FROM recording_credit_evidence
    WHERE credit_name IS NOT NULL
  `).all() as IdNameRow[];

  const update = db.prepare('UPDATE recording_credit_evidence SET credit_name_canonical = ? WHERE id = ?');
  let updated = 0;
  for (const row of rows) {
    const canonical = buildCreditCanonicalKey(row.name || '');
    if (!canonical) continue;
    update.run(canonical, row.id);
    updated += 1;
  }
  return updated;
}

function backfillMembershipCanonical(db: Database.Database): number {
  const rows = db.prepare(`
    SELECT id, band_name, person_name
    FROM artist_membership_evidence
    WHERE band_name IS NOT NULL
      AND person_name IS NOT NULL
  `).all() as IdMembershipRow[];

  const update = db.prepare('UPDATE artist_membership_evidence SET band_name_canonical = ?, person_name_canonical = ? WHERE id = ?');
  let updated = 0;
  for (const row of rows) {
    const bandCanonical = buildArtistCanonicalKey(row.band_name || '');
    const personCanonical = buildPersonCanonicalKey(row.person_name || '');
    if (!bandCanonical || !personCanonical) continue;
    update.run(bandCanonical, personCanonical, row.id);
    updated += 1;
  }
  return updated;
}

function dedupeEvidenceRows(db: Database.Database): { studios: number; credits: number; memberships: number } {
  const deleteDuplicateStudios = db.prepare(`
    DELETE FROM recording_studio_evidence
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM recording_studio_evidence
      GROUP BY recording_id, COALESCE(studio_name_canonical, lower(trim(studio_name))), source_playlist_id
    )
  `);

  const deleteDuplicateCredits = db.prepare(`
    DELETE FROM recording_credit_evidence
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM recording_credit_evidence
      GROUP BY recording_id, COALESCE(credit_name_canonical, lower(trim(credit_name))), lower(trim(credit_role)), source_playlist_id
    )
  `);

  const deleteDuplicateMemberships = db.prepare(`
    DELETE FROM artist_membership_evidence
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM artist_membership_evidence
      GROUP BY COALESCE(band_name_canonical, lower(trim(band_name))), COALESCE(person_name_canonical, lower(trim(person_name))), lower(trim(COALESCE(member_role, ''))), source_playlist_id
    )
  `);

  const studios = deleteDuplicateStudios.run().changes;
  const credits = deleteDuplicateCredits.run().changes;
  const memberships = deleteDuplicateMemberships.run().changes;
  return { studios, credits, memberships };
}

function run(): void {
  runMembershipBackfill();
  runCreditBackfill();

  const db = new Database('playlists.db');

  const result = db.transaction(() => {
    ensureSchema(db);
    const studioUpdated = backfillStudioCanonical(db);
    const creditUpdated = backfillCreditCanonical(db);
    const membershipUpdated = backfillMembershipCanonical(db);
    const deduped = dedupeEvidenceRows(db);
    return { studioUpdated, creditUpdated, membershipUpdated, deduped };
  })();

  const report: EnrichReport = {
    generatedAt: new Date().toISOString(),
    studioCanonicalUpdated: result.studioUpdated,
    creditCanonicalUpdated: result.creditUpdated,
    membershipCanonicalUpdated: result.membershipUpdated,
    dedupedStudioRows: result.deduped.studios,
    dedupedCreditRows: result.deduped.credits,
    dedupedMembershipRows: result.deduped.memberships,
    evidenceCounts: {
      playlists: readTableCount(db, 'playlists'),
      systemPlaylists: readSystemPlaylistCount(db),
      userPlaylists: readUserPlaylistCount(db),
      recordings: readTableCount(db, 'recordings'),
      recordingEquipmentEvidence: readTableCount(db, 'recording_equipment_evidence'),
      recordingStudioEvidence: readTableCount(db, 'recording_studio_evidence'),
      recordingCreditEvidence: readTableCount(db, 'recording_credit_evidence'),
      artistMembershipEvidence: readTableCount(db, 'artist_membership_evidence'),
    },
  };

  const artifactsDir = path.resolve(process.cwd(), 'eval-artifacts');
  fs.mkdirSync(artifactsDir, { recursive: true });
  const reportPath = path.join(artifactsDir, 'enrich-evidence.json');
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  const markdownPath = path.join(artifactsDir, 'enrich-evidence.md');
  fs.writeFileSync(markdownPath, buildMarkdownSummary(report));

  console.log('[enrich:evidence] done');
  console.log(`[enrich:evidence] studio_canonical_updated=${report.studioCanonicalUpdated}`);
  console.log(`[enrich:evidence] credit_canonical_updated=${report.creditCanonicalUpdated}`);
  console.log(`[enrich:evidence] membership_canonical_updated=${report.membershipCanonicalUpdated}`);
  console.log(`[enrich:evidence] deduped_studio_rows=${report.dedupedStudioRows}`);
  console.log(`[enrich:evidence] deduped_credit_rows=${report.dedupedCreditRows}`);
  console.log(`[enrich:evidence] deduped_membership_rows=${report.dedupedMembershipRows}`);
  console.log(`[enrich:evidence] evidence_artist_membership=${report.evidenceCounts.artistMembershipEvidence}`);
  console.log(`[enrich:evidence] evidence_recording_credit=${report.evidenceCounts.recordingCreditEvidence}`);
  console.log(`[enrich:evidence] evidence_recording_studio=${report.evidenceCounts.recordingStudioEvidence}`);
  console.log(`[enrich:evidence] evidence_recording_equipment=${report.evidenceCounts.recordingEquipmentEvidence}`);
  console.log(`[enrich:evidence] evidence_system_playlists=${report.evidenceCounts.systemPlaylists}`);
  console.log(`[enrich:evidence] evidence_user_playlists=${report.evidenceCounts.userPlaylists}`);
  console.log(`[enrich:evidence] report_path=${reportPath}`);
  console.log(`[enrich:evidence] markdown_path=${markdownPath}`);
}

run();
