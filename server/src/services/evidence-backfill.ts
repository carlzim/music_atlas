import Database from 'better-sqlite3';
import {
  getPlaylistByCacheKey,
  getPlaylistByPrompt,
  savePlaylist,
} from './db.js';
import {
  fetchMusicBrainzCreditTracks,
  resolveMusicBrainzPerson,
} from './musicbrainz.js';
import { buildCreditCanonicalKey, canonicalizeDisplayName } from './normalize.js';

export const SUPPORTED_MUSICBRAINZ_CREDIT_ROLES = new Set(['producer', 'engineer', 'arranger']);

export interface MusicBrainzCreditBackfillParams {
  name: string;
  role: string;
  limit?: number;
}

export interface MusicBrainzCreditBackfillResult {
  name: string;
  role: string;
  mbid: string;
  mbCandidates: number;
  sourcePlaylistId: number;
  insertedRecordings: number;
  insertedEvidence: number;
  skippedExistingEvidence: number;
  skippedInvalid: number;
}

function normalizeRecordingToken(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function buildRecordingCanonicalKey(artist: string, title: string): string {
  return `${normalizeRecordingToken(artist)}::${normalizeRecordingToken(title)}`;
}

function normalizeLimit(limit: number | undefined): number {
  const parsed = Number(limit ?? 150);
  if (!Number.isFinite(parsed)) return 150;
  return Math.max(1, Math.min(1000, Math.floor(parsed)));
}

function ensureSourcePlaylistId(creditName: string, creditRole: string): number {
  const sourcePrompt = `[system] credit evidence backfill from musicbrainz :: ${creditRole} :: ${creditName}`;
  const existing = getPlaylistByPrompt(sourcePrompt) || getPlaylistByCacheKey(sourcePrompt);
  if (existing) return existing.id;

  const created = savePlaylist(
    sourcePrompt,
    `System MB credit seed (${creditRole})`,
    'Synthetic playlist row used as source for MusicBrainz credit evidence seeding.',
    '[]',
    JSON.stringify(['system_seed', 'credit', 'musicbrainz'])
  );

  return created.id;
}

export async function backfillCreditFromMusicBrainz(params: MusicBrainzCreditBackfillParams): Promise<MusicBrainzCreditBackfillResult> {
  const creditName = canonicalizeDisplayName(params.name || '');
  const creditRole = String(params.role || '').trim().toLowerCase();
  const limit = normalizeLimit(params.limit);

  if (!creditName) {
    throw new Error('name is required');
  }
  if (!creditRole || !SUPPORTED_MUSICBRAINZ_CREDIT_ROLES.has(creditRole)) {
    throw new Error(`role must be one of: ${Array.from(SUPPORTED_MUSICBRAINZ_CREDIT_ROLES).join(', ')}`);
  }

  const resolved = await resolveMusicBrainzPerson(creditName);
  if (!resolved) {
    throw new Error(`MusicBrainz person not found for name: ${creditName}`);
  }

  const mbTracks = await fetchMusicBrainzCreditTracks(resolved.id, creditRole, limit);
  const sourcePlaylistId = ensureSourcePlaylistId(creditName, creditRole);
  const db = new Database('playlists.db');

  const selectRecordingByCanonical = db.prepare(`
    SELECT id
    FROM recordings
    WHERE canonical_key = ?
    LIMIT 1
  `);
  const insertRecording = db.prepare(`
    INSERT INTO recordings (artist, title, canonical_key)
    VALUES (?, ?, ?)
  `);

  const creditExists = db.prepare(`
    SELECT 1
    FROM recording_credit_evidence
    WHERE recording_id = ?
      AND source_playlist_id = ?
      AND COALESCE(credit_name_canonical, lower(trim(credit_name))) = ?
      AND lower(trim(credit_role)) = ?
    LIMIT 1
  `);

  const insertCredit = db.prepare(`
    INSERT INTO recording_credit_evidence (recording_id, credit_name, credit_name_canonical, credit_role, source_playlist_id)
    VALUES (?, ?, ?, ?, ?)
  `);

  const creditCanonical = buildCreditCanonicalKey(creditName);
  if (!creditCanonical) {
    throw new Error(`Could not canonicalize credit name: ${creditName}`);
  }

  let insertedRecordings = 0;
  let insertedEvidence = 0;
  let skippedExistingEvidence = 0;
  let skippedInvalid = 0;

  for (const track of mbTracks) {
    const artist = canonicalizeDisplayName(track.artist);
    const title = track.title.trim();
    if (!artist || !title) {
      skippedInvalid += 1;
      continue;
    }

    const recordingCanonical = buildRecordingCanonicalKey(artist, title);
    if (!recordingCanonical) {
      skippedInvalid += 1;
      continue;
    }

    let recordingRow = selectRecordingByCanonical.get(recordingCanonical) as { id: number } | undefined;
    if (!recordingRow) {
      const insertResult = insertRecording.run(artist, title, recordingCanonical);
      recordingRow = { id: insertResult.lastInsertRowid as number };
      insertedRecordings += 1;
    }

    const exists = creditExists.get(recordingRow.id, sourcePlaylistId, creditCanonical, creditRole) as { 1: number } | undefined;
    if (exists) {
      skippedExistingEvidence += 1;
      continue;
    }

    insertCredit.run(recordingRow.id, creditName, creditCanonical, creditRole, sourcePlaylistId);
    insertedEvidence += 1;
  }

  return {
    name: creditName,
    role: creditRole,
    mbid: resolved.id,
    mbCandidates: mbTracks.length,
    sourcePlaylistId,
    insertedRecordings,
    insertedEvidence,
    skippedExistingEvidence,
    skippedInvalid,
  };
}
