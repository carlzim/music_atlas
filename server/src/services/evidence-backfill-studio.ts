import Database from 'better-sqlite3';
import {
  getPlaylistByCacheKey,
  getPlaylistByPrompt,
  savePlaylist,
} from './db.js';
import { fetchDiscogsStudioTracksByArtistHints, fetchDiscogsStudioTracksByLabel, isDiscogsConfigured, searchDiscogsStudioLabelId } from './discogs.js';
import { buildStudioCanonicalKey, canonicalizeDisplayName } from './normalize.js';
import { resolveStudioIdentity, resolveStudioIdentityFromPrompt } from './studio-identity.js';

export interface StudioEvidenceBackfillParams {
  studioName: string;
  prompt?: string;
  artistHints?: string[];
  limit?: number;
}

export interface StudioEvidenceBackfillResult {
  attempted: boolean;
  studioName: string;
  studioIdentityKey?: string;
  discogsLabelId?: number;
  discogsLabelSource?: 'identity' | 'search';
  imported: number;
  insertedRecordings: number;
  insertedEvidence: number;
  skippedExistingEvidence: number;
  skippedInvalid: number;
  skippedReason?: string;
}

function normalizeRecordingToken(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function buildRecordingCanonicalKey(artist: string, title: string): string {
  return `${normalizeRecordingToken(artist)}::${normalizeRecordingToken(title)}`;
}

function normalizeLimit(limit: number | undefined): number {
  const parsed = Number(limit ?? 200);
  if (!Number.isFinite(parsed)) return 200;
  return Math.max(1, Math.min(400, Math.floor(parsed)));
}

function ensureSourcePlaylistId(studioName: string): number {
  const sourcePrompt = `[system] studio evidence backfill from discogs :: ${studioName}`;
  const existing = getPlaylistByPrompt(sourcePrompt) || getPlaylistByCacheKey(sourcePrompt);
  if (existing) return existing.id;

  const created = savePlaylist(
    sourcePrompt,
    `System studio seed (${studioName})`,
    'Synthetic playlist row used as source for Discogs studio evidence seeding.',
    '[]',
    JSON.stringify(['system_seed', 'studio', 'discogs'])
  );

  return created.id;
}

export async function backfillStudioFromDiscogs(params: StudioEvidenceBackfillParams): Promise<StudioEvidenceBackfillResult> {
  const inputStudio = canonicalizeDisplayName(params.studioName || '');
  const resolvedFromPrompt = params.prompt ? resolveStudioIdentityFromPrompt(params.prompt) : null;
  const resolved = resolveStudioIdentity(inputStudio) || resolvedFromPrompt;
  const studioName = resolved?.primaryName || inputStudio;
  const studioIdentityKey = resolved?.key;
  let discogsLabelId = resolved?.discogsLabelId;
  let discogsLabelSource: 'identity' | 'search' | undefined = discogsLabelId ? 'identity' : undefined;
  const limit = normalizeLimit(params.limit);

  if (!studioName) {
    return {
      attempted: false,
      studioName: '',
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason: 'missing_studio_name',
    };
  }

  if (!isDiscogsConfigured()) {
    return {
      attempted: false,
      studioName,
      studioIdentityKey,
      discogsLabelId,
      discogsLabelSource,
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason: 'missing_discogs_token',
    };
  }

  if (!discogsLabelId || !Number.isFinite(discogsLabelId) || discogsLabelId <= 0) {
    const searchCandidates = Array.from(
      new Set(
        [
          studioName,
          ...(resolved?.acceptedStudioNames || []),
          studioName.replace(/,\s*stockholm$/i, ''),
          studioName.replace(/,\s*london$/i, ''),
          studioName.replace(/,\s*los angeles$/i, ''),
        ]
          .map((value) => canonicalizeDisplayName(value || ''))
          .filter((value) => value.length > 0)
      )
    );

    for (const searchCandidate of searchCandidates) {
      try {
        discogsLabelId = await searchDiscogsStudioLabelId(searchCandidate) || undefined;
      } catch {
        discogsLabelId = undefined;
      }
      if (discogsLabelId && Number.isFinite(discogsLabelId) && discogsLabelId > 0) {
        discogsLabelSource = 'search';
        break;
      }
    }
  }

  if (!discogsLabelId || !Number.isFinite(discogsLabelId) || discogsLabelId <= 0) {
    return {
      attempted: false,
      studioName,
      studioIdentityKey,
      discogsLabelId,
      discogsLabelSource,
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason: 'studio_discogs_label_missing',
    };
  }

  const sourcePlaylistId = ensureSourcePlaylistId(studioName);
  let discogsTracks = await fetchDiscogsStudioTracksByLabel(discogsLabelId, studioName, limit);
  const artistHints = Array.isArray(params.artistHints)
    ? Array.from(new Set(params.artistHints.map((value) => canonicalizeDisplayName(value || '')).filter((value) => value.length > 0))).slice(0, 12)
    : [];
  if (artistHints.length > 0) {
    const fallbackLimit = Math.max(30, Math.min(limit, 100));
    const fallbackTracks = await fetchDiscogsStudioTracksByArtistHints(studioName, artistHints, fallbackLimit);
    const dedupeKeys = new Set<string>();
    const merged: typeof discogsTracks = [];
    for (const row of [...fallbackTracks, ...discogsTracks]) {
      const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
      if (dedupeKeys.has(key)) continue;
      dedupeKeys.add(key);
      merged.push(row);
      if (merged.length >= limit) break;
    }
    discogsTracks = merged;
  }
  if (discogsTracks.length === 0) {
    return {
      attempted: true,
      studioName,
      studioIdentityKey,
      discogsLabelId,
      discogsLabelSource,
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason: 'no_rows',
    };
  }

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
  const studioEvidenceExists = db.prepare(`
    SELECT 1
    FROM recording_studio_evidence
    WHERE recording_id = ?
      AND source_playlist_id = ?
      AND COALESCE(studio_name_canonical, lower(trim(studio_name))) = ?
    LIMIT 1
  `);
  const insertStudioEvidence = db.prepare(`
    INSERT INTO recording_studio_evidence (recording_id, studio_name, studio_name_canonical, source_playlist_id)
    VALUES (?, ?, ?, ?)
  `);
  const deletePriorStudioEvidenceForSource = db.prepare(`
    DELETE FROM recording_studio_evidence
    WHERE source_playlist_id = ?
      AND COALESCE(studio_name_canonical, lower(trim(studio_name))) = ?
  `);

  const studioCanonical = buildStudioCanonicalKey(studioName);
  deletePriorStudioEvidenceForSource.run(sourcePlaylistId, studioCanonical);
  let insertedRecordings = 0;
  let insertedEvidence = 0;
  let skippedExistingEvidence = 0;
  let skippedInvalid = 0;

  for (const track of discogsTracks) {
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

    const exists = studioEvidenceExists.get(recordingRow.id, sourcePlaylistId, studioCanonical) as { 1: number } | undefined;
    if (exists) {
      skippedExistingEvidence += 1;
      continue;
    }

    insertStudioEvidence.run(recordingRow.id, studioName, studioCanonical, sourcePlaylistId);
    insertedEvidence += 1;
  }

  return {
    attempted: true,
    studioName,
    studioIdentityKey,
    discogsLabelId,
    discogsLabelSource,
    imported: discogsTracks.length,
    insertedRecordings,
    insertedEvidence,
    skippedExistingEvidence,
    skippedInvalid,
    skippedReason: discogsTracks.length > 0 ? undefined : 'no_rows',
  };
}
