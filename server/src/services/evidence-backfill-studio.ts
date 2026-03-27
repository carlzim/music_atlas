import Database from 'better-sqlite3';
import {
  getPlaylistByCacheKey,
  getPlaylistByPrompt,
  savePlaylist,
} from './db.js';
import { fetchDiscogsStudioTracksByArtistCatalog, fetchDiscogsStudioTracksByArtistHints, fetchDiscogsStudioTracksByLabel, isDiscogsConfigured, searchDiscogsStudioLabelId } from './discogs.js';
import { fetchMusicBrainzStudioTracksByPlace, resolveMusicBrainzStudioPlace } from './musicbrainz.js';
import { buildStudioCanonicalKey, canonicalizeDisplayName } from './normalize.js';
import { resolveStudioIdentity, resolveStudioIdentityFromPrompt } from './studio-identity.js';

export interface StudioEvidenceBackfillParams {
  studioName: string;
  prompt?: string;
  artistHints?: string[];
  activeStartYear?: number;
  activeEndYear?: number;
  curatedRecordedTracks?: Array<{ artist: string; title: string }>;
  limit?: number;
}

export interface StudioEvidenceBackfillResult {
  attempted: boolean;
  source: 'discogs' | 'musicbrainz' | 'hybrid';
  studioName: string;
  studioIdentityKey?: string;
  musicBrainzPlaceId?: string;
  musicBrainzPlaceName?: string;
  discogsLabelId?: number;
  discogsLabelSource?: 'identity' | 'search';
  imported: number;
  insertedRecordings: number;
  insertedEvidence: number;
  skippedExistingEvidence: number;
  skippedInvalid: number;
  skippedReason?: string;
}

type StudioSeedTrack = {
  artist: string;
  title: string;
  studioName: string;
  releaseId: number;
  releaseTitle: string;
  sourceRef: string;
  source: 'discogs' | 'musicbrainz';
  recordedYear?: number;
};

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

function normalizeTrackKey(artist: string, title: string): string {
  return `${artist.trim().toLowerCase()}::${title.trim().toLowerCase()}`;
}

function normalizeArtistKey(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildPreferredArtistKeys(values: string[]): string[] {
  return Array.from(
    new Set(
      values
        .map((value) => normalizeArtistKey(value || ''))
        .filter((value) => value.length >= 3)
    )
  );
}

function artistMatchesPreferred(artist: string, preferredArtistKeys: string[]): boolean {
  if (preferredArtistKeys.length === 0) return false;
  const normalizedArtist = normalizeArtistKey(artist);
  if (!normalizedArtist) return false;
  return preferredArtistKeys.some((preferred) => normalizedArtist === preferred || normalizedArtist.includes(preferred));
}

function extractYear(value: string | null | undefined): number | null {
  if (!value) return null;
  const match = value.match(/\b(19\d{2}|20\d{2})\b/);
  if (!match) return null;
  const year = Number(match[1]);
  if (!Number.isFinite(year) || year < 1900 || year > 2099) return null;
  return Math.floor(year);
}

function isVariantStudioTitle(title: string): boolean {
  const value = title.toLowerCase();
  return /\b(remix|mix|demo|instrumental|karaoke|take\s*\d+|take\b|alternate|alt\.|version|radio edit|extended|acoustic|live|rehearsal|dub|isolated|half[\s-]?mixed)\b/.test(value);
}

function isLikelySongRecording(artist: string, title: string): boolean {
  if (!artist || !title) return false;
  if (title.length > 90) return false;

  const artistCommaCount = (artist.match(/,/g) || []).length;
  if (artistCommaCount >= 2) return false;

  const artistLower = artist.toLowerCase();
  const titleLower = title.toLowerCase();
  if (/\b(orchestra|symphony|philharmonic|choir|ensemble|quartet|quintet|conductor)\b/.test(artistLower)) return false;
  if (/\b(op\.|opus|concerto|sonata|suite|movement|act\s+\d+|scene\s+\d+|recitativo|aria|overture)\b/.test(titleLower)) return false;
  if (title.includes(':')) return false;

  return true;
}

function countUniqueArtists(rows: Array<{ artist: string }>): number {
  return new Set(rows.map((row) => row.artist.trim().toLowerCase()).filter(Boolean)).size;
}

function countPreferredArtistMatches(
  rows: Array<{ artist: string }>,
  preferredArtistKeys: string[]
): number {
  if (preferredArtistKeys.length === 0) return 0;
  return rows.reduce((count, row) => count + (artistMatchesPreferred(row.artist, preferredArtistKeys) ? 1 : 0), 0);
}

function countUniquePreferredArtists(
  rows: Array<{ artist: string }>,
  preferredArtistKeys: string[]
): number {
  if (preferredArtistKeys.length === 0) return 0;
  const matched = new Set<string>();
  for (const row of rows) {
    const normalizedArtist = normalizeArtistKey(row.artist);
    if (!normalizedArtist) continue;
    for (const preferred of preferredArtistKeys) {
      if (normalizedArtist === preferred || normalizedArtist.includes(preferred)) {
        matched.add(preferred);
      }
    }
  }
  return matched.size;
}

function isMainstreamStudioPrompt(prompt: string | undefined): boolean {
  const value = (prompt || '').toLowerCase();
  if (!value) return false;
  return /\bbest known\b|\bwell known\b|\biconic\b|\bclassic\b|\bessential\b|\bhits?\b|\bpopular\b|\bfamous\b|\bmegaklassiker\b/.test(value);
}

function getStudioSeedQualityScore(
  track: StudioSeedTrack,
  preferredArtistKeys: string[],
  mainstreamPrompt: boolean
): number {
  const artist = track.artist;
  const title = track.title;
  let score = 0;

  if (artistMatchesPreferred(artist, preferredArtistKeys)) score += 12;
  if (track.source === 'discogs') score += mainstreamPrompt ? 5 : 2;
  if (isLikelySongRecording(artist, title)) score += 3;
  if (isVariantStudioTitle(title)) score -= 10;

  const artistCommaCount = (artist.match(/,/g) || []).length;
  if (artistCommaCount >= 2) score -= 8;
  if (title.includes(':')) score -= 6;
  if (artist.length >= 60) score -= 4;
  if (title.length >= 70) score -= 4;

  const artistLower = artist.toLowerCase();
  const titleLower = title.toLowerCase();
  if (/\b(orchestra|symphony|philharmonic|choir|ensemble|quartet|quintet|conductor)\b/.test(artistLower)) score -= 10;
  if (/\b(op\.|opus|concerto|sonata|suite|movement|act\s+\d+|scene\s+\d+|recitativo|aria|overture)\b/.test(titleLower)) score -= 10;

  return score;
}

function prioritizeStudioSeedTracks(
  tracks: StudioSeedTrack[],
  preferredArtistKeys: string[],
  mainstreamPrompt: boolean
): StudioSeedTrack[] {
  const scored = tracks.map((track, index) => ({
    track,
    index,
    score: getStudioSeedQualityScore(track, preferredArtistKeys, mainstreamPrompt),
  }));

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.index - b.index;
  });

  return scored.map((row) => row.track);
}

function diversifyStudioSeedTracks<T extends { artist: string; title: string }>(tracks: T[], desired: number): T[] {
  if (tracks.length <= 1 || desired <= 0) return tracks.slice(0, Math.max(0, desired));

  const target = Math.min(desired, tracks.length);
  const selected: T[] = [];
  const selectedKeys = new Set<string>();
  const artistCounts = new Map<string, number>();

  const addPass = (artistCap: number): void => {
    for (const track of tracks) {
      if (selected.length >= target) return;
      const key = normalizeTrackKey(track.artist, track.title);
      if (!key || selectedKeys.has(key)) continue;
      const artistKey = track.artist.trim().toLowerCase();
      if (!artistKey) continue;
      const count = artistCounts.get(artistKey) || 0;
      if (count >= artistCap) continue;
      selected.push(track);
      selectedKeys.add(key);
      artistCounts.set(artistKey, count + 1);
    }
  };

  addPass(1);
  if (selected.length < target) addPass(2);
  if (selected.length < target) addPass(3);
  if (selected.length < target) addPass(5);

  if (selected.length < target) {
    for (const track of tracks) {
      if (selected.length >= target) break;
      const key = normalizeTrackKey(track.artist, track.title);
      if (!key || selectedKeys.has(key)) continue;
      selected.push(track);
      selectedKeys.add(key);
    }
  }

  return selected;
}

function ensureSourcePlaylistId(studioName: string, source: 'discogs' | 'musicbrainz'): number {
  const sourcePrompt = `[system] studio evidence backfill from ${source} :: ${studioName}`;
  const existing = getPlaylistByPrompt(sourcePrompt) || getPlaylistByCacheKey(sourcePrompt);
  if (existing) return existing.id;

  const created = savePlaylist(
    sourcePrompt,
    `System studio seed (${studioName})`,
    `Synthetic playlist row used as source for ${source === 'musicbrainz' ? 'MusicBrainz' : 'Discogs'} studio evidence seeding.`,
    '[]',
    JSON.stringify(['system_seed', 'studio', source])
  );

  return created.id;
}

export async function backfillStudioFromDiscogs(params: StudioEvidenceBackfillParams): Promise<StudioEvidenceBackfillResult> {
  const inputStudio = canonicalizeDisplayName(params.studioName || '');
  const resolvedFromPrompt = params.prompt ? resolveStudioIdentityFromPrompt(params.prompt) : null;
  const resolved = resolveStudioIdentity(inputStudio) || resolvedFromPrompt;
  const studioName = resolved?.primaryName || inputStudio;
  const studioIdentityKey = resolved?.key;
  const acceptedStudioNames = Array.isArray(resolved?.acceptedStudioNames)
    ? resolved.acceptedStudioNames
    : [];
  let discogsLabelId = resolved?.discogsLabelId;
  let discogsLabelSource: 'identity' | 'search' | undefined = discogsLabelId ? 'identity' : undefined;
  let musicBrainzPlaceId: string | undefined;
  let musicBrainzPlaceName: string | undefined;
  const limit = normalizeLimit(params.limit);
  const activeStartYear = typeof params.activeStartYear === 'number' ? params.activeStartYear : undefined;
  const activeEndYear = typeof params.activeEndYear === 'number' ? params.activeEndYear : undefined;
  const musicBrainzEnabled = process.env.ENABLE_STUDIO_MUSICBRAINZ_BACKFILL !== 'false';
  const discogsEnabled = process.env.ENABLE_STUDIO_DISCOGS_BACKFILL !== 'false' && isDiscogsConfigured();
  const artistHints = Array.isArray(params.artistHints)
    ? Array.from(new Set(params.artistHints.map((value) => canonicalizeDisplayName(value || '')).filter((value) => value.length > 0))).slice(0, 12)
    : [];
  const identityPreferredArtists = Array.isArray(resolved?.preferredArtists)
    ? resolved.preferredArtists.map((value) => canonicalizeDisplayName(value || '')).filter((value) => value.length > 0)
    : [];
  const mainstreamPrompt = isMainstreamStudioPrompt(params.prompt);
  const effectiveArtistHints = Array.from(
    new Set([
      ...artistHints,
      ...(mainstreamPrompt ? identityPreferredArtists : identityPreferredArtists.slice(0, 5)),
    ])
  ).slice(0, 16);
  const preferredArtistKeys = buildPreferredArtistKeys(Array.isArray(resolved?.preferredArtists) ? resolved.preferredArtists : []);

  let attemptedMusicBrainz = false;
  let attemptedDiscogs = false;
  let studioTracks: StudioSeedTrack[] = [];

  if (!studioName) {
    return {
      attempted: false,
      source: 'hybrid',
      studioName: '',
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason: 'missing_studio_name',
    };
  }

  if (musicBrainzEnabled) {
    attemptedMusicBrainz = true;
    try {
      const preferredPlaceId = typeof resolved?.musicBrainzPlaceId === 'string' ? resolved.musicBrainzPlaceId.trim() : '';
      const resolvedPlace = preferredPlaceId
        ? { id: preferredPlaceId, name: studioName, score: 100, type: 'Studio', disambiguation: '' }
        : await resolveMusicBrainzStudioPlace(studioName, acceptedStudioNames);
      if (resolvedPlace) {
        musicBrainzPlaceId = resolvedPlace.id;
        musicBrainzPlaceName = resolvedPlace.name;
        const mbTracks = await fetchMusicBrainzStudioTracksByPlace(resolvedPlace.id, Math.max(limit * 12, 320));
        const mbSeed: StudioSeedTrack[] = [];
        for (let index = 0; index < mbTracks.length; index += 1) {
          const row = mbTracks[index];
          const artist = canonicalizeDisplayName(row.artist || '');
          const title = (row.title || '').trim();
          if (!artist || !title) continue;
          if (isVariantStudioTitle(title)) continue;
          if (!isLikelySongRecording(artist, title)) continue;

          const year = extractYear(row.begin) ?? extractYear(row.end);
          if (year !== null) {
            if (typeof activeStartYear === 'number' && year < activeStartYear) continue;
            if (typeof activeEndYear === 'number' && year > activeEndYear) continue;
          }

          mbSeed.push({
            artist,
            title,
            studioName,
            releaseId: 10_000_000 + index,
            releaseTitle: 'MusicBrainz recorded-at relation',
            sourceRef: `musicbrainz:place:${resolvedPlace.id}:recording:${row.recordingMbid}`,
            source: 'musicbrainz',
            recordedYear: year ?? undefined,
          });
        }

        const dedupe = new Set<string>();
        const uniqueMbSeed: StudioSeedTrack[] = [];
        for (const row of mbSeed) {
          const key = normalizeTrackKey(row.artist, row.title);
          if (!key || dedupe.has(key)) continue;
          dedupe.add(key);
          uniqueMbSeed.push(row);
        }

        studioTracks = diversifyStudioSeedTracks(uniqueMbSeed, Math.max(limit * 3, 120));
      }
    } catch {
      // MusicBrainz studio backfill is best-effort.
    }
  }

  const minimumBreadthTarget = Math.min(8, Math.max(4, Math.floor(limit / 3)));
  const preferredMatchTarget = Math.min(8, Math.max(3, Math.floor(limit / 2)));
  const preferredMatchCount = countPreferredArtistMatches(studioTracks, preferredArtistKeys);
  const uniquePreferredArtists = countUniquePreferredArtists(studioTracks, preferredArtistKeys);
  const preferredArtistVarietyTarget = Math.min(4, preferredArtistKeys.length);
  const shouldUseDiscogsFallback =
    studioTracks.length < Math.min(40, limit)
    || countUniqueArtists(studioTracks) < minimumBreadthTarget
    || (mainstreamPrompt && preferredArtistKeys.length > 0 && (
      preferredMatchCount < preferredMatchTarget
      || uniquePreferredArtists < preferredArtistVarietyTarget
    ));

  if (discogsEnabled && shouldUseDiscogsFallback) {
    attemptedDiscogs = true;

    if (!discogsLabelId || !Number.isFinite(discogsLabelId) || discogsLabelId <= 0) {
      const searchCandidates = Array.from(
        new Set(
          [
            studioName,
            ...acceptedStudioNames,
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

    if (discogsLabelId && Number.isFinite(discogsLabelId) && discogsLabelId > 0) {
      let discogsTracks = await fetchDiscogsStudioTracksByLabel(discogsLabelId, studioName, Math.max(limit, 120), activeStartYear, activeEndYear);
      if (effectiveArtistHints.length > 0) {
        const fallbackLimit = Math.max(50, Math.min(limit, 120));
        const fallbackTracks = await fetchDiscogsStudioTracksByArtistHints(studioName, effectiveArtistHints, fallbackLimit, activeStartYear, activeEndYear);
        const dedupeKeys = new Set<string>();
        const merged: typeof discogsTracks = [];
        for (const row of [...fallbackTracks, ...discogsTracks]) {
          const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
          if (dedupeKeys.has(key)) continue;
          dedupeKeys.add(key);
          merged.push(row);
          if (merged.length >= Math.max(limit, 120)) break;
        }
        discogsTracks = merged;
      }
      if ((discogsTracks.length < Math.min(30, limit) || countUniqueArtists(discogsTracks) < minimumBreadthTarget) && effectiveArtistHints.length > 0) {
        const catalogTracks = await fetchDiscogsStudioTracksByArtistCatalog(studioName, effectiveArtistHints, Math.max(50, Math.min(limit, 120)), activeStartYear, activeEndYear);
        const dedupeKeys = new Set<string>();
        const merged: typeof discogsTracks = [];
        for (const row of [...catalogTracks, ...discogsTracks]) {
          const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
          if (dedupeKeys.has(key)) continue;
          dedupeKeys.add(key);
          merged.push(row);
          if (merged.length >= Math.max(limit, 120)) break;
        }
        discogsTracks = merged;
      }

      const discogsSeed: StudioSeedTrack[] = discogsTracks.map((row) => ({
        artist: canonicalizeDisplayName(row.artist),
        title: row.title.trim(),
        studioName,
        releaseId: row.releaseId,
        releaseTitle: row.releaseTitle,
        sourceRef: row.sourceRef,
        source: 'discogs',
      }));

      const mergedByKey = new Map<string, StudioSeedTrack>();
      const sourcePriority = mainstreamPrompt ? [...discogsSeed, ...studioTracks] : [...studioTracks, ...discogsSeed];
      for (const row of sourcePriority) {
        const key = normalizeTrackKey(row.artist, row.title);
        if (!key || mergedByKey.has(key)) continue;
        mergedByKey.set(key, row);
      }
      studioTracks = Array.from(mergedByKey.values());
    }
  }

  studioTracks = prioritizeStudioSeedTracks(studioTracks, preferredArtistKeys, mainstreamPrompt);

  const curatedRecordedTracks = Array.isArray(params.curatedRecordedTracks)
    ? params.curatedRecordedTracks
    : Array.isArray(resolved?.curatedRecordedTracks)
      ? resolved.curatedRecordedTracks
      : [];
  if (curatedRecordedTracks.length > 0) {
    const dedupeKeys = new Set<string>();
    const merged: StudioSeedTrack[] = [];
    let syntheticReleaseId = 1;
    for (const curated of curatedRecordedTracks) {
      const artist = canonicalizeDisplayName(curated.artist || '');
      const title = (curated.title || '').trim();
      if (!artist || !title) continue;
      const key = `${artist.toLowerCase()}::${title.toLowerCase()}`;
      if (dedupeKeys.has(key)) continue;
      dedupeKeys.add(key);
      merged.unshift({
        artist,
        title,
        studioName,
        releaseId: syntheticReleaseId,
        releaseTitle: 'Curated studio recording seed',
        sourceRef: `curated:studio:${studioIdentityKey || buildStudioCanonicalKey(studioName)}:${syntheticReleaseId}`,
        source: 'discogs',
      });
      syntheticReleaseId += 1;
    }
    for (const row of studioTracks) {
      const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}`;
      if (dedupeKeys.has(key)) continue;
      dedupeKeys.add(key);
      merged.push(row);
      if (merged.length >= Math.max(limit, 120)) break;
    }
    studioTracks = merged;
  }

  studioTracks = diversifyStudioSeedTracks(studioTracks, Math.max(limit, 120));

  if (studioTracks.length === 0) {
    const skippedReason = !attemptedMusicBrainz && !attemptedDiscogs
      ? (!musicBrainzEnabled && !discogsEnabled ? 'all_sources_disabled' : !discogsEnabled ? 'missing_discogs_token' : 'musicbrainz_not_attempted')
      : 'no_rows';
    return {
      attempted: attemptedMusicBrainz || attemptedDiscogs,
      source: attemptedMusicBrainz && attemptedDiscogs ? 'hybrid' : attemptedMusicBrainz ? 'musicbrainz' : 'discogs',
      studioName,
      studioIdentityKey,
      musicBrainzPlaceId,
      musicBrainzPlaceName,
      discogsLabelId,
      discogsLabelSource,
      imported: 0,
      insertedRecordings: 0,
      insertedEvidence: 0,
      skippedExistingEvidence: 0,
      skippedInvalid: 0,
      skippedReason,
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
  const sourcesUsed = new Set<'discogs' | 'musicbrainz'>(studioTracks.map((row) => row.source));
  const sourcePlaylistBySource = new Map<'discogs' | 'musicbrainz', number>();
  for (const source of sourcesUsed) {
    const sourcePlaylistId = ensureSourcePlaylistId(studioName, source);
    sourcePlaylistBySource.set(source, sourcePlaylistId);
    deletePriorStudioEvidenceForSource.run(sourcePlaylistId, studioCanonical);
  }

  let insertedRecordings = 0;
  let insertedEvidence = 0;
  let skippedExistingEvidence = 0;
  let skippedInvalid = 0;

  for (const track of studioTracks) {
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

    const sourcePlaylistId = sourcePlaylistBySource.get(track.source);
    if (typeof sourcePlaylistId !== 'number' || !Number.isFinite(sourcePlaylistId)) {
      skippedInvalid += 1;
      continue;
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
    attempted: attemptedMusicBrainz || attemptedDiscogs,
    source: attemptedMusicBrainz && attemptedDiscogs ? 'hybrid' : attemptedMusicBrainz ? 'musicbrainz' : 'discogs',
    studioName,
    studioIdentityKey,
    musicBrainzPlaceId,
    musicBrainzPlaceName,
    discogsLabelId,
    discogsLabelSource,
    imported: studioTracks.length,
    insertedRecordings,
    insertedEvidence,
    skippedExistingEvidence,
    skippedInvalid,
    skippedReason: studioTracks.length > 0 ? undefined : 'no_rows',
  };
}
