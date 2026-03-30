import Database from 'better-sqlite3';
import {
  getPlaylistByCacheKey,
  getPlaylistByPrompt,
  savePlaylist,
} from './db.js';
import { fetchDiscogsStudioTracksByArtistCatalog, fetchDiscogsStudioTracksByArtistHints, fetchDiscogsStudioTracksByLabel, isDiscogsConfigured, searchDiscogsStudioLabelId } from './discogs.js';
import { fetchMusicBrainzStudioTracksByPlace, resolveMusicBrainzStudioPlaces } from './musicbrainz.js';
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
  mbArtistFrequency?: number;
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

function promptWantsClassicalMusic(prompt: string | undefined): boolean {
  const value = (prompt || '').toLowerCase();
  if (!value) return false;
  return /\bclassical\b|\borchestral\b|\bsymphon(?:y|ic)\b|\bconcerto\b|\bsonata\b|\bchamber\b|\bbaroque\b|\bopera\b|\bfilm score\b|\bsoundtrack\b/.test(value);
}

function isLikelyClassicalOrScoreRecording(artist: string, title: string): boolean {
  const artistLower = artist.toLowerCase();
  const titleLower = title.toLowerCase();

  if (/\b(orchestra|symphony|philharmonic|choir|ensemble|quartet|quintet|conductor|maestro)\b/.test(artistLower)) return true;
  if (/\b(op\.|opus|concerto|sonata|suite|movement|act\s+\d+|scene\s+\d+|recitativo|aria|overture|requiem|nocturne|etude|waltz|prelude|prélude|fugue|scherzo|mazurka|bourree|sarabande|gigue|largo|adagio|allegro|andante)\b/.test(titleLower)) return true;
  if (/\b(op\.?\s*\d+|no\.?\s*\d+|d\.?\s*\d+|k\.?\s*\d+|kv\.?\s*\d+|bwv\s*\d+)\b/.test(titleLower)) return true;
  if (/\b(der|die|das)\s+[a-zäöüß]+\b/.test(titleLower) && /\bd\.?\s*\d+\b/.test(titleLower)) return true;
  if (/\b(original motion picture soundtrack|motion picture|film score|soundtrack)\b/.test(titleLower)) return true;
  if (title.includes(':')) return true;
  if ((artist.split(',').length - 1) >= 1 && /\b(op\.|d\.|bwv|kv)\b/.test(titleLower)) return true;

  return false;
}

function shouldKeepStudioTrackByClassicalPolicy(
  artist: string,
  title: string,
  wantsClassicalMusic: boolean
): boolean {
  if (wantsClassicalMusic) return true;
  if (!isLikelyClassicalOrScoreRecording(artist, title)) return true;
  return false;
}

function countUniqueArtists(rows: Array<{ artist: string }>): number {
  return new Set(rows.map((row) => row.artist.trim().toLowerCase()).filter(Boolean)).size;
}

function isMainstreamStudioPrompt(prompt: string | undefined): boolean {
  const value = (prompt || '').toLowerCase();
  if (!value) return false;
  return /\bbest known\b|\bwell known\b|\biconic\b|\bclassic\b|\bessential\b|\bhits?\b|\bpopular\b|\bfamous\b|\bmegaklassiker\b/.test(value);
}

function getStudioSeedQualityScore(
  track: StudioSeedTrack,
  preferredArtistKeys: string[],
  mainstreamPrompt: boolean,
  wantsClassicalMusic: boolean
): number {
  const artist = track.artist;
  const title = track.title;
  let score = 0;

  if (artistMatchesPreferred(artist, preferredArtistKeys)) score += 2;
  if (track.source === 'discogs') score += mainstreamPrompt ? 5 : 2;
  if (track.source === 'musicbrainz' && typeof track.mbArtistFrequency === 'number' && Number.isFinite(track.mbArtistFrequency)) {
    score += Math.min(28, Math.sqrt(Math.max(1, track.mbArtistFrequency)) * 2.2);
    if (track.mbArtistFrequency <= 2) score -= 6;
  }
  if (isLikelySongRecording(artist, title)) score += 3;
  if (wantsClassicalMusic && isLikelyClassicalOrScoreRecording(artist, title)) score += 18;
  if (wantsClassicalMusic && !isLikelyClassicalOrScoreRecording(artist, title)) score -= 6;
  if (isVariantStudioTitle(title)) score -= 10;

  const artistCommaCount = (artist.match(/,/g) || []).length;
  if (artistCommaCount >= 2) score -= 8;
  if (/\(\d+\)/.test(artist)) score -= 6;
  if (/\bfeat\.?\b/i.test(artist)) score -= 4;
  if (title.includes(':') && !wantsClassicalMusic) score -= 6;
  if (artist.length >= 60) score -= 4;
  if (title.length >= 70) score -= 4;

  const artistLower = artist.toLowerCase();
  const titleLower = title.toLowerCase();
  if (!wantsClassicalMusic && /\b(orchestra|symphony|philharmonic|choir|ensemble|quartet|quintet|conductor)\b/.test(artistLower)) score -= 10;
  if (!wantsClassicalMusic && /\b(op\.|opus|concerto|sonata|suite|movement|act\s+\d+|scene\s+\d+|recitativo|aria|overture)\b/.test(titleLower)) score -= 10;

  return score;
}

function prioritizeStudioSeedTracks(
  tracks: StudioSeedTrack[],
  preferredArtistKeys: string[],
  mainstreamPrompt: boolean,
  wantsClassicalMusic: boolean
): StudioSeedTrack[] {
  const scored = tracks.map((track, index) => ({
    track,
    index,
    score: getStudioSeedQualityScore(track, preferredArtistKeys, mainstreamPrompt, wantsClassicalMusic),
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

function limitStudioSeedTracksByArtist<T extends { artist: string; title: string }>(
  tracks: T[],
  desired: number,
  perArtistCap = 4
): T[] {
  if (tracks.length <= 1 || desired <= 0) return tracks.slice(0, Math.max(0, desired));
  const target = Math.min(desired, tracks.length);
  const selected: T[] = [];
  const selectedKeys = new Set<string>();
  const artistCounts = new Map<string, number>();

  for (const track of tracks) {
    if (selected.length >= target) break;
    const key = normalizeTrackKey(track.artist, track.title);
    if (!key || selectedKeys.has(key)) continue;
    const artistKey = normalizeArtistKey(track.artist);
    if (!artistKey) continue;
    const count = artistCounts.get(artistKey) || 0;
    if (count >= perArtistCap) continue;
    selected.push(track);
    selectedKeys.add(key);
    artistCounts.set(artistKey, count + 1);
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

function getExistingSourcePlaylistId(studioName: string, source: 'discogs' | 'musicbrainz'): number | undefined {
  const sourcePrompt = `[system] studio evidence backfill from ${source} :: ${studioName}`;
  const existing = getPlaylistByPrompt(sourcePrompt) || getPlaylistByCacheKey(sourcePrompt);
  return existing?.id;
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
  const wantsClassicalMusic = promptWantsClassicalMusic(params.prompt);
  const mainstreamPrompt = isMainstreamStudioPrompt(params.prompt) && !wantsClassicalMusic;
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
      const explicitPlaceIds = Array.from(
        new Set(
          [
            ...(Array.isArray(resolved?.musicBrainzPlaceIds) ? resolved.musicBrainzPlaceIds : []),
            typeof resolved?.musicBrainzPlaceId === 'string' ? resolved.musicBrainzPlaceId : '',
          ]
            .map((value) => String(value || '').trim())
            .filter((value) => value.length > 0)
        )
      );

      const discoveredPlaces = await resolveMusicBrainzStudioPlaces(studioName, acceptedStudioNames, 5);
      const placeCandidates = wantsClassicalMusic
        ? [
          ...discoveredPlaces,
          ...explicitPlaceIds.map((id) => ({ id, name: studioName, score: 100, type: 'Studio', disambiguation: '' })),
        ]
        : [
          ...explicitPlaceIds.map((id) => ({ id, name: studioName, score: 100, type: 'Studio', disambiguation: '' })),
          ...discoveredPlaces,
        ];

      const uniquePlaces = new Map<string, { id: string; name: string; score: number; type: string; disambiguation: string }>();
      for (const place of placeCandidates) {
        const existing = uniquePlaces.get(place.id);
        if (!existing || place.score > existing.score) {
          uniquePlaces.set(place.id, place);
        }
      }

      const rankedPlaces = Array.from(uniquePlaces.values())
        .sort((a, b) => b.score - a.score)
        .slice(0, mainstreamPrompt ? 3 : 4);

      if (rankedPlaces.length > 0) {
        musicBrainzPlaceId = rankedPlaces[0].id;
        musicBrainzPlaceName = rankedPlaces[0].name;

        const mbTracks: Array<{
          placeId: string;
          artist: string;
          title: string;
          recordingMbid: string;
          begin: string | null;
          end: string | null;
        }> = [];

        const maxSeed = Math.max(limit * 12, 320);
        const perPlaceLimit = Math.max(120, Math.floor(maxSeed / Math.max(1, rankedPlaces.length)));
        for (const place of rankedPlaces) {
          const rows = await fetchMusicBrainzStudioTracksByPlace(place.id, perPlaceLimit);
          for (const row of rows) {
            mbTracks.push({
              placeId: place.id,
              artist: row.artist,
              title: row.title,
              recordingMbid: row.recordingMbid,
              begin: row.begin,
              end: row.end,
            });
          }
        }

        const mbArtistFrequency = new Map<string, number>();
        for (const row of mbTracks) {
          const artistKey = normalizeArtistKey(canonicalizeDisplayName(row.artist || ''));
          if (!artistKey) continue;
          mbArtistFrequency.set(artistKey, (mbArtistFrequency.get(artistKey) || 0) + 1);
        }

        const mbSeed: StudioSeedTrack[] = [];
        for (let index = 0; index < mbTracks.length; index += 1) {
          const row = mbTracks[index];
          const artist = canonicalizeDisplayName(row.artist || '');
          const title = (row.title || '').trim();
          if (!artist || !title) continue;
          if (isVariantStudioTitle(title)) continue;
          if (!wantsClassicalMusic && !isLikelySongRecording(artist, title)) continue;
          if (!shouldKeepStudioTrackByClassicalPolicy(artist, title, wantsClassicalMusic)) continue;

          const year = extractYear(row.begin) ?? extractYear(row.end);
          if (year !== null) {
            if (typeof activeStartYear === 'number' && year < activeStartYear) continue;
            if (typeof activeEndYear === 'number' && year > activeEndYear) continue;
          }
          if (mainstreamPrompt && !wantsClassicalMusic && typeof activeStartYear === 'number' && year === null) {
            continue;
          }

          const artistKey = normalizeArtistKey(artist);
          const artistFrequency = artistKey ? (mbArtistFrequency.get(artistKey) || 0) : 0;

          mbSeed.push({
            artist,
            title,
            studioName,
            releaseId: 10_000_000 + index,
            releaseTitle: 'MusicBrainz recorded-at relation',
            sourceRef: `musicbrainz:place:${row.placeId}:recording:${row.recordingMbid}`,
            source: 'musicbrainz',
            recordedYear: year ?? undefined,
            mbArtistFrequency: artistFrequency,
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

        studioTracks = uniqueMbSeed;
      }
    } catch {
      // MusicBrainz studio backfill is best-effort.
    }
  }

  const minimumBreadthTarget = Math.min(8, Math.max(4, Math.floor(limit / 3)));
  const hasSufficientMusicBrainzCore = studioTracks.length >= 16 || countUniqueArtists(studioTracks) >= 8;
  const shouldUseDiscogsFallback =
    (!hasSufficientMusicBrainzCore)
    && (
      studioTracks.length < Math.min(40, limit)
      || countUniqueArtists(studioTracks) < minimumBreadthTarget
    );

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
      try {
      const desiredDiscogsSeed = Math.max(limit, 120);
      const mainstreamHintLimit = mainstreamPrompt ? 3 : effectiveArtistHints.length;
      const prioritizedHints = effectiveArtistHints.slice(0, mainstreamHintLimit);

      let discogsTracks: Array<{
        artist: string;
        title: string;
        studioName: string;
        releaseId: number;
        releaseTitle: string;
        sourceRef: string;
      }> = [];

      if (mainstreamPrompt && prioritizedHints.length > 0) {
        discogsTracks = await fetchDiscogsStudioTracksByArtistCatalog(
          studioName,
          prioritizedHints,
          Math.max(20, Math.min(desiredDiscogsSeed, 45)),
          activeStartYear,
          activeEndYear
        );
      }

      const mainstreamQuickMode = mainstreamPrompt && studioTracks.length >= 80;

      const needLabelFetch =
        !mainstreamQuickMode
        && (
        !mainstreamPrompt
        || discogsTracks.length < Math.min(35, limit)
        || countUniqueArtists(discogsTracks) < minimumBreadthTarget
        );

      if (needLabelFetch) {
        const labelTracks = await fetchDiscogsStudioTracksByLabel(
          discogsLabelId,
          studioName,
          desiredDiscogsSeed,
          activeStartYear,
          activeEndYear
        );
        const dedupeKeys = new Set<string>();
        const merged: typeof discogsTracks = [];
        for (const row of [...discogsTracks, ...labelTracks]) {
          const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
          if (dedupeKeys.has(key)) continue;
          dedupeKeys.add(key);
          merged.push(row);
          if (merged.length >= desiredDiscogsSeed) break;
        }
        discogsTracks = merged;
      }

      if (prioritizedHints.length > 0) {
        const hintTracks = await fetchDiscogsStudioTracksByArtistHints(
          studioName,
          prioritizedHints,
          mainstreamQuickMode
            ? Math.max(15, Math.min(limit, 30))
            : Math.max(35, Math.min(limit, 80)),
          activeStartYear,
          activeEndYear
        );
        const dedupeKeys = new Set<string>();
        const merged: typeof discogsTracks = [];
        for (const row of [...hintTracks, ...discogsTracks]) {
          const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
          if (dedupeKeys.has(key)) continue;
          dedupeKeys.add(key);
          merged.push(row);
          if (merged.length >= desiredDiscogsSeed) break;
        }
        discogsTracks = merged;
      }

      if (!mainstreamQuickMode && (discogsTracks.length < Math.min(30, limit) || countUniqueArtists(discogsTracks) < minimumBreadthTarget) && prioritizedHints.length > 0) {
        const catalogTracks = await fetchDiscogsStudioTracksByArtistCatalog(studioName, prioritizedHints, Math.max(40, Math.min(limit, 90)), activeStartYear, activeEndYear);
        const dedupeKeys = new Set<string>();
        const merged: typeof discogsTracks = [];
        for (const row of [...catalogTracks, ...discogsTracks]) {
          const key = `${row.artist.toLowerCase()}::${row.title.toLowerCase()}::${row.releaseId}`;
          if (dedupeKeys.has(key)) continue;
          dedupeKeys.add(key);
          merged.push(row);
          if (merged.length >= desiredDiscogsSeed) break;
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
        source: 'discogs' as const,
      }))
        .filter((row) => {
          if (!row.artist || !row.title) return false;
          if (isVariantStudioTitle(row.title)) return false;
          if (!wantsClassicalMusic && !isLikelySongRecording(row.artist, row.title)) return false;
          return shouldKeepStudioTrackByClassicalPolicy(row.artist, row.title, wantsClassicalMusic);
        });

      const mergedByKey = new Map<string, StudioSeedTrack>();
      const hasStrongMbBase = studioTracks.length >= 24 && countUniqueArtists(studioTracks) >= 10;
      const trimmedDiscogsSeed = hasStrongMbBase ? discogsSeed.slice(0, 36) : discogsSeed;
      const protectedMb = mainstreamPrompt ? studioTracks.slice(0, 24) : studioTracks.slice(0, 12);
      const sourcePriority = [...protectedMb, ...trimmedDiscogsSeed, ...studioTracks.slice(protectedMb.length)];
      for (const row of sourcePriority) {
        const key = normalizeTrackKey(row.artist, row.title);
        if (!key || mergedByKey.has(key)) continue;
        mergedByKey.set(key, row);
      }
      studioTracks = Array.from(mergedByKey.values());
      } catch {
        // Discogs fallback should not fail studio generation if rate-limited.
      }
    }
  }

  studioTracks = prioritizeStudioSeedTracks(studioTracks, preferredArtistKeys, mainstreamPrompt, wantsClassicalMusic);

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

  studioTracks = limitStudioSeedTracksByArtist(studioTracks, Math.max(limit * 3, 180), 4);

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

  const studioCanonicalKeys = Array.from(
    new Set(
      [studioName, ...acceptedStudioNames]
        .map((value) => buildStudioCanonicalKey(value))
        .filter((value) => value.length > 0)
    )
  );
  const studioCanonical = studioCanonicalKeys[0] || buildStudioCanonicalKey(studioName);
  const sourcesUsed = new Set<'discogs' | 'musicbrainz'>(studioTracks.map((row) => row.source));
  const sourcePlaylistBySource = new Map<'discogs' | 'musicbrainz', number>();
  for (const source of ['discogs', 'musicbrainz'] as const) {
    const sourcePlaylistId = sourcesUsed.has(source)
      ? ensureSourcePlaylistId(studioName, source)
      : getExistingSourcePlaylistId(studioName, source);
    if (typeof sourcePlaylistId !== 'number' || !Number.isFinite(sourcePlaylistId)) continue;
    if (sourcesUsed.has(source)) {
      sourcePlaylistBySource.set(source, sourcePlaylistId);
    }
    for (const canonicalKey of studioCanonicalKeys) {
      deletePriorStudioEvidenceForSource.run(sourcePlaylistId, canonicalKey);
    }
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
