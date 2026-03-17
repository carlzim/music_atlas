export interface MusicBrainzArtistSearchResult {
  id: string;
  name: string;
  score: number;
  type: string;
}

interface MusicBrainzCreditTrack {
  artist: string;
  title: string;
  relationType: string;
  recordingMbid: string;
}

interface MusicBrainzReleaseRelation {
  releaseMbid: string;
  relationType: string;
  releaseArtistCredit: unknown;
}

export interface MusicBrainzMembershipEdge {
  personMbid: string;
  personName: string;
  groupMbid: string;
  groupName: string;
  memberRole: string | null;
  begin: string | null;
  end: string | null;
  sourceRef: string;
}

const MUSICBRAINZ_BASE_URL = 'https://musicbrainz.org/ws/2';
const MIN_REQUEST_INTERVAL_MS = 1100;
let lastRequestStartedAt = 0;

function getUserAgent(): string {
  const configured = (process.env.MUSICBRAINZ_USER_AGENT || '').trim();
  if (configured) return configured;
  return 'playlist-app/1.0 (music evidence backfill)';
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimitedFetchJson(url: string): Promise<unknown> {
  const now = Date.now();
  const elapsed = now - lastRequestStartedAt;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await sleep(MIN_REQUEST_INTERVAL_MS - elapsed);
  }
  lastRequestStartedAt = Date.now();

  const response = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'User-Agent': getUserAgent(),
    },
  });

  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`MusicBrainz request failed (${response.status}): ${body.slice(0, 240)}`);
  }

  return response.json();
}

function normalizeMbType(value: string | null | undefined): string {
  return (value || '').trim().toLowerCase();
}

function formatArtistCredit(artistCredit: unknown): string {
  if (!Array.isArray(artistCredit)) return '';

  let combined = '';
  for (const item of artistCredit) {
    if (!item || typeof item !== 'object') continue;
    const entry = item as { name?: unknown; artist?: { name?: unknown }; joinphrase?: unknown };
    const name = typeof entry.name === 'string'
      ? entry.name
      : typeof entry.artist?.name === 'string'
        ? entry.artist.name
        : '';
    const joinphrase = typeof entry.joinphrase === 'string' ? entry.joinphrase : '';
    if (!name) continue;
    combined += `${name}${joinphrase}`;
  }

  return combined.trim();
}

function mapCreditRoleToRelationTypes(role: string): Set<string> {
  const normalized = role.trim().toLowerCase();
  if (normalized === 'producer') {
    return new Set(['producer']);
  }
  if (normalized === 'engineer') {
    return new Set(['engineer', 'recording engineer']);
  }
  if (normalized === 'arranger') {
    return new Set(['arranger', 'orchestrator']);
  }
  return new Set([normalized]);
}

function addCreditTrackIfUnique(
  output: MusicBrainzCreditTrack[],
  dedupe: Set<string>,
  artist: string,
  title: string,
  relationType: string,
  recordingMbid: string,
  limit: number
): void {
  const cleanArtist = artist.trim();
  const cleanTitle = title.trim();
  const cleanMbid = recordingMbid.trim();
  if (!cleanArtist || !cleanTitle || !cleanMbid) return;

  const key = `${cleanMbid}::${cleanArtist.toLowerCase()}::${cleanTitle.toLowerCase()}`;
  if (dedupe.has(key)) return;
  dedupe.add(key);
  output.push({ artist: cleanArtist, title: cleanTitle, relationType, recordingMbid: cleanMbid });
  if (output.length > limit) {
    output.length = limit;
  }
}

async function fetchTracksFromMusicBrainzReleaseRelation(
  relation: MusicBrainzReleaseRelation,
  relationTypes: Set<string>,
  limit: number
): Promise<MusicBrainzCreditTrack[]> {
  if (limit <= 0) return [];

  const url = `${MUSICBRAINZ_BASE_URL}/release/${encodeURIComponent(relation.releaseMbid)}?inc=recordings+artist-credits&fmt=json`;
  const raw = await rateLimitedFetchJson(url) as {
    media?: unknown[];
    ['artist-credit']?: unknown;
    title?: unknown;
  };

  const media = Array.isArray(raw.media) ? raw.media : [];
  const releaseArtist = formatArtistCredit(raw['artist-credit']) || formatArtistCredit(relation.releaseArtistCredit);
  const tracks: MusicBrainzCreditTrack[] = [];
  const dedupe = new Set<string>();

  for (const medium of media) {
    if (!medium || typeof medium !== 'object') continue;
    const mediumRow = medium as { tracks?: unknown[] };
    const mediumTracks = Array.isArray(mediumRow.tracks) ? mediumRow.tracks : [];

    for (const item of mediumTracks) {
      if (!item || typeof item !== 'object') continue;
      const trackRow = item as {
        title?: unknown;
        recording?: { id?: unknown; title?: unknown; ['artist-credit']?: unknown };
        ['artist-credit']?: unknown;
      };
      const recording = trackRow.recording;
      const recordingMbid = typeof recording?.id === 'string' && recording.id.trim().length > 0
        ? recording.id.trim()
        : `${relation.releaseMbid}::${tracks.length + 1}`;
      const title = typeof recording?.title === 'string' && recording.title.trim().length > 0
        ? recording.title.trim()
        : typeof trackRow.title === 'string'
          ? trackRow.title.trim()
          : '';
      const artist = formatArtistCredit(trackRow['artist-credit'])
        || formatArtistCredit(recording?.['artist-credit'])
        || releaseArtist;

      addCreditTrackIfUnique(tracks, dedupe, artist, title, relation.relationType, recordingMbid, limit);
      if (tracks.length >= limit) return tracks;
    }
  }

  if (tracks.length === 0 && relationTypes.has(relation.relationType)) {
    const releaseTitle = typeof raw.title === 'string' ? raw.title.trim() : '';
    addCreditTrackIfUnique(
      tracks,
      dedupe,
      releaseArtist,
      releaseTitle,
      relation.relationType,
      `${relation.releaseMbid}::release-title`,
      limit
    );
  }

  return tracks;
}

export async function searchMusicBrainzArtistsByName(name: string, limit = 5): Promise<MusicBrainzArtistSearchResult[]> {
  const trimmed = name.trim();
  if (!trimmed) return [];

  const safeLimit = Math.max(1, Math.min(20, Math.floor(limit)));
  const query = encodeURIComponent(trimmed);
  const url = `${MUSICBRAINZ_BASE_URL}/artist/?query=${query}&fmt=json&limit=${safeLimit}`;
  const raw = await rateLimitedFetchJson(url) as { artists?: unknown[] };
  const artists = Array.isArray(raw.artists) ? raw.artists : [];

  return artists
    .map((item) => {
      if (!item || typeof item !== 'object') return null;
      const row = item as { id?: unknown; name?: unknown; score?: unknown; type?: unknown };
      const id = typeof row.id === 'string' ? row.id.trim() : '';
      const artistName = typeof row.name === 'string' ? row.name.trim() : '';
      const score = Number(row.score || 0);
      const type = typeof row.type === 'string' ? row.type.trim() : '';
      if (!id || !artistName) return null;
      return { id, name: artistName, score, type };
    })
    .filter((item): item is MusicBrainzArtistSearchResult => Boolean(item));
}

export async function resolveMusicBrainzPerson(name: string): Promise<MusicBrainzArtistSearchResult | null> {
  const candidates = await searchMusicBrainzArtistsByName(name, 10);
  if (candidates.length === 0) return null;

  const people = candidates.filter((candidate) => normalizeMbType(candidate.type) === 'person');
  const pool = people.length > 0 ? people : candidates;
  pool.sort((a, b) => b.score - a.score);
  return pool[0] || null;
}

export async function resolveMusicBrainzArtist(name: string): Promise<MusicBrainzArtistSearchResult | null> {
  const candidates = await searchMusicBrainzArtistsByName(name, 10);
  if (candidates.length === 0) return null;

  const preferred = candidates.filter((candidate) => {
    const type = normalizeMbType(candidate.type);
    return type === 'group' || type === 'person';
  });
  const pool = preferred.length > 0 ? preferred : candidates;
  pool.sort((a, b) => b.score - a.score);
  return pool[0] || null;
}

export async function fetchMusicBrainzCreditTracks(
  artistMbid: string,
  creditRole: string,
  limit = 200
): Promise<MusicBrainzCreditTrack[]> {
  const mbid = artistMbid.trim();
  if (!mbid) return [];

  const relationTypes = mapCreditRoleToRelationTypes(creditRole);
  const safeLimit = Math.max(1, Math.min(1000, Math.floor(limit)));
  const url = `${MUSICBRAINZ_BASE_URL}/artist/${encodeURIComponent(mbid)}?inc=recording-rels+release-rels+artist-credits&fmt=json`;
  const raw = await rateLimitedFetchJson(url) as { relations?: unknown[] };
  const relations = Array.isArray(raw.relations) ? raw.relations : [];

  const dedupe = new Set<string>();
  const output: MusicBrainzCreditTrack[] = [];
  const releaseRelations: MusicBrainzReleaseRelation[] = [];

  for (const relation of relations) {
    if (!relation || typeof relation !== 'object') continue;
    const row = relation as {
      type?: unknown;
      ['target-type']?: unknown;
      recording?: { id?: unknown; title?: unknown; ['artist-credit']?: unknown };
      release?: { id?: unknown; ['artist-credit']?: unknown };
    };

    const relationType = typeof row.type === 'string' ? row.type.trim().toLowerCase() : '';
    const targetType = typeof row['target-type'] === 'string' ? row['target-type'].trim().toLowerCase() : '';
    if (!relationType || !relationTypes.has(relationType)) continue;
    if (targetType === 'recording') {
      const recording = row.recording;
      const recordingMbid = typeof recording?.id === 'string' ? recording.id.trim() : '';
      const title = typeof recording?.title === 'string' ? recording.title.trim() : '';
      const artist = formatArtistCredit(recording?.['artist-credit']);
      addCreditTrackIfUnique(output, dedupe, artist, title, relationType, recordingMbid, safeLimit);
      if (output.length >= safeLimit) break;
      continue;
    }

    if (targetType === 'release') {
      const release = row.release;
      const releaseMbid = typeof release?.id === 'string' ? release.id.trim() : '';
      if (!releaseMbid) continue;
      releaseRelations.push({
        releaseMbid,
        relationType,
        releaseArtistCredit: release?.['artist-credit'],
      });
    }
  }

  if (output.length < safeLimit && releaseRelations.length > 0) {
    const releaseDedupe = new Set<string>();
    const uniqueReleaseRelations = releaseRelations.filter((item) => {
      const key = `${item.releaseMbid}::${item.relationType}`;
      if (releaseDedupe.has(key)) return false;
      releaseDedupe.add(key);
      return true;
    });

    const MAX_RELEASE_FETCHES = Math.max(5, Math.min(80, Math.floor(safeLimit / 2)));
    for (const relation of uniqueReleaseRelations.slice(0, MAX_RELEASE_FETCHES)) {
      if (output.length >= safeLimit) break;
      try {
        const releaseTracks = await fetchTracksFromMusicBrainzReleaseRelation(
          relation,
          relationTypes,
          safeLimit - output.length
        );
        for (const track of releaseTracks) {
          addCreditTrackIfUnique(
            output,
            dedupe,
            track.artist,
            track.title,
            track.relationType,
            track.recordingMbid,
            safeLimit
          );
          if (output.length >= safeLimit) break;
        }
      } catch {
        // Release expansion is best-effort.
      }
    }
  }

  return output;
}

export async function fetchMusicBrainzGroupMembers(
  groupMbid: string,
  limit = 80
): Promise<MusicBrainzMembershipEdge[]> {
  const mbid = groupMbid.trim();
  if (!mbid) return [];

  const safeLimit = Math.max(1, Math.min(500, Math.floor(limit)));
  const url = `${MUSICBRAINZ_BASE_URL}/artist/${encodeURIComponent(mbid)}?inc=artist-rels&fmt=json`;
  const raw = await rateLimitedFetchJson(url) as {
    name?: unknown;
    relations?: unknown[];
  };

  const groupName = typeof raw.name === 'string' ? raw.name.trim() : '';
  const relations = Array.isArray(raw.relations) ? raw.relations : [];
  const dedupe = new Set<string>();
  const output: MusicBrainzMembershipEdge[] = [];

  for (const relation of relations) {
    if (!relation || typeof relation !== 'object') continue;
    const row = relation as {
      type?: unknown;
      direction?: unknown;
      ['target-type']?: unknown;
      ['type-id']?: unknown;
      begin?: unknown;
      end?: unknown;
      attributes?: unknown;
      artist?: { id?: unknown; name?: unknown };
    };

    const relationType = typeof row.type === 'string' ? row.type.trim().toLowerCase() : '';
    const targetType = typeof row['target-type'] === 'string' ? row['target-type'].trim().toLowerCase() : '';
    if (relationType !== 'member of band' || targetType !== 'artist') continue;

    const artist = row.artist;
    const personMbid = typeof artist?.id === 'string' ? artist.id.trim() : '';
    const personName = typeof artist?.name === 'string' ? artist.name.trim() : '';
    if (!personMbid || !personName || !groupName) continue;

    const attributes = Array.isArray(row.attributes)
      ? row.attributes.filter((item): item is string => typeof item === 'string' && item.trim().length > 0).map((item) => item.trim())
      : [];
    const role = attributes.length > 0 ? attributes[0] : null;
    const begin = typeof row.begin === 'string' && row.begin.trim().length > 0 ? row.begin.trim() : null;
    const end = typeof row.end === 'string' && row.end.trim().length > 0 ? row.end.trim() : null;
    const typeId = typeof row['type-id'] === 'string' ? row['type-id'].trim() : '';

    const dedupeKey = `${personMbid}::${groupMbid}`;
    if (dedupe.has(dedupeKey)) continue;
    dedupe.add(dedupeKey);

    output.push({
      personMbid,
      personName,
      groupMbid: mbid,
      groupName,
      memberRole: role,
      begin,
      end,
      sourceRef: `${typeId || 'member-of-band'}::${personMbid}::${mbid}`,
    });

    if (output.length >= safeLimit) break;
  }

  return output;
}
