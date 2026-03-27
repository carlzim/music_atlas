import { buildStudioCanonicalKey } from './normalize.js';

export interface DiscogsReleaseCredit {
  artist: string;
  title: string;
  creditName: string;
  creditRole: string;
  releaseId: number;
  releaseTitle: string;
  sourceRef: string;
}

const DISCOGS_BASE_URL = 'https://api.discogs.com';
const MIN_REQUEST_INTERVAL_MS = 1200;
const DEFAULT_DISCOGS_FETCH_TIMEOUT_MS = 15000;
let lastRequestStartedAt = 0;
const studioLabelSearchCache = new Map<string, number | null>();

function getDiscogsFetchTimeoutMs(): number {
  const parsed = Number(process.env.DISCOGS_FETCH_TIMEOUT_MS || DEFAULT_DISCOGS_FETCH_TIMEOUT_MS);
  if (!Number.isFinite(parsed) || parsed < 2000) return DEFAULT_DISCOGS_FETCH_TIMEOUT_MS;
  return Math.floor(parsed);
}

function isRetryableDiscogsStatus(status: number): boolean {
  return status === 429 || status === 503 || status === 502 || status === 504;
}

function getDiscogsUserAgent(): string {
  const configured = (process.env.DISCOGS_USER_AGENT || '').trim();
  if (configured) return configured;
  return 'playlist-app/1.0 (+local-dev)';
}

function getDiscogsToken(): string {
  return (process.env.DISCOGS_TOKEN || '').trim();
}

function getDiscogsConsumerKey(): string {
  return (process.env.DISCOGS_CONSUMER_KEY || '').trim();
}

function getDiscogsConsumerSecret(): string {
  return (process.env.DISCOGS_CONSUMER_SECRET || '').trim();
}

export function isDiscogsConfigured(): boolean {
  const token = getDiscogsToken();
  if (token.length > 0) return true;
  return getDiscogsConsumerKey().length > 0 && getDiscogsConsumerSecret().length > 0;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function rateLimitedDiscogsFetch(url: string): Promise<unknown> {
  const token = getDiscogsToken();
  const consumerKey = getDiscogsConsumerKey();
  const consumerSecret = getDiscogsConsumerSecret();
  if (!token && !(consumerKey && consumerSecret)) {
    throw new Error('Missing DISCOGS_TOKEN or DISCOGS_CONSUMER_KEY/DISCOGS_CONSUMER_SECRET');
  }

  const now = Date.now();
  const elapsed = now - lastRequestStartedAt;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await sleep(MIN_REQUEST_INTERVAL_MS - elapsed);
  }
  lastRequestStartedAt = Date.now();

  const retryDelays = [0, 1200, 2400];
  let lastError = '';

  for (let i = 0; i < retryDelays.length; i += 1) {
    if (retryDelays[i] > 0) {
      await sleep(retryDelays[i]);
    }

    const requestUrl = !token
      ? (() => {
          const parsed = new URL(url);
          parsed.searchParams.set('key', consumerKey);
          parsed.searchParams.set('secret', consumerSecret);
          return parsed.toString();
        })()
      : url;

    const headers: Record<string, string> = {
      'User-Agent': getDiscogsUserAgent(),
    };
    if (token) {
      headers.Authorization = `Discogs token=${token}`;
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), getDiscogsFetchTimeoutMs());
    let response: Response;
    try {
      response = await fetch(requestUrl, { headers, signal: controller.signal });
    } catch (error) {
      clearTimeout(timeout);
      const message = error instanceof Error ? error.message : String(error);
      const aborted = error instanceof DOMException && error.name === 'AbortError';
      const detail = aborted
        ? `Discogs request timed out after ${getDiscogsFetchTimeoutMs()}ms`
        : `Discogs request failed: ${message.slice(0, 240)}`;
      lastError = detail;
      if (i === retryDelays.length - 1) {
        throw new Error(detail);
      }
      continue;
    }
    clearTimeout(timeout);

    if (response.ok) {
      return response.json();
    }

    const body = await response.text().catch(() => '');
    const detail = `Discogs request failed (${response.status}): ${body.slice(0, 240)}`;
    lastError = detail;

    if (!isRetryableDiscogsStatus(response.status) || i === retryDelays.length - 1) {
      throw new Error(detail);
    }
  }

  throw new Error(lastError || 'Discogs request failed');
}

function normalizeDiscogsRole(role: string): string | null {
  const normalized = role.trim().toLowerCase();
  if (!normalized) return null;

  if (normalized.includes('producer')) return 'producer';
  if (normalized.includes('engineer') || normalized.includes('mixed') || normalized.includes('mix')) return 'engineer';
  if (normalized.includes('arrang')) return 'arranger';
  if (normalized.includes('art direction')) return 'art_director';
  if (normalized.includes('photograph')) return 'photographer';
  if (normalized.includes('design') || normalized.includes('sleeve')) return 'cover_designer';
  return null;
}

function normalizeDiscogsName(value: string): string {
  return value
    .toLowerCase()
    .replace(/\*+/g, ' ')
    .replace(/\(\d+\)/g, ' ')
    .replace(/[’']/g, "'")
    .replace(/[^a-z0-9\s/&,+-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeName(value: string): string[] {
  return normalizeDiscogsName(value)
    .replace(/[\/,+&-]/g, ' ')
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length >= 2);
}

function discogsNameMatches(candidate: string, target: string): boolean {
  const candidateNorm = normalizeDiscogsName(candidate);
  const targetNorm = normalizeDiscogsName(target);
  if (!candidateNorm || !targetNorm) return false;
  if (candidateNorm === targetNorm) return true;
  if (candidateNorm.includes(targetNorm)) return true;

  const candidateTokens = new Set(tokenizeName(candidate));
  const targetTokens = tokenizeName(target);
  if (candidateTokens.size === 0 || targetTokens.length === 0) return false;

  for (const token of targetTokens) {
    if (!candidateTokens.has(token)) return false;
  }
  return true;
}

function extractDiscogsArtistName(value: unknown): string {
  if (!value || typeof value !== 'object') return '';
  const row = value as { name?: unknown; anv?: unknown };
  if (typeof row.anv === 'string' && row.anv.trim().length > 0) return row.anv.trim();
  if (typeof row.name === 'string' && row.name.trim().length > 0) return row.name.trim();
  return '';
}

function hasMatchingCredit(entries: unknown[], creditName: string, normalizedRole: string): boolean {
  for (const entry of entries) {
    if (!entry || typeof entry !== 'object') continue;
    const row = entry as { role?: unknown; roles?: unknown; name?: unknown; anv?: unknown };
    const roleRaw = typeof row.role === 'string'
      ? row.role
      : Array.isArray(row.roles)
        ? row.roles.filter((r): r is string => typeof r === 'string').join(', ')
        : '';
    const mapped = normalizeDiscogsRole(roleRaw);
    if (mapped !== normalizedRole) continue;

    const candidate = extractDiscogsArtistName(row);
    if (!candidate) continue;
    if (discogsNameMatches(candidate, creditName)) return true;
  }
  return false;
}

export async function fetchDiscogsReleaseCreditsByQuery(
  query: string,
  creditName: string,
  creditRole: string,
  limit = 20
): Promise<DiscogsReleaseCredit[]> {
  const normalizedRole = normalizeDiscogsRole(creditRole);
  if (!normalizedRole) return [];

  const safeLimit = Math.max(1, Math.min(50, Math.floor(limit)));
  const searchByCreditUrl = `${DISCOGS_BASE_URL}/database/search?credit=${encodeURIComponent(creditName)}&type=release&per_page=${safeLimit}`;
  const searchByCreditRaw = await rateLimitedDiscogsFetch(searchByCreditUrl) as { results?: unknown[] };
  const byCredit = Array.isArray(searchByCreditRaw.results) ? searchByCreditRaw.results : [];
  let byQuery: unknown[] = [];

  if (query.trim().length > 0) {
    const searchByQueryUrl = `${DISCOGS_BASE_URL}/database/search?q=${encodeURIComponent(query)}&type=release&per_page=${safeLimit}`;
    const searchByQueryRaw = await rateLimitedDiscogsFetch(searchByQueryUrl) as { results?: unknown[] };
    byQuery = Array.isArray(searchByQueryRaw.results) ? searchByQueryRaw.results : [];
  }

  const releaseSearchDedup = new Set<number>();
  const results: unknown[] = [];
  for (const row of [...byCredit, ...byQuery]) {
    if (!row || typeof row !== 'object') continue;
    const value = row as { id?: unknown };
    const releaseId = typeof value.id === 'number' ? value.id : Number(value.id || 0);
    if (!Number.isFinite(releaseId) || releaseId <= 0 || releaseSearchDedup.has(releaseId)) continue;
    releaseSearchDedup.add(releaseId);
    results.push(row);
    if (results.length >= safeLimit) break;
  }

  const output: DiscogsReleaseCredit[] = [];
  const dedupe = new Set<string>();

  for (const result of results) {
    if (!result || typeof result !== 'object') continue;
    const row = result as { id?: unknown; title?: unknown };
    const releaseId = typeof row.id === 'number' ? row.id : Number(row.id || 0);
    const releaseTitle = typeof row.title === 'string' ? row.title.trim() : '';
    if (!Number.isFinite(releaseId) || releaseId <= 0) continue;

    const releaseUrl = `${DISCOGS_BASE_URL}/releases/${releaseId}`;
    const releaseRaw = await rateLimitedDiscogsFetch(releaseUrl) as {
      title?: unknown;
      tracklist?: unknown[];
      extraartists?: unknown[];
    };

    const releaseName = typeof releaseRaw.title === 'string' && releaseRaw.title.trim().length > 0
      ? releaseRaw.title.trim()
      : releaseTitle;
    const releaseArtists = Array.isArray((releaseRaw as { artists?: unknown[] }).artists)
      ? ((releaseRaw as { artists?: unknown[] }).artists || [])
          .map((artist) => extractDiscogsArtistName(artist))
          .filter((value) => value.length > 0)
      : [];

    const releaseCredits = Array.isArray(releaseRaw.extraartists) ? releaseRaw.extraartists : [];
    const releaseHasMatchingCredit = hasMatchingCredit(releaseCredits, creditName, normalizedRole);

    const tracks = Array.isArray(releaseRaw.tracklist) ? releaseRaw.tracklist : [];
    for (const track of tracks) {
      if (!track || typeof track !== 'object') continue;
      const trackRow = track as {
        title?: unknown;
        artists?: unknown[];
      };
      const trackTitle = typeof trackRow.title === 'string' ? trackRow.title.trim() : '';
      if (!trackTitle) continue;

      const trackCredits = Array.isArray((trackRow as { extraartists?: unknown[] }).extraartists)
        ? ((trackRow as { extraartists?: unknown[] }).extraartists || [])
        : [];
      const trackHasMatchingCredit = hasMatchingCredit(trackCredits, creditName, normalizedRole);
      if (!releaseHasMatchingCredit && !trackHasMatchingCredit) {
        continue;
      }

      const artistName = Array.isArray(trackRow.artists) && trackRow.artists.length > 0
        ? (() => {
            const first = trackRow.artists?.[0] as { name?: unknown; anv?: unknown } | undefined;
            return extractDiscogsArtistName(first || null);
          })()
        : (releaseArtists[0] || '');
      if (!artistName) continue;

      const key = `${artistName.toLowerCase()}::${trackTitle.toLowerCase()}::${normalizedRole}::${creditName.toLowerCase()}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);

      output.push({
        artist: artistName,
        title: trackTitle,
        creditName: creditName.trim(),
        creditRole: normalizedRole,
        releaseId,
        releaseTitle: releaseName,
        sourceRef: `discogs:release:${releaseId}`,
      });
    }
  }

  return output;
}

export interface DiscogsStudioTrack {
  artist: string;
  title: string;
  studioName: string;
  releaseId: number;
  releaseTitle: string;
  sourceRef: string;
}

function studioNameLikelyMatches(candidate: string, target: string): boolean {
  const a = buildStudioCanonicalKey(candidate);
  const b = buildStudioCanonicalKey(target);
  if (!a || !b) return false;
  if (a === b) return true;
  return a.includes(b) || b.includes(a);
}

function getDiscogsStudioMaxReleaseFetches(): number {
  const parsed = Number(process.env.DISCOGS_STUDIO_MAX_RELEASE_FETCHES || 12);
  if (!Number.isFinite(parsed) || parsed < 8) return 12;
  return Math.max(8, Math.min(80, Math.floor(parsed)));
}

function extractReleaseArtists(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => extractDiscogsArtistName(item))
    .filter((name) => name.length > 0);
}

export async function fetchDiscogsStudioTracksByLabel(
  labelId: number,
  targetStudioName: string,
  limit = 120
): Promise<DiscogsStudioTrack[]> {
  const safeLabelId = Math.floor(Number(labelId));
  const targetStudio = targetStudioName.trim();
  const safeLimit = Math.max(1, Math.min(400, Math.floor(limit)));
  if (!Number.isFinite(safeLabelId) || safeLabelId <= 0 || !targetStudio) return [];

  const releaseIds: number[] = [];
  const seenReleaseIds = new Set<number>();
  const perPage = 100;
  const maxPages = 4;
  const maxReleaseFetches = Math.min(safeLimit, getDiscogsStudioMaxReleaseFetches());

  for (let page = 1; page <= maxPages; page += 1) {
    const url = `${DISCOGS_BASE_URL}/labels/${safeLabelId}/releases?per_page=${perPage}&page=${page}`;
    const raw = await rateLimitedDiscogsFetch(url) as { releases?: unknown[]; pagination?: { pages?: unknown } };
    const releases = Array.isArray(raw.releases) ? raw.releases : [];

    for (const item of releases) {
      if (!item || typeof item !== 'object') continue;
      const row = item as { id?: unknown; main_release?: unknown };
      const idFromMain = typeof row.main_release === 'number' ? row.main_release : Number(row.main_release || 0);
      const id = idFromMain > 0 ? idFromMain : (typeof row.id === 'number' ? row.id : Number(row.id || 0));
      if (!Number.isFinite(id) || id <= 0 || seenReleaseIds.has(id)) continue;
      seenReleaseIds.add(id);
      releaseIds.push(id);
      if (releaseIds.length >= maxReleaseFetches) break;
    }

    const totalPages = Number((raw.pagination as { pages?: unknown } | undefined)?.pages || page);
    if (releaseIds.length >= maxReleaseFetches || !Number.isFinite(totalPages) || page >= totalPages) break;
  }

  const output: DiscogsStudioTrack[] = [];
  const dedupe = new Set<string>();

  for (const releaseId of releaseIds) {
    if (output.length >= safeLimit) break;

    const releaseUrl = `${DISCOGS_BASE_URL}/releases/${releaseId}`;
    const releaseRaw = await rateLimitedDiscogsFetch(releaseUrl) as {
      title?: unknown;
      artists?: unknown[];
      labels?: unknown[];
      companies?: unknown[];
      tracklist?: unknown[];
    };

    const releaseTitle = typeof releaseRaw.title === 'string' ? releaseRaw.title.trim() : '';
    const releaseArtists = extractReleaseArtists(releaseRaw.artists);

    const companyRecordedAtNames = Array.isArray(releaseRaw.companies)
      ? releaseRaw.companies
          .map((item) => {
            if (!item || typeof item !== 'object') return { name: '', role: '' };
            const value = item as { name?: unknown; entity_type_name?: unknown };
            const name = typeof value.name === 'string' ? value.name.trim() : '';
            const role = typeof value.entity_type_name === 'string' ? value.entity_type_name.trim().toLowerCase() : '';
            return { name, role };
          })
          .filter((row) => row.name.length > 0 && row.role.includes('recorded'))
          .map((row) => row.name)
      : [];

    const recordedAtHints = Array.from(new Set(companyRecordedAtNames));
    if (recordedAtHints.length > 0) {
      const hasRecordedAtStudioMatch = recordedAtHints.some((name) => studioNameLikelyMatches(name, targetStudio));
      if (!hasRecordedAtStudioMatch) continue;
    }

    const tracklist = Array.isArray(releaseRaw.tracklist) ? releaseRaw.tracklist : [];
    for (const track of tracklist) {
      if (output.length >= safeLimit) break;
      if (!track || typeof track !== 'object') continue;
      const row = track as { title?: unknown; artists?: unknown[]; type_?: unknown; type?: unknown };
      const title = typeof row.title === 'string' ? row.title.trim() : '';
      if (!title) continue;

      const typeValue = typeof row.type_ === 'string'
        ? row.type_.trim().toLowerCase()
        : typeof row.type === 'string'
          ? row.type.trim().toLowerCase()
          : '';
      if (typeValue && typeValue !== 'track' && typeValue !== 'index') continue;

      const trackArtists = extractReleaseArtists(row.artists);
      const artist = (trackArtists[0] || releaseArtists[0] || '').trim();
      if (!artist) continue;

      const key = `${artist.toLowerCase()}::${title.toLowerCase()}::${safeLabelId}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);

      output.push({
        artist,
        title,
        studioName: targetStudio,
        releaseId,
        releaseTitle,
        sourceRef: `discogs:label:${safeLabelId}:release:${releaseId}`,
      });
    }
  }

  return output;
}

export async function searchDiscogsStudioLabelId(studioName: string): Promise<number | null> {
  const targetStudio = studioName.trim();
  if (!targetStudio) return null;

  const cacheKey = buildStudioCanonicalKey(targetStudio);
  if (cacheKey) {
    const cached = studioLabelSearchCache.get(cacheKey);
    if (cached !== undefined) return cached;
  }

  const url = `${DISCOGS_BASE_URL}/database/search?type=label&q=${encodeURIComponent(targetStudio)}&per_page=12`;
  const raw = await rateLimitedDiscogsFetch(url) as { results?: unknown[] };
  const results = Array.isArray(raw.results) ? raw.results : [];

  let best: { id: number; score: number } | null = null;
  for (const item of results) {
    if (!item || typeof item !== 'object') continue;
    const row = item as { id?: unknown; title?: unknown; label?: unknown; type?: unknown };
    const type = typeof row.type === 'string' ? row.type.trim().toLowerCase() : '';
    if (type && type !== 'label') continue;

    const id = typeof row.id === 'number' ? row.id : Number(row.id || 0);
    if (!Number.isFinite(id) || id <= 0) continue;

    const candidates = [row.title, row.label]
      .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
      .map((value) => value.trim());
    if (candidates.length === 0) continue;

    let score = 0;
    for (const candidate of candidates) {
      if (!studioNameLikelyMatches(candidate, targetStudio)) continue;
      const candidateKey = buildStudioCanonicalKey(candidate);
      const targetKey = buildStudioCanonicalKey(targetStudio);
      if (candidateKey && targetKey && candidateKey === targetKey) {
        score = Math.max(score, 1000 + candidate.length);
      } else {
        score = Math.max(score, 500 + candidate.length);
      }
    }
    if (score <= 0) continue;

    if (!best || score > best.score) {
      best = { id, score };
    }
  }

  const resolved = best?.id || null;
  if (cacheKey) {
    studioLabelSearchCache.set(cacheKey, resolved);
  }

  return resolved;
}
