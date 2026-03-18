import { buildArtistCanonicalKey, buildPersonCanonicalKey, canonicalizeDisplayName } from './normalize.js';

export interface SpotifyTrackInfo {
  spotify_url: string | null;
  album_image_url: string | null;
  release_year: number | null;
}

export interface SpotifyTrackDebugInfo extends SpotifyTrackInfo {
  score: number | null;
  matchedTitle: string | null;
  matchedAlbumTitle: string | null;
}

interface SpotifyCandidate {
  spotify_id: string | null;
  spotify_uri: string | null;
  spotify_url: string | null;
  album_image_url: string | null;
  title: string;
  album_title: string;
  artists: string[];
  release_year: number | null;
  popularity: number;
  duration_ms: number | null;
}

interface SpotifySearchResult {
  candidates: SpotifyCandidate[];
  rateLimitedAbort: boolean;
}

interface ArtistSearchCacheEntry {
  candidates: SpotifyCandidate[];
  rateLimitedAbort: boolean;
  fetchedAt: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let accessToken: string | null = null;
let tokenExpiry: number = 0;
const artistSearchCache = new Map<string, ArtistSearchCacheEntry>();
const ARTIST_CACHE_TTL_MS = 10 * 60 * 1000;
let spotifyGlobalBlockedUntil = 0;
let loggedSpotifySearchUrl = false;
let loggedSpotifyFinalQuery = false;

const GLOBAL_RATE_LIMIT_FALLBACK_MS = 30 * 1000;
const GLOBAL_RATE_LIMIT_MAX_MS = 60 * 1000;

async function getAccessToken(): Promise<string | null> {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;

  console.log('[Spotify] Checking credentials - clientId present:', !!clientId, ', clientSecret present:', !!clientSecret);

  if (!clientId || !clientSecret) {
    console.log('[Spotify] No credentials - Spotify lookup skipped');
    return null;
  }

  if (accessToken && Date.now() < tokenExpiry) {
    console.log('[Spotify] Using cached token');
    return accessToken;
  }

  console.log('[Spotify] Requesting new access token...');

  try {
    const response = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic ' + Buffer.from(clientId + ':' + clientSecret).toString('base64')
      },
      body: 'grant_type=client_credentials'
    });

    const data = await response.json();
    
    if (data.access_token) {
      accessToken = data.access_token;
      tokenExpiry = Date.now() + (data.expires_in * 1000) - 60000;
      console.log('[Spotify] Access token obtained successfully');
      return accessToken;
    } else {
      console.log('[Spotify] Failed to get access token - no token in response');
    }
  } catch (e) {
    console.error('[Spotify] Failed to get access token:', e);
  }

  return null;
}

// Clean song title by removing extra info like (remastered), (live), etc.
function cleanSongTitle(song: string): string {
  return song
    .replace(/\s*\(.*?\)\s*/g, '')  // Remove (text)
    .replace(/\s*\[.*?\]\s*/g, '')    // Remove [text]
    .replace(/\s*-\s*.*$/g, '')       // Remove - and everything after
    .replace(/\blive\b/gi, '')        // Remove "live"
    .replace(/\bremix\b/gi, '')       // Remove "remix"
    .replace(/\bedit\b/gi, '')        // Remove "edit"
    .trim();
}

function normalizeForMatch(value: string): string {
  return value
    .toLowerCase()
    .replace(/\b(feat\.?|ft\.?|featuring)\b.*$/i, '')
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/&/g, ' and ')
    .replace(/^[\s.,!?;:'"`-]+|[\s.,!?;:'"`-]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeTitleKey(value: string): string {
  return normalizeForMatch(canonicalizeDisplayName(value))
    .replace(/[’']/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function getArtistMatchKeys(value: string): Set<string> {
  const display = canonicalizeDisplayName(value);
  const keys = new Set<string>();

  const normalized = normalizeForMatch(display);
  if (normalized) keys.add(normalized);

  const personKey = buildPersonCanonicalKey(display);
  if (personKey) keys.add(personKey);

  const artistKey = buildArtistCanonicalKey(display);
  if (artistKey) keys.add(artistKey);

  return keys;
}

function validateCandidateMatch(
  artist: string,
  song: string,
  candidate: SpotifyCandidate,
  allowTitleSuffix = false
): { ok: boolean; reason?: string } {
  const requestedArtistKeys = getArtistMatchKeys(artist);
  const candidateArtistKeys = candidate.artists.flatMap((candidateArtist) => Array.from(getArtistMatchKeys(candidateArtist)));
  const artistOk = Array.from(requestedArtistKeys).some((key) => candidateArtistKeys.includes(key));
  if (!artistOk) {
    return { ok: false, reason: 'artist mismatch' };
  }

  const requestedTitle = normalizeTitleKey(song);
  const cleanedRequestedTitle = normalizeTitleKey(cleanSongTitle(song));
  const candidateTitle = normalizeTitleKey(candidate.title);
  const hasAllowedVersionSuffix = (baseTitle: string): boolean => {
    if (!baseTitle || candidateTitle.length <= baseTitle.length) return false;
    if (!candidateTitle.startsWith(`${baseTitle} `)) return false;
    const suffix = candidateTitle.slice(baseTitle.length).trim();
    if (!suffix) return false;
    const compactSuffix = suffix.replace(/^[-\s]+/, '').trim();
    const tokenCount = compactSuffix.split(' ').filter(Boolean).length;
    if (tokenCount === 0 || tokenCount > 8) return false;
    return /\b(remaster(?:ed)?|mix|edit|version|mono|stereo|acoustic|instrumental|karaoke|live|session|demo|deluxe|expanded|anniversary|explicit|clean|single|take)\b/.test(compactSuffix);
  };

  const hasRequestedPrefix = allowTitleSuffix
    && (
      (requestedTitle.length > 0 && (candidateTitle === requestedTitle || candidateTitle.startsWith(`${requestedTitle} `)))
      || (cleanedRequestedTitle.length > 0 && (candidateTitle === cleanedRequestedTitle || candidateTitle.startsWith(`${cleanedRequestedTitle} `)))
    );
  const hasVersionSuffixMatch =
    (requestedTitle.length > 0 && hasAllowedVersionSuffix(requestedTitle))
    || (cleanedRequestedTitle.length > 0 && hasAllowedVersionSuffix(cleanedRequestedTitle));
  const titleOk = candidateTitle === requestedTitle
    || candidateTitle === cleanedRequestedTitle
    || hasRequestedPrefix
    || hasVersionSuffixMatch;
  if (!titleOk) {
    return { ok: false, reason: 'title mismatch' };
  }

  return { ok: true };
}

function getPromptAlbumTokens(promptContext: string): Set<string> {
  const stopWords = new Set([
    'the', 'and', 'for', 'with', 'from', 'that', 'this', 'best', 'songs', 'song', 'tracks', 'track',
    'playlist', 'recordings', 'recording', 'music', 'about', 'into', 'your', 'their', 'its', 'his', 'her',
    'of', 'to', 'in', 'on', 'at', 'by', 'an', 'a', 'is', 'are', 'so', 'called'
  ]);

  const tokens = promptContext
    .toLowerCase()
    .split(/[^a-z0-9]+/g)
    .filter((token) => token.length >= 4 && !stopWords.has(token));

  return new Set(tokens);
}

function isLiveOrVenuePrompt(promptContext: string): boolean {
  const text = promptContext.toLowerCase();
  return /\blive\b|\bvenue\b|\bconcert\b|\bperformance\b|\bshow\b|\btour\b|\bhollywood bowl\b|\bcbgb\b|\bhall\b|\barena\b|\btheatre\b|\btheater\b/.test(text);
}

function normalizeVenueText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[’']/g, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractExplicitVenue(promptContext: string): string | null {
  const text = promptContext.trim();
  if (!text) return null;

  const patterns = [
    /\blive recordings?\s+from\s+([^.!?,;]+)/i,
    /\blive\s+at\s+([^.!?,;]+)/i,
    /\brecordings?\s+from\s+([^.!?,;]+)/i,
    /\bfrom\s+([^.!?,;]+)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const venue = normalizeVenueText(match[1] || '');
    if (!venue) continue;
    if (venue.split(' ').length > 6) continue;
    if (/^(the|best|songs|tracks|recordings|live)$/.test(venue)) continue;
    return venue;
  }

  return null;
}

function getVenueAliases(venue: string): string[] {
  const base = normalizeVenueText(venue);
  if (!base) return [];
  const aliases = new Set<string>([base]);
  if (base.startsWith('the ')) {
    aliases.add(base.slice(4));
  }
  if (base.endsWith('s')) aliases.add(base.slice(0, -1));
  if (!base.endsWith('s')) aliases.add(`${base}s`);
  if (base === 'cbgb') aliases.add('cbgbs');
  if (base === 'cbgbs') aliases.add('cbgb');
  if (base === 'hollywood bowl' || base === 'the hollywood bowl') {
    aliases.add('hollywood bowl');
    aliases.add('the hollywood bowl');
  }
  return Array.from(aliases).filter(Boolean);
}

function scoreSpotifyCandidate(
  artist: string,
  song: string,
  candidate: SpotifyCandidate,
  promptAlbumTokens: Set<string>,
  livePrompt: boolean,
  explicitVenue: string | null
): number {
  let score = 0;
  const requestedArtistKeys = getArtistMatchKeys(artist);
  const requestedTitle = normalizeTitleKey(song);
  const cleanedRequestedTitle = normalizeTitleKey(cleanSongTitle(song));
  const candidateTitle = normalizeTitleKey(candidate.title);
  const candidateArtists = candidate.artists.flatMap((candidateArtist) => Array.from(getArtistMatchKeys(candidateArtist)));
  const albumTitle = normalizeForMatch(candidate.album_title || '');
  const titleAndAlbum = `${candidateTitle} ${albumTitle}`;

  if (Array.from(requestedArtistKeys).some((key) => candidateArtists.includes(key))) score += 3;
  if (candidateTitle === requestedTitle) {
    score += 3;
  } else if (candidateTitle === cleanedRequestedTitle) {
    score += 1;
  }

  if (promptAlbumTokens.size > 0 && albumTitle) {
    for (const token of promptAlbumTokens) {
      if (albumTitle.includes(token)) {
        score += 2;
        break;
      }
    }
  }

  const versionNoiseRegex = /\bdemo\b|\bouttake\b|\balternate\b|\brehearsal\b|\barchives?\b|\bbootleg\b|\bearly\s+version\b/;
  if (versionNoiseRegex.test(titleAndAlbum)) {
    score -= 3;
  }

  const liveRegex = /\blive\b|\bin concert\b/;
  if (liveRegex.test(titleAndAlbum)) {
    score += livePrompt ? 2 : -2;
  }

  if (explicitVenue) {
    const venueAliases = getVenueAliases(explicitVenue);
    const hasVenueMatch = venueAliases.some((alias) => alias.length > 0 && titleAndAlbum.includes(alias));

    if (hasVenueMatch) {
      score += 6;
    } else {
      const otherVenueOrLocationRegex = /\blive\s+(?:at|in)\b|\bacademy\b|\bvillage gate\b|\bmaxs? kansas city\b|\bbudokan\b|\bfillmore\b|\bhollywood bowl\b|\bsan francisco\b|\blondon\b|\btokyo\b|\bparis\b|\bberlin\b|\bnew york\b|\bnyc\b/;
      if (otherVenueOrLocationRegex.test(titleAndAlbum)) {
        score -= 6;
      }
    }

    if (!liveRegex.test(titleAndAlbum)) {
      score -= 3;
    }
  }

  if (!livePrompt) {
    const archiveStyleRegex = /\bdeluxe\b|\bexpanded\b|\banniversary\b|\bremaster(?:ed)?\b/;
    if (archiveStyleRegex.test(titleAndAlbum)) {
      score -= 1;
    }
  }

  return score;
}

function getArtistCacheKey(artist: string, song: string, promptContext = ''): string {
  const explicitVenue = extractExplicitVenue(promptContext) || '';
  const liveFlag = isLiveOrVenuePrompt(promptContext) ? 'live' : 'default';
  const artistCanonical = buildArtistCanonicalKey(artist) || normalizeForMatch(artist);
  const titleCanonical = normalizeTitleKey(cleanSongTitle(song) || song);
  return `${artistCanonical}::${titleCanonical}::${normalizeForMatch(explicitVenue)}::${liveFlag}`;
}

function sanitizeQueryValue(value: string): string {
  return value.replace(/["”“]/g, ' ').replace(/\s+/g, ' ').trim();
}

async function searchSpotify(query: string, token: string, limit = 5): Promise<SpotifySearchResult> {
  const REQUEST_THROTTLE_MS = 300;
  const params = new URLSearchParams({
    q: query,
    type: 'track',
    limit: String(limit),
  });
  const requestUrl = `https://api.spotify.com/v1/search?${params.toString()}`;

  if (!loggedSpotifySearchUrl) {
    console.log(`[Spotify] Search request URL: ${requestUrl}`);
    loggedSpotifySearchUrl = true;
  }

  if (Date.now() < spotifyGlobalBlockedUntil) {
    console.warn('[Spotify] Search skipped due to active global rate limit cooldown');
    return { candidates: [], rateLimitedAbort: true };
  }

  for (let attempt = 0; attempt < 2; attempt += 1) {
    console.log(`[Spotify] Waiting ${REQUEST_THROTTLE_MS} ms before Spotify search`);
    await sleep(REQUEST_THROTTLE_MS);

    const response = await fetch(requestUrl, {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });

    if (!response.ok) {
      const bodyText = await response.text();
      const isRateLimited = response.status === 429 || bodyText.toLowerCase().includes('too many requests');

      if (isRateLimited) {
        const retryAfterHeader = response.headers.get('retry-after');
        const rawRetryAfterSeconds = Number.parseInt(retryAfterHeader || '', 10);
        const rawCooldownMs = Number.isFinite(rawRetryAfterSeconds) && rawRetryAfterSeconds > 0
          ? rawRetryAfterSeconds * 1000
          : GLOBAL_RATE_LIMIT_FALLBACK_MS;
        const globalCooldownMs = Math.min(GLOBAL_RATE_LIMIT_MAX_MS, Math.max(GLOBAL_RATE_LIMIT_FALLBACK_MS, rawCooldownMs));
        spotifyGlobalBlockedUntil = Date.now() + globalCooldownMs;
        console.warn(`[Spotify] Rate limited, raw Retry-After: ${retryAfterHeader ?? '(missing)'}`);
        console.warn(`[Spotify] Rate limited, effective global cooldown: ${Math.round(globalCooldownMs / 1000)} seconds`);

        if (attempt === 0) {
          const retryAfterSeconds = Number.parseInt(retryAfterHeader || '', 10);
          const safeRetryAfter = Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : 1;
          const waitSeconds = Math.min(5, Math.max(1, safeRetryAfter));
          console.warn(`[Spotify] Rate limited, retrying after ${waitSeconds} seconds`);
          await sleep(waitSeconds * 1000);
          continue;
        }

        console.warn('[Spotify] Rate limit retry failed');
        console.warn('[Spotify] Search aborted due to rate limit');
        return { candidates: [], rateLimitedAbort: true };
      }

      console.warn(`[Spotify] Search failed with status ${response.status}`);
      return { candidates: [], rateLimitedAbort: false };
    }

    let data: any;
    try {
      data = await response.json();
    } catch {
      console.warn('[Spotify] Search returned invalid JSON');
      return { candidates: [], rateLimitedAbort: false };
    }

    if (!data.tracks?.items?.length) return { candidates: [], rateLimitedAbort: false };

    return {
      candidates: data.tracks.items.map((track: any) => ({
        spotify_id: typeof track.id === 'string' ? track.id : null,
        spotify_uri: typeof track.uri === 'string' ? track.uri : null,
        spotify_url: track.external_urls?.spotify || null,
        album_image_url: track.album?.images?.[0]?.url || null,
        title: typeof track.name === 'string' ? track.name : '',
        album_title: typeof track.album?.name === 'string' ? track.album.name : '',
        popularity: typeof track.popularity === 'number' && Number.isFinite(track.popularity) ? track.popularity : 0,
        duration_ms: typeof track.duration_ms === 'number' && Number.isFinite(track.duration_ms) ? track.duration_ms : null,
        release_year: (() => {
          const releaseDate = typeof track.album?.release_date === 'string' ? track.album.release_date : '';
          const yearMatch = releaseDate.match(/^(\d{4})/);
          if (!yearMatch) return null;
          const year = Number.parseInt(yearMatch[1], 10);
          return Number.isFinite(year) ? year : null;
        })(),
        artists: Array.isArray(track.artists)
          ? track.artists
              .map((artistItem: any) => (typeof artistItem?.name === 'string' ? artistItem.name : ''))
              .filter((name: string) => name.length > 0)
          : [],
      })),
      rateLimitedAbort: false,
    };
  }

  return { candidates: [], rateLimitedAbort: false };
}

async function searchTrackInternal(artist: string, song: string, promptContext = ''): Promise<SpotifyTrackDebugInfo> {
  console.log(`[Spotify] Searching for: "${song}" by "${artist}"`);
  
  const token = await getAccessToken();
  
  if (!token) {
    console.log('[Spotify] No token - skipping search');
    return { spotify_url: null, album_image_url: null, release_year: null, score: null, matchedTitle: null, matchedAlbumTitle: null };
  }

  const explicitVenue = extractExplicitVenue(promptContext);
  const artistCacheKey = getArtistCacheKey(artist, song, promptContext);
  if (!artistCacheKey) {
    return { spotify_url: null, album_image_url: null, release_year: null, score: null, matchedTitle: null, matchedAlbumTitle: null };
  }

  let cacheEntry = artistSearchCache.get(artistCacheKey);
  const isExpired = !cacheEntry || Date.now() - cacheEntry.fetchedAt > ARTIST_CACHE_TTL_MS;

  if (isExpired) {
    const safeSong = sanitizeQueryValue(cleanSongTitle(song) || song);
    const safeArtist = sanitizeQueryValue(artist);
    const safeVenue = explicitVenue ? sanitizeQueryValue(explicitVenue) : '';
    const structuredQuery = safeVenue
      ? `track:"${safeSong}" artist:"${safeArtist}" ${safeVenue}`
      : `track:"${safeSong}" artist:"${safeArtist}"`;
    const fallbackQuery = safeVenue
      ? `${safeSong} ${safeArtist} ${safeVenue}`.trim()
      : `${safeSong} ${safeArtist}`.trim();
    const titleOnlyStructuredQuery = `track:"${safeSong}"`;
    const titleOnlyFallbackQuery = safeSong;

    if (!loggedSpotifyFinalQuery) {
      console.log(`[Spotify] Final query (primary): ${structuredQuery}`);
      console.log(`[Spotify] Final query (fallback): ${fallbackQuery}`);
      loggedSpotifyFinalQuery = true;
    }

    try {
      let result = await searchSpotify(structuredQuery, token, 10);
      if (result.candidates.length === 0 && !result.rateLimitedAbort) {
        result = await searchSpotify(fallbackQuery, token, 10);
      }
      if (result.candidates.length === 0 && !result.rateLimitedAbort && explicitVenue) {
        const baseStructuredQuery = `track:"${safeSong}" artist:"${safeArtist}"`;
        const baseFallbackQuery = `${safeSong} ${safeArtist}`.trim();
        result = await searchSpotify(baseStructuredQuery, token, 10);
        if (result.candidates.length === 0 && !result.rateLimitedAbort) {
          result = await searchSpotify(baseFallbackQuery, token, 10);
        }
      }
      if (result.candidates.length === 0 && !result.rateLimitedAbort) {
        result = await searchSpotify(titleOnlyStructuredQuery, token, 10);
      }
      if (result.candidates.length === 0 && !result.rateLimitedAbort) {
        result = await searchSpotify(titleOnlyFallbackQuery, token, 10);
      }
      cacheEntry = {
        candidates: result.candidates,
        rateLimitedAbort: result.rateLimitedAbort,
        fetchedAt: Date.now(),
      };
      artistSearchCache.set(artistCacheKey, cacheEntry);
    } catch (e) {
      console.error('[Spotify] Artist search error:', e);
      return { spotify_url: null, album_image_url: null, release_year: null, score: null, matchedTitle: null, matchedAlbumTitle: null };
    }
  }

  if (!cacheEntry || cacheEntry.rateLimitedAbort) {
    console.warn('[Spotify] Skipping match due to prior rate limit for this artist');
    return { spotify_url: null, album_image_url: null, release_year: null, score: null, matchedTitle: null, matchedAlbumTitle: null };
  }

  const promptAlbumTokens = getPromptAlbumTokens(promptContext);
  const livePrompt = isLiveOrVenuePrompt(promptContext);
  const allowTitleSuffix = Boolean(explicitVenue);
  let bestCandidate: SpotifyCandidate | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;

  for (const candidate of cacheEntry.candidates) {
    const validation = validateCandidateMatch(artist, song, candidate, allowTitleSuffix);
    if (!validation.ok) continue;

    const score = scoreSpotifyCandidate(artist, song, candidate, promptAlbumTokens, livePrompt, explicitVenue);
    if (score > bestScore) {
      bestScore = score;
      bestCandidate = candidate;
    }
  }

  if (bestCandidate) {
    console.log(`[Spotify] Found match: "${song}" by "${artist}" (score=${bestScore})`);
    return {
      spotify_url: bestCandidate.spotify_url,
      album_image_url: bestCandidate.album_image_url,
      release_year: bestCandidate.release_year,
      score: bestScore,
      matchedTitle: bestCandidate.title || null,
      matchedAlbumTitle: bestCandidate.album_title || null,
    };
  }

  console.log('[Spotify] No match found');
  return { spotify_url: null, album_image_url: null, release_year: null, score: null, matchedTitle: null, matchedAlbumTitle: null };
}

export async function searchTrack(artist: string, song: string, promptContext = ''): Promise<SpotifyTrackInfo> {
  const result = await searchTrackInternal(artist, song, promptContext);
  return {
    spotify_url: result.spotify_url,
    album_image_url: result.album_image_url,
    release_year: result.release_year,
  };
}

export async function searchTrackWithDiagnostics(
  artist: string,
  song: string,
  promptContext = ''
): Promise<SpotifyTrackDebugInfo> {
  return searchTrackInternal(artist, song, promptContext);
}

export async function searchTrackByIsrc(isrc: string): Promise<SpotifyTrackInfo> {
  const normalized = String(isrc || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (normalized.length < 12) {
    return { spotify_url: null, album_image_url: null, release_year: null };
  }

  const token = await getAccessToken();
  if (!token) {
    return { spotify_url: null, album_image_url: null, release_year: null };
  }

  try {
    const result = await searchSpotify(`isrc:${normalized}`, token, 10);
    if (result.candidates.length === 0) {
      return { spotify_url: null, album_image_url: null, release_year: null };
    }

    const best = result.candidates
      .slice()
      .sort((a, b) => {
        if (b.popularity !== a.popularity) return b.popularity - a.popularity;
        const aYear = typeof a.release_year === 'number' ? a.release_year : 0;
        const bYear = typeof b.release_year === 'number' ? b.release_year : 0;
        return bYear - aYear;
      })[0];

    return {
      spotify_url: best?.spotify_url || null,
      album_image_url: best?.album_image_url || null,
      release_year: best?.release_year ?? null,
    };
  } catch {
    return { spotify_url: null, album_image_url: null, release_year: null };
  }
}
