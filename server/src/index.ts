import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { randomUUID } from 'crypto';
import fs from 'fs';
import path from 'path';
import { detectCreditPromptForEval, generatePlaylist } from './services/gemini.js';
import { searchTrackByIsrc, searchTrackCandidates, searchTrackCandidatesByIsrc, searchTrackWithDiagnostics } from './services/spotify.js';
import { canonicalizeEquipmentName, getAllPlaylists, getPlaylistById, getPlaylistsByTag, getRelatedPlaylists, getTopTags, getPlaylistsByPlace, getPlaylistsByScene, getArtistAtlas, getCountryAtlas, getCityAtlas, getStudioAtlas, getVenueAtlas, getEquipmentAtlas, getConnectionPath, getCreditAtlas, getDuplicateTagCandidates, getTagStats, getRecordingDurationMs, getRecordingIsrc, getRecordingSpotifyUrl, isGenericEquipmentName, isValidAtlasNodeType, mergeTagExact, searchAtlasNodeSuggestions, setRecordingDurationMs, setRecordingIsrc, setRecordingSpotifyUrl, updatePlaylistTrackSpotifyUrls } from './services/db.js';
import { backfillCreditFromMusicBrainz } from './services/evidence-backfill.js';
import { resolveMusicBrainzRecordingMetadata } from './services/musicbrainz.js';
import { backfillTruthCreditsFromDiscogs } from './services/truth-credit-layer.js';

const app = express();
const PORT = Number(process.env.PORT || 3001);

app.use(cors());
app.use(express.json());

interface PromptRequest {
  prompt: string;
}

function isTransientPlaylistError(error: unknown): boolean {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error || '').toLowerCase();
  return message.includes('503')
    || message.includes('service unavailable')
    || message.includes('high demand')
    || message.includes('temporarily unavailable')
    || message.includes('overload')
    || message.includes('fetch failed')
    || message.includes('request failed')
    || message.includes('timed out')
    || message.includes('timeout');
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

const DEFAULT_PLAYLIST_TIMEOUT_MS = 45000;

function getPlaylistTimeoutMs(): number {
  const parsed = Number(process.env.PLAYLIST_TIMEOUT_MS || DEFAULT_PLAYLIST_TIMEOUT_MS);
  if (!Number.isFinite(parsed) || parsed < 10000) return DEFAULT_PLAYLIST_TIMEOUT_MS;
  return Math.floor(parsed);
}

async function generatePlaylistWithTimeout(prompt: string, attempt: number): Promise<Awaited<ReturnType<typeof generatePlaylist>>> {
  const timeoutMs = getPlaylistTimeoutMs();
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      generatePlaylist(prompt),
      new Promise<never>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`Playlist generation timed out after ${timeoutMs}ms (attempt ${attempt}/3)`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

interface MergeTagRequest {
  source: string;
  target: string;
}

interface CreditEvidenceBackfillRequest {
  name: string;
  role: string;
  prompt?: string;
  query?: string;
  limit?: number;
}

function parseOrigin(value: string | undefined): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    return new URL(raw).origin;
  } catch {
    return '';
  }
}

function canUseFrontendOrigin(candidateOrigin: string, configuredFrontendUrl: string): boolean {
  if (!candidateOrigin) return false;
  const configuredOrigin = parseOrigin(configuredFrontendUrl);
  if (configuredOrigin && candidateOrigin === configuredOrigin) return true;

  try {
    const url = new URL(candidateOrigin);
    return url.protocol === 'http:' && (url.hostname === 'localhost' || url.hostname === '127.0.0.1');
  } catch {
    return false;
  }
}

function buildSpotifyState(stateNonce: string, returnTo: string, returnOrigin: string): string {
  const payload = Buffer.from(JSON.stringify({
    n: stateNonce,
    r: returnTo,
    o: returnOrigin,
  })).toString('base64url');
  return `${stateNonce}.${payload}`;
}

function parseSpotifyState(stateRaw: string): { nonce: string; returnTo: string; returnOrigin: string } | null {
  const value = String(stateRaw || '').trim();
  if (!value) return null;
  const dot = value.indexOf('.');
  if (dot <= 0) return null;
  const nonce = value.slice(0, dot).trim();
  const payload = value.slice(dot + 1).trim();
  if (!nonce || !payload) return null;
  try {
    const decoded = JSON.parse(Buffer.from(payload, 'base64url').toString('utf-8')) as {
      n?: unknown;
      r?: unknown;
      o?: unknown;
    };
    const payloadNonce = typeof decoded.n === 'string' ? decoded.n.trim() : '';
    const returnTo = typeof decoded.r === 'string' ? decoded.r.trim() : '';
    const returnOrigin = typeof decoded.o === 'string' ? decoded.o.trim() : '';
    if (!payloadNonce || payloadNonce !== nonce) return null;
    return {
      nonce,
      returnTo: returnTo.startsWith('/') ? returnTo : '/',
      returnOrigin,
    };
  } catch {
    return null;
  }
}

function readArtifactJson<T>(fileName: string): T | null {
  try {
    const filePath = path.resolve(process.cwd(), 'eval-artifacts', fileName);
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function parseStringArray(raw: string | null | undefined): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed)
      ? parsed.filter((item): item is string => typeof item === 'string' && item.length > 0)
      : [];
  } catch {
    return [];
  }
}

const ALLOWED_CREDIT_ROLES = new Set([
  'producer',
  'cover_designer',
  'photographer',
  'art_director',
  'design_studio',
  'engineer',
  'arranger',
  'session_musician',
]);

const ALLOWED_EQUIPMENT_CATEGORIES = new Set([
  'instrument',
  'microphone',
  'synthesizer',
  'drum_machine',
  'effect',
  'amplifier',
  'console',
  'tape_machine',
  'sampler',
  'other',
]);

function parseCredits(raw: string | null | undefined): Array<{ name: string; role: string }> {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (item) => item
          && typeof item === 'object'
          && typeof item.name === 'string'
          && item.name.trim().length > 0
          && typeof item.role === 'string'
          && ALLOWED_CREDIT_ROLES.has(item.role)
      )
      .slice(0, 5)
      .map((item) => ({ name: item.name.trim(), role: item.role }));
  } catch {
    return [];
  }
}

function parseEquipment(raw: string | null | undefined): Array<{ name: string; category: string }> {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (item) => item
          && typeof item === 'object'
          && typeof item.name === 'string'
          && item.name.trim().length > 0
          && typeof item.category === 'string'
          && ALLOWED_EQUIPMENT_CATEGORIES.has(item.category)
      )
      .slice(0, 8)
      .map((item) => ({ name: canonicalizeEquipmentName(item.name), category: item.category }))
      .filter((item) => item.name.length > 0 && !isGenericEquipmentName(item.name));
  } catch {
    return [];
  }
}

function getLegacyPlaceEntries(placesRaw: string | null | undefined, placeRaw: string | null | undefined): string[] {
  const fromArray = parseStringArray(placesRaw);
  if (fromArray.length > 0) return fromArray;
  if (placeRaw && placeRaw.trim().length > 0) return [placeRaw.trim()];
  return [];
}

function getLegacyLocationFallback(placesRaw: string | null | undefined, placeRaw: string | null | undefined): {
  countries: string[];
  cities: string[];
  studios: string[];
  venues: string[];
} {
  const countries = new Set<string>();
  const cities = new Set<string>();
  const studios = new Set<string>();
  const venues = new Set<string>();

  const entries = getLegacyPlaceEntries(placesRaw, placeRaw);
  const studioRegex = /\b(studio|studios|recorders|recording)\b/i;
  const venueRegex = /\b(club|hall|theatre|theater|arena|venue|cbgb)\b/i;
  const countryNames = new Set([
    'usa', 'united states', 'united kingdom', 'uk', 'england', 'france', 'germany', 'italy', 'spain',
    'sweden', 'norway', 'denmark', 'finland', 'japan', 'canada', 'australia', 'brazil', 'mexico', 'ireland'
  ]);

  for (const rawEntry of entries) {
    const entry = rawEntry.trim();
    if (!entry) continue;

    const parts = entry.split(',').map((part) => part.trim()).filter(Boolean);
    const head = parts[0] || '';
    const tail = parts.length > 1 ? parts[parts.length - 1] : '';
    const tailLower = tail.toLowerCase();

    if (studioRegex.test(head) || studioRegex.test(entry)) {
      if (head) studios.add(head);
      if (tail && countryNames.has(tailLower)) countries.add(tail);
      if (tail && !countryNames.has(tailLower)) cities.add(tail);
      continue;
    }

    if (venueRegex.test(head) || venueRegex.test(entry)) {
      if (head) venues.add(head);
      if (tail && countryNames.has(tailLower)) countries.add(tail);
      if (tail && !countryNames.has(tailLower)) cities.add(tail);
      continue;
    }

    if (parts.length === 2 && countryNames.has(tailLower)) {
      if (head) cities.add(head);
      countries.add(tail);
      continue;
    }

    if (countryNames.has(head.toLowerCase())) {
      countries.add(head);
    }
  }

  return {
    countries: Array.from(countries),
    cities: Array.from(cities),
    studios: Array.from(studios),
    venues: Array.from(venues)
  };
}

function parseCookies(cookieHeader: string | undefined): Record<string, string> {
  if (!cookieHeader) return {};

  const cookies: Record<string, string> = {};
  for (const part of cookieHeader.split(';')) {
    const [key, ...rest] = part.trim().split('=');
    if (!key) continue;
    cookies[key] = decodeURIComponent(rest.join('='));
  }
  return cookies;
}

function buildCookie(name: string, value: string, maxAgeSeconds: number): string {
  return `${name}=${encodeURIComponent(value)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSeconds}`;
}

function clearCookie(name: string): string {
  return `${name}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`;
}

function spotifyUrlToUri(url: string): string | null {
  const match = url.match(/spotify\.com\/track\/([A-Za-z0-9]+)/);
  if (!match) return null;
  return `spotify:track:${match[1]}`;
}

function getSpotifyConfig(): { clientId: string; clientSecret: string; redirectUri: string; frontendUrl: string } | null {
  const clientId = process.env.SPOTIFY_CLIENT_ID;
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
  const redirectUri = process.env.SPOTIFY_REDIRECT_URI;
  const frontendUrl = process.env.FRONTEND_URL;

  if (!clientId || !clientSecret || !redirectUri || !frontendUrl) {
    return null;
  }

  return { clientId, clientSecret, redirectUri, frontendUrl };
}

app.post('/api/playlist', async (req, res) => {
  const { prompt } = req.body as PromptRequest;

  if (!prompt) {
    res.status(400).json({ error: 'Prompt is required' });
    return;
  }

  if (!process.env.GEMINI_API_KEY) {
    console.error('GEMINI_API_KEY is not set!');
    res.status(500).json({ error: 'GEMINI_API_KEY not configured' });
    return;
  }

  console.log('API key is configured:', process.env.GEMINI_API_KEY.substring(0, 10) + '...');

  try {
    console.log('Generating playlist for:', prompt);
    let result: Awaited<ReturnType<typeof generatePlaylist>>;
    try {
      result = await generatePlaylistWithTimeout(prompt, 1);
    } catch (firstError) {
      if (!isTransientPlaylistError(firstError)) throw firstError;
      console.log('[api] transient playlist generation error, retrying (attempt 2/3)');
      await sleep(1200);
      try {
        result = await generatePlaylistWithTimeout(prompt, 2);
      } catch (secondError) {
        if (!isTransientPlaylistError(secondError)) throw secondError;
        console.log('[api] transient playlist generation error, retrying (attempt 3/3)');
        await sleep(2500);
        result = await generatePlaylistWithTimeout(prompt, 3);
      }
    }
    console.log('Playlist generated, cached:', result.cached);
    res.json({ ...result.playlist, cached: result.cached, verification: result.verification, truth: result.truth });
  } catch (error) {
    console.error('Gemini error:', error);
    
    const message = error instanceof Error ? error.message : 'Failed to generate playlist';
    const isEvidenceConstraintError =
      typeof message === 'string'
      && (
        message.includes('Not enough verified credit evidence')
        || message.includes('strongly verified tracks were found for this credit constraint')
        || message.includes('No verified')
      );
    const isTimeoutError = typeof message === 'string' && message.toLowerCase().includes('timed out');
    const transient = isTransientPlaylistError(error);
    res.status(isEvidenceConstraintError ? 422 : isTimeoutError ? 504 : transient ? 503 : 500).json({ error: message });
  }
});

app.get('/api/quality/status', (_req, res) => {
  const qualityStatus = readArtifactJson<Record<string, unknown>>('quality-status.json');
  const reasonQuality = readArtifactJson<Record<string, unknown>>('reason-quality.json');

  if (!qualityStatus) {
    res.status(404).json({ error: 'quality-status artifact not found' });
    return;
  }

  res.json({
    qualityStatus,
    reasonQuality,
  });
});

app.post('/api/evidence/backfill-credit', async (req, res) => {
  const { name, role, prompt, limit } = req.body as CreditEvidenceBackfillRequest;

  let resolvedName = typeof name === 'string' ? name.trim() : '';
  let resolvedRole = typeof role === 'string' ? role.trim() : '';

  if ((!resolvedName || !resolvedRole) && typeof prompt === 'string' && prompt.trim()) {
    const detected = detectCreditPromptForEval(prompt);
    if (detected) {
      resolvedName = resolvedName || detected.name;
      resolvedRole = resolvedRole || detected.role;
    }
  }

  if (!resolvedName || !resolvedRole) {
    res.status(400).json({ error: 'name and role are required (or provide a credit prompt to infer them)' });
    return;
  }

  try {
    const result = await backfillCreditFromMusicBrainz({
      name: resolvedName,
      role: resolvedRole,
      limit: typeof limit === 'number' ? limit : undefined,
    });

    res.json({
      success: true,
      ...result,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to backfill credit evidence';
    res.status(422).json({ error: message });
  }
});

app.post('/api/evidence/backfill-credit-truth', async (req, res) => {
  const { name, role, prompt, query, limit } = req.body as CreditEvidenceBackfillRequest;

  let resolvedName = typeof name === 'string' ? name.trim() : '';
  let resolvedRole = typeof role === 'string' ? role.trim() : '';
  let resolvedQuery = typeof query === 'string' ? query.trim() : '';

  if ((!resolvedName || !resolvedRole) && typeof prompt === 'string' && prompt.trim()) {
    const detected = detectCreditPromptForEval(prompt);
    if (detected) {
      resolvedName = resolvedName || detected.name;
      resolvedRole = resolvedRole || detected.role;
      resolvedQuery = resolvedQuery || prompt.trim();
    }
  }

  if (!resolvedName || !resolvedRole) {
    res.status(400).json({ error: 'name and role are required (or provide a credit prompt to infer them)' });
    return;
  }

  if (!resolvedQuery) {
    resolvedQuery = `${resolvedName} ${resolvedRole}`;
  }

  try {
    const result = await backfillTruthCreditsFromDiscogs({
      creditName: resolvedName,
      creditRole: resolvedRole,
      query: resolvedQuery,
      limit: typeof limit === 'number' ? limit : undefined,
    });

    res.json({ success: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to backfill truth credit evidence';
    res.status(422).json({ error: message });
  }
});

app.get('/api/spotify/login', (req, res) => {
  const config = getSpotifyConfig();
  if (!config) {
    res.status(500).json({ error: 'Spotify auth is not configured' });
    return;
  }

  const state = randomUUID();
  const returnToRaw = String(req.query.returnTo || '/');
  const returnTo = returnToRaw.startsWith('/') ? returnToRaw : '/';
  const returnOriginQuery = parseOrigin(typeof req.query.returnOrigin === 'string' ? req.query.returnOrigin : '');
  const requestOrigin = parseOrigin(typeof req.headers.origin === 'string' ? req.headers.origin : '');
  const refererOrigin = parseOrigin(typeof req.headers.referer === 'string' ? req.headers.referer : '');
  const returnOrigin = [returnOriginQuery, requestOrigin, refererOrigin]
    .find((origin) => canUseFrontendOrigin(origin || '', config.frontendUrl)) || parseOrigin(config.frontendUrl);

  const spotifyScope = 'playlist-modify-private playlist-modify-public';
  console.log('[spotify-auth] login scopes:', spotifyScope);

  const oauthState = buildSpotifyState(state, returnTo, returnOrigin);

  const params = new URLSearchParams({
    response_type: 'code',
    client_id: config.clientId,
    scope: spotifyScope,
    redirect_uri: config.redirectUri,
    state: oauthState,
  });

  res.setHeader('Set-Cookie', [
    buildCookie('spotify_oauth_state', oauthState, 600),
    buildCookie('spotify_return_to', returnTo, 600),
    buildCookie('spotify_return_origin', returnOrigin, 600),
  ]);

  res.redirect(`https://accounts.spotify.com/authorize?${params.toString()}`);
});

app.get('/api/spotify/callback', async (req, res) => {
  const config = getSpotifyConfig();
  if (!config) {
    res.status(500).send('Spotify auth is not configured');
    return;
  }

  const cookies = parseCookies(req.headers.cookie);
  const stateFromCookie = cookies.spotify_oauth_state;
  const stateFromQuery = String(req.query.state || '');
  const stateFromQueryPayload = parseSpotifyState(stateFromQuery);
  const code = String(req.query.code || '');
  const returnTo = cookies.spotify_return_to || stateFromQueryPayload?.returnTo || '/';
  const cookieOrigin = parseOrigin(cookies.spotify_return_origin || stateFromQueryPayload?.returnOrigin || '');
  const configuredFrontendOrigin = parseOrigin(config.frontendUrl);
  const frontendOrigin = configuredFrontendOrigin || (canUseFrontendOrigin(cookieOrigin, config.frontendUrl) ? cookieOrigin : '');
  const redirectWithSpotifyStatus = (status: 'connected' | 'auth_failed', reason?: string) => {
    const params = new URLSearchParams({ spotify: status });
    if (reason && reason.trim().length > 0) params.set('spotify_reason', reason.trim());
    res.redirect(`${frontendOrigin}${returnTo}?${params.toString()}`);
  };

  const spotifyAuthError = typeof req.query.error === 'string' ? req.query.error.trim() : '';

  if (!code) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to'), clearCookie('spotify_return_origin')]);
    redirectWithSpotifyStatus('auth_failed', spotifyAuthError ? `spotify_error:${spotifyAuthError}` : 'missing_code');
    return;
  }

  if (stateFromCookie && stateFromCookie !== stateFromQuery) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to'), clearCookie('spotify_return_origin')]);
    redirectWithSpotifyStatus('auth_failed', 'state_mismatch');
    return;
  }

  if (!stateFromCookie && !stateFromQueryPayload) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to'), clearCookie('spotify_return_origin')]);
    redirectWithSpotifyStatus('auth_failed', 'missing_state_cookie');
    return;
  }

  try {
    const tokenResponse = await fetch('https://accounts.spotify.com/api/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`,
      },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: config.redirectUri,
      }),
    });

    const tokenData = await tokenResponse.json() as { access_token?: string; expires_in?: number; scope?: string; error?: string };
    console.log('[spotify-auth] callback token scope:', tokenData.scope || '(none)');
    console.log('[spotify-auth] token source: authorization_code flow');
    if (!tokenResponse.ok || !tokenData.access_token || !tokenData.expires_in) {
      res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to'), clearCookie('spotify_return_origin')]);
      const reason = tokenData.error ? `token_exchange_failed:${tokenData.error}` : 'token_exchange_failed';
      redirectWithSpotifyStatus('auth_failed', reason);
      return;
    }

    const expiresAt = Date.now() + tokenData.expires_in * 1000;
    res.setHeader('Set-Cookie', [
      buildCookie('spotify_access_token', tokenData.access_token, tokenData.expires_in),
      buildCookie('spotify_token_expires_at', String(expiresAt), tokenData.expires_in),
      buildCookie('spotify_token_scope', tokenData.scope || '', tokenData.expires_in),
      clearCookie('spotify_oauth_state'),
      clearCookie('spotify_return_to'),
      clearCookie('spotify_return_origin'),
    ]);

    redirectWithSpotifyStatus('connected');
  } catch {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to'), clearCookie('spotify_return_origin')]);
    redirectWithSpotifyStatus('auth_failed', 'callback_exception');
  }
});

app.post('/api/spotify/save-playlist/:id', async (req, res) => {
  console.log('[spotify-save] entering save-playlist endpoint');

  const config = getSpotifyConfig();
  if (!config) {
    res.status(500).json({ error: 'Spotify auth is not configured' });
    return;
  }

  const cookies = parseCookies(req.headers.cookie);
  const accessToken = cookies.spotify_access_token;
  const expiresAt = Number(cookies.spotify_token_expires_at || '0');
  const tokenScope = cookies.spotify_token_scope || '';
  console.log('[spotify-save] token cookie exists:', !!accessToken);
  console.log('[spotify-save] token expired:', !expiresAt || Date.now() >= expiresAt);
  console.log('[spotify-save] granted token scope:', tokenScope || '(none)');

  if (!accessToken || !expiresAt || Date.now() >= expiresAt) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_access_token'), clearCookie('spotify_token_expires_at')]);
    res.status(401).json({ error: 'Spotify login required or expired token' });
    return;
  }

  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) {
    res.status(400).json({ error: 'Invalid playlist id' });
    return;
  }

  const playlist = getPlaylistById(id);
  if (!playlist) {
    res.status(404).json({ error: 'Playlist not found' });
    return;
  }

  let tracks: Array<{ song?: string; artist?: string; spotify_url?: string }> = [];
  try {
    const parsed = JSON.parse(playlist.tracks);
    if (Array.isArray(parsed)) tracks = parsed;
  } catch {
    tracks = [];
  }

  const trackQueries = tracks.filter(
    (track): track is { song: string; artist: string; spotify_url?: string } => typeof track.song === 'string' && track.song.trim().length > 0 && typeof track.artist === 'string' && track.artist.trim().length > 0
  );

  const uris: string[] = [];
  const usedUris = new Set<string>();
  const playlistTrackSpotifyUpdates: Array<{ artist: string; song: string; spotifyUrl: string }> = [];
  let matchedTrackCount = 0;
  const skippedTracks: Array<{ artist: string; song: string; reason: string }> = [];
  let reusedExistingSpotifyUrls = 0;
  let reusedRecordingSpotifyUrls = 0;
  let matchedViaIsrc = 0;
  let discoveredIsrcCount = 0;
  let searchedSpotifyMatches = 0;
  let uncertainSearchMatches = 0;
  for (const track of trackQueries) {
    let matchedCurrentTrack = false;
    let skipReason = 'no_match_found';
    const existingUri = typeof track.spotify_url === 'string' ? spotifyUrlToUri(track.spotify_url) : null;
    if (existingUri && usedUris.has(existingUri)) {
      skipReason = 'existing_uri_already_used';
    }
    if (existingUri && !usedUris.has(existingUri)) {
      uris.push(existingUri);
      usedUris.add(existingUri);
      reusedExistingSpotifyUrls += 1;
      if (typeof track.spotify_url === 'string' && track.spotify_url.trim().length > 0) {
        playlistTrackSpotifyUpdates.push({ artist: track.artist, song: track.song, spotifyUrl: track.spotify_url.trim() });
      }
      matchedCurrentTrack = true;
      matchedTrackCount += 1;
      continue;
    }

    const storedSpotifyUrl = getRecordingSpotifyUrl(track.artist, track.song);
    const storedUri = storedSpotifyUrl ? spotifyUrlToUri(storedSpotifyUrl) : null;
    if (storedUri && usedUris.has(storedUri)) {
      skipReason = 'cached_uri_already_used';
    }
    if (storedUri && !usedUris.has(storedUri)) {
      uris.push(storedUri);
      usedUris.add(storedUri);
      reusedRecordingSpotifyUrls += 1;
      if (storedSpotifyUrl) {
        playlistTrackSpotifyUpdates.push({ artist: track.artist, song: track.song, spotifyUrl: storedSpotifyUrl });
      }
      matchedCurrentTrack = true;
      matchedTrackCount += 1;
      continue;
    }

    let recordingIsrc = getRecordingIsrc(track.artist, track.song);
    let recordingDurationMs = getRecordingDurationMs(track.artist, track.song);
    if (!recordingIsrc || !recordingDurationMs) {
      try {
        const metadata = await resolveMusicBrainzRecordingMetadata(track.artist, track.song);
        if (metadata.isrc && !recordingIsrc) {
          recordingIsrc = metadata.isrc;
          setRecordingIsrc(track.artist, track.song, metadata.isrc);
          discoveredIsrcCount += 1;
        }
        if (typeof metadata.durationMs === 'number' && Number.isFinite(metadata.durationMs) && metadata.durationMs > 0 && !recordingDurationMs) {
          recordingDurationMs = Math.max(0, Math.floor(metadata.durationMs));
          setRecordingDurationMs(track.artist, track.song, recordingDurationMs);
        }
      } catch (error) {
        console.log('[spotify-save] mb isrc lookup skipped:', { artist: track.artist, song: track.song, error: error instanceof Error ? error.message : String(error) });
      }
    }

    if (recordingIsrc) {
      try {
        const spotifyIsrcCandidates = await searchTrackCandidatesByIsrc(recordingIsrc, 6);
        let isrcCollisionCount = 0;
        for (const candidate of spotifyIsrcCandidates) {
          if (!candidate.spotify_url) continue;
          const uri = spotifyUrlToUri(candidate.spotify_url);
          if (!uri) continue;
          if (usedUris.has(uri)) {
            isrcCollisionCount += 1;
            continue;
          }
          uris.push(uri);
          usedUris.add(uri);
          matchedViaIsrc += 1;
          setRecordingSpotifyUrl(track.artist, track.song, candidate.spotify_url);
          playlistTrackSpotifyUpdates.push({ artist: track.artist, song: track.song, spotifyUrl: candidate.spotify_url });
          if (typeof candidate.duration_ms === 'number' && Number.isFinite(candidate.duration_ms) && candidate.duration_ms > 0) {
            setRecordingDurationMs(track.artist, track.song, candidate.duration_ms);
          }
          matchedCurrentTrack = true;
          matchedTrackCount += 1;
          break;
        }

        if (!matchedCurrentTrack && spotifyIsrcCandidates.length > 0 && isrcCollisionCount > 0) {
          skipReason = isrcCollisionCount >= spotifyIsrcCandidates.length
            ? 'isrc_uri_collision'
            : 'isrc_candidate_mismatch';
        }

        if (matchedCurrentTrack) continue;

        const spotifyByIsrc = await searchTrackByIsrc(recordingIsrc);
        if (spotifyByIsrc.spotify_url) {
          const uri = spotifyUrlToUri(spotifyByIsrc.spotify_url);
          if (uri && !usedUris.has(uri)) {
            uris.push(uri);
            usedUris.add(uri);
            matchedViaIsrc += 1;
            setRecordingSpotifyUrl(track.artist, track.song, spotifyByIsrc.spotify_url);
            playlistTrackSpotifyUpdates.push({ artist: track.artist, song: track.song, spotifyUrl: spotifyByIsrc.spotify_url });
            if (typeof spotifyByIsrc.duration_ms === 'number' && Number.isFinite(spotifyByIsrc.duration_ms) && spotifyByIsrc.duration_ms > 0) {
              setRecordingDurationMs(track.artist, track.song, spotifyByIsrc.duration_ms);
            }
            matchedCurrentTrack = true;
            matchedTrackCount += 1;
            continue;
          }
          if (uri && usedUris.has(uri)) {
            skipReason = 'isrc_primary_uri_collision';
          }
        }
      } catch (error) {
        console.log('[spotify-save] isrc spotify lookup skipped:', { artist: track.artist, song: track.song, isrc: recordingIsrc, error: error instanceof Error ? error.message : String(error) });
        skipReason = 'isrc_lookup_error';
      }
    }

    try {
      const candidates = await searchTrackCandidates(track.artist, track.song, playlist.prompt, 6, recordingDurationMs);
      let candidateUriCollisionCount = 0;
      let selectedUrl: string | null = null;
      let selectedUri: string | null = null;
      let selectedDurationMs: number | null = null;
      let selectedScore: number | null = null;

      for (const candidate of candidates) {
        const candidateUrl = candidate.spotify_url;
        if (!candidateUrl) continue;
        const candidateUri = spotifyUrlToUri(candidateUrl);
        if (!candidateUri) continue;
        if (usedUris.has(candidateUri)) {
          candidateUriCollisionCount += 1;
          continue;
        }
        selectedUrl = candidateUrl;
        selectedUri = candidateUri;
        selectedDurationMs = typeof candidate.duration_ms === 'number' && Number.isFinite(candidate.duration_ms)
          ? candidate.duration_ms
          : null;
        selectedScore = typeof candidate.score === 'number' && Number.isFinite(candidate.score)
          ? candidate.score
          : null;
        break;
      }

      if (!selectedUrl) {
        if (candidates.length === 0) {
          skipReason = 'search_no_candidates';
        } else if (candidateUriCollisionCount >= candidates.length) {
          skipReason = 'search_uri_collision';
        }
      }

      if (!selectedUrl || !selectedUri) {
        const spotifyInfo = await searchTrackWithDiagnostics(track.artist, track.song, playlist.prompt, recordingDurationMs);
        if (spotifyInfo.spotify_url) {
          const fallbackUri = spotifyUrlToUri(spotifyInfo.spotify_url);
          if (fallbackUri && !usedUris.has(fallbackUri)) {
            selectedUrl = spotifyInfo.spotify_url;
            selectedUri = fallbackUri;
            selectedDurationMs = typeof spotifyInfo.duration_ms === 'number' && Number.isFinite(spotifyInfo.duration_ms)
              ? spotifyInfo.duration_ms
              : null;
            selectedScore = typeof spotifyInfo.score === 'number' && Number.isFinite(spotifyInfo.score)
              ? spotifyInfo.score
              : null;
          } else if (fallbackUri && usedUris.has(fallbackUri)) {
            skipReason = 'fallback_uri_collision';
          }
        } else {
          skipReason = 'fallback_no_match';
        }
      }

      if (selectedUrl && selectedUri) {
        uris.push(selectedUri);
        usedUris.add(selectedUri);
        searchedSpotifyMatches += 1;
        if (typeof selectedScore === 'number' && selectedScore <= 0) {
          uncertainSearchMatches += 1;
        }
        setRecordingSpotifyUrl(track.artist, track.song, selectedUrl);
        playlistTrackSpotifyUpdates.push({ artist: track.artist, song: track.song, spotifyUrl: selectedUrl });
        if (typeof selectedDurationMs === 'number' && Number.isFinite(selectedDurationMs) && selectedDurationMs > 0) {
          setRecordingDurationMs(track.artist, track.song, selectedDurationMs);
        }
        matchedCurrentTrack = true;
        matchedTrackCount += 1;
      }
    } catch (error) {
      console.error('[spotify-save] track match failed:', { artist: track.artist, song: track.song, error });
      skipReason = 'search_error';
    }

    if (!matchedCurrentTrack) {
      skippedTracks.push({ artist: track.artist, song: track.song, reason: skipReason });
    }
  }

  const dedupedUris = Array.from(new Set(uris));

  const matched = matchedTrackCount;
  const skipped = trackQueries.length - matched;
  const addedTracks = dedupedUris.length;
  const duplicateUriMatches = Math.max(0, matched - addedTracks);

  console.log('[spotify-save] matched tracks count:', matched);
  console.log('[spotify-save] added tracks count:', addedTracks);
  console.log('[spotify-save] skipped tracks count:', skipped);
  console.log('[spotify-save] duplicate-uri matches:', duplicateUriMatches);
  console.log('[spotify-save] reused existing spotify urls:', reusedExistingSpotifyUrls);
  console.log('[spotify-save] reused recording spotify urls:', reusedRecordingSpotifyUrls);
  console.log('[spotify-save] matched via isrc:', matchedViaIsrc);
  console.log('[spotify-save] discovered isrc count:', discoveredIsrcCount);
  console.log('[spotify-save] searched spotify matches:', searchedSpotifyMatches);
  console.log('[spotify-save] uncertain search matches:', uncertainSearchMatches);
  console.log('[spotify-save] first track uris:', dedupedUris.slice(0, 5));

  if (matched === 0) {
    res.status(400).json({ error: 'No Spotify-matched tracks found in this playlist', matched, skipped });
    return;
  }

  try {
    const meResponse = await fetch('https://api.spotify.com/v1/me', {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    console.log('[spotify-save] /me status:', meResponse.status);

    if (meResponse.status === 401) {
      res.setHeader('Set-Cookie', [clearCookie('spotify_access_token'), clearCookie('spotify_token_expires_at')]);
      res.status(401).json({ error: 'Spotify token expired. Please login again.' });
      return;
    }

    const meData = await meResponse.json() as { id?: string; display_name?: string; email?: string };
    console.log('[spotify-save] /me body:', {
      id: meData.id,
      display_name: meData.display_name,
      email: meData.email,
    });
    if (!meResponse.ok || !meData.id) {
      console.error('[spotify-save] /me failed status:', meResponse.status);
      console.error('[spotify-save] /me failed body:', meData);
      res.status(500).json({ error: 'Failed to fetch Spotify user profile' });
      return;
    }

    const createPlaylistUrl = 'https://api.spotify.com/v1/me/playlists';
    const createPlaylistBody = {
      name: playlist.title,
      description: playlist.description,
      public: false,
    };

    console.log('[spotify-save] create-playlist user id:', meData.id);
    console.log('[spotify-save] create-playlist url:', createPlaylistUrl);
    console.log('[spotify-save] create-playlist body:', createPlaylistBody);

    const createResponse = await fetch(createPlaylistUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(createPlaylistBody),
    });

    const created = await createResponse.json() as { id?: string; external_urls?: { spotify?: string } };
    if (!createResponse.ok || !created.id) {
      console.error('[spotify-save] create-playlist failed status:', createResponse.status);
      console.error('[spotify-save] create-playlist failed body:', created);
      res.status(500).json({ error: 'Failed to create Spotify playlist' });
      return;
    }

    const addTracksUrl = `https://api.spotify.com/v1/playlists/${created.id}/items`;
    const addTracksBody = { uris: dedupedUris };
    console.log('[spotify-save] created playlist id:', created.id);
    console.log('[spotify-save] add-tracks url:', addTracksUrl);
    console.log('[spotify-save] add-tracks body:', addTracksBody);

    const addTracksResponse = await fetch(addTracksUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(addTracksBody),
    });

    if (!addTracksResponse.ok) {
      let addTracksBody: unknown = null;
      try {
        addTracksBody = await addTracksResponse.json();
      } catch {
        addTracksBody = null;
      }
      console.error('[spotify-save] add-tracks failed status:', addTracksResponse.status);
      console.error('[spotify-save] add-tracks failed body:', addTracksBody);
      res.status(500).json({ error: 'Playlist created, but failed to add tracks' });
      return;
    }

    const persistedTrackSpotifyUrls = updatePlaylistTrackSpotifyUrls(id, playlistTrackSpotifyUpdates);
    console.log('[spotify-save] persisted spotify urls on playlist tracks:', persistedTrackSpotifyUrls);

    res.json({
      success: true,
      spotifyPlaylistUrl: created.external_urls?.spotify || null,
      addedTracks,
      matched,
      skipped,
      duplicateUriMatches,
      uncertainMatches: uncertainSearchMatches,
      skippedTracks: skippedTracks.slice(0, 20),
      matchSources: {
        trackSpotifyUrl: reusedExistingSpotifyUrls,
        recordingSpotifyUrl: reusedRecordingSpotifyUrls,
        isrc: matchedViaIsrc,
        search: searchedSpotifyMatches,
      },
    });
  } catch (error) {
    console.error('[spotify-save] caught error:', error);
    if (error instanceof Error) {
      console.error('[spotify-save] caught error message:', error.message);
      console.error('[spotify-save] caught error stack:', error.stack);
    }
    res.status(500).json({ error: 'Failed to save playlist to Spotify' });
  }
});

app.get('/api/playlists', (req, res) => {
  const playlists = getAllPlaylists();
  res.json(playlists);
});

app.get('/api/playlists/:id', (req, res) => {
  const id = parseInt(req.params.id);
  const playlist = getPlaylistById(id);
  
  if (!playlist) {
    res.status(404).json({ error: 'Playlist not found' });
    return;
  }
  
  res.json({
    ...playlist,
    tracks: JSON.parse(playlist.tracks),
    tags: parseStringArray(playlist.tags),
    places: getLegacyPlaceEntries(playlist.places, playlist.place),
    countries: (() => {
      const fromNew = parseStringArray(playlist.countries);
      if (fromNew.length > 0) return fromNew;
      return getLegacyLocationFallback(playlist.places, playlist.place).countries;
    })(),
    cities: (() => {
      const fromNew = parseStringArray(playlist.cities);
      if (fromNew.length > 0) return fromNew;
      return getLegacyLocationFallback(playlist.places, playlist.place).cities;
    })(),
    studios: (() => {
      const fromNew = parseStringArray(playlist.studios);
      if (fromNew.length > 0) return fromNew;
      return getLegacyLocationFallback(playlist.places, playlist.place).studios;
    })(),
    venues: (() => {
      const fromNew = parseStringArray(playlist.venues);
      if (fromNew.length > 0) return fromNew;
      return getLegacyLocationFallback(playlist.places, playlist.place).venues;
    })(),
    scenes: (() => {
      const fromNew = parseStringArray(playlist.scenes);
      if (fromNew.length > 0) return fromNew;
      return playlist.scene ? [playlist.scene] : [];
    })(),
    influences: (() => {
      if (!playlist.influences) return [];
      try {
        const parsed = JSON.parse(playlist.influences);
        if (!Array.isArray(parsed)) return [];
        return parsed
          .filter((item) => item && typeof item === 'object' && typeof item.from === 'string' && typeof item.to === 'string')
          .map((item) => ({ from: item.from, to: item.to }));
      } catch {
        return [];
      }
    })(),
    credits: parseCredits(playlist.credits),
    equipment: parseEquipment(playlist.equipment)
  });
});

app.get('/api/tags/:tag', (req, res) => {
  const tag = req.params.tag;
  const playlists = getPlaylistsByTag(tag);
  res.json(playlists);
});

app.get('/api/places/:place', (req, res) => {
  const place = req.params.place;
  const playlists = getPlaylistsByPlace(place);
  res.json(playlists);
});

app.get('/api/scenes/:scene', (req, res) => {
  const scene = req.params.scene;
  const playlists = getPlaylistsByScene(scene);
  res.json(playlists);
});

app.get('/api/countries/:country', (req, res) => {
  const country = req.params.country;
  const data = getCountryAtlas(country);
  res.json(data);
});

app.get('/api/cities/:city', (req, res) => {
  const city = req.params.city;
  const data = getCityAtlas(city);
  res.json(data);
});

app.get('/api/studios/:studio', (req, res) => {
  const studio = req.params.studio;
  const data = getStudioAtlas(studio);
  res.json(data);
});

app.get('/api/venues/:venue', (req, res) => {
  const venue = req.params.venue;
  const data = getVenueAtlas(venue);
  res.json(data);
});

app.get('/api/equipment/:name', (req, res) => {
  const name = req.params.name;
  if (!name || !name.trim()) {
    res.status(400).json({ error: 'Equipment name is required' });
    return;
  }

  const data = getEquipmentAtlas(name);
  res.json(data);
});

app.get('/api/artists/:artist', (req, res) => {
  const artist = req.params.artist;
  const data = getArtistAtlas(artist);
  res.json(data);
});

app.get('/api/credits/:name', (req, res) => {
  const name = req.params.name;
  if (!name || !name.trim()) {
    res.status(400).json({ error: 'Credit name is required' });
    return;
  }

  const data = getCreditAtlas(name);
  res.json(data);
});

app.get('/api/tags', (req, res) => {
  const tags = getTopTags(12);
  res.json(tags);
});

app.get('/api/tags/duplicates', (req, res) => {
  const duplicates = getDuplicateTagCandidates();
  res.json(duplicates);
});

app.get('/api/tags/stats', (req, res) => {
  const stats = getTagStats();
  res.json(stats);
});

app.post('/api/tags/merge', (req, res) => {
  const { source, target } = req.body as MergeTagRequest;

  if (typeof source !== 'string' || typeof target !== 'string') {
    res.status(400).json({ error: 'source and target must be strings' });
    return;
  }

  const result = mergeTagExact(source, target);
  res.json(result);
});

app.get('/api/playlists/:id/related', (req, res) => {
  const id = parseInt(req.params.id);
  if (Number.isNaN(id)) {
    res.status(400).json({ error: 'Invalid playlist id' });
    return;
  }

  const playlists = getRelatedPlaylists(id);
  res.json(playlists);
});

app.get('/api/connect', (req, res) => {
  const fromType = String(req.query.fromType || '');
  const fromValue = String(req.query.fromValue || '').trim();
  const toType = String(req.query.toType || '');
  const toValue = String(req.query.toValue || '').trim();

  if (!isValidAtlasNodeType(fromType) || !isValidAtlasNodeType(toType)) {
    res.status(400).json({ error: 'Invalid node type' });
    return;
  }

  if (!fromValue || !toValue) {
    res.status(400).json({ error: 'fromValue and toValue are required' });
    return;
  }

  const result = getConnectionPath(fromType, fromValue, toType, toValue, 3);
  res.json(result);
});

app.get('/api/connect/suggest', (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) {
    res.json([]);
    return;
  }

  const suggestions = searchAtlasNodeSuggestions(q, 20);
  res.json(suggestions);
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
