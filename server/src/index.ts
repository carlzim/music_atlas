import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { randomUUID } from 'crypto';
import fs from 'fs';
import path from 'path';
import { detectCreditPromptForEval, generatePlaylist } from './services/gemini.js';
import { searchTrack } from './services/spotify.js';
import { canonicalizeEquipmentName, getAllPlaylists, getPlaylistById, getPlaylistsByTag, getRelatedPlaylists, getTopTags, getPlaylistsByPlace, getPlaylistsByScene, getArtistAtlas, getCountryAtlas, getCityAtlas, getStudioAtlas, getVenueAtlas, getEquipmentAtlas, getConnectionPath, getCreditAtlas, getDuplicateTagCandidates, getTagStats, isGenericEquipmentName, isValidAtlasNodeType, mergeTagExact, searchAtlasNodeSuggestions } from './services/db.js';
import { backfillCreditFromMusicBrainz } from './services/evidence-backfill.js';
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

  const spotifyScope = 'playlist-modify-private playlist-modify-public';
  console.log('[spotify-auth] login scopes:', spotifyScope);

  const params = new URLSearchParams({
    response_type: 'code',
    client_id: config.clientId,
    scope: spotifyScope,
    redirect_uri: config.redirectUri,
    state,
  });

  res.setHeader('Set-Cookie', [
    buildCookie('spotify_oauth_state', state, 600),
    buildCookie('spotify_return_to', returnTo, 600),
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
  const code = String(req.query.code || '');
  const returnTo = cookies.spotify_return_to || '/';
  const redirectWithSpotifyStatus = (status: 'connected' | 'auth_failed', reason?: string) => {
    const params = new URLSearchParams({ spotify: status });
    if (reason && reason.trim().length > 0) params.set('spotify_reason', reason.trim());
    res.redirect(`${config.frontendUrl}${returnTo}?${params.toString()}`);
  };

  if (!code) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to')]);
    redirectWithSpotifyStatus('auth_failed', 'missing_code');
    return;
  }

  if (!stateFromCookie) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to')]);
    redirectWithSpotifyStatus('auth_failed', 'missing_state_cookie');
    return;
  }

  if (stateFromCookie !== stateFromQuery) {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to')]);
    redirectWithSpotifyStatus('auth_failed', 'state_mismatch');
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
      res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to')]);
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
    ]);

    redirectWithSpotifyStatus('connected');
  } catch {
    res.setHeader('Set-Cookie', [clearCookie('spotify_oauth_state'), clearCookie('spotify_return_to')]);
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

  let tracks: Array<{ song?: string; artist?: string }> = [];
  try {
    const parsed = JSON.parse(playlist.tracks);
    if (Array.isArray(parsed)) tracks = parsed;
  } catch {
    tracks = [];
  }

  const trackQueries = tracks.filter(
    (track): track is { song: string; artist: string } => typeof track.song === 'string' && track.song.trim().length > 0 && typeof track.artist === 'string' && track.artist.trim().length > 0
  );

  const uris: string[] = [];
  for (const track of trackQueries) {
    try {
      const spotifyInfo = await searchTrack(track.artist, track.song, playlist.prompt);
      if (!spotifyInfo.spotify_url) continue;
      const uri = spotifyUrlToUri(spotifyInfo.spotify_url);
      if (uri) uris.push(uri);
    } catch (error) {
      console.error('[spotify-save] track match failed:', { artist: track.artist, song: track.song, error });
    }
  }

  const dedupedUris = Array.from(new Set(uris));

  const matched = dedupedUris.length;
  const skipped = trackQueries.length - matched;

  console.log('[spotify-save] matched tracks count:', matched);
  console.log('[spotify-save] skipped tracks count:', skipped);
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

    res.json({
      success: true,
      spotifyPlaylistUrl: created.external_urls?.spotify || null,
      addedTracks: matched,
      matched,
      skipped,
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
