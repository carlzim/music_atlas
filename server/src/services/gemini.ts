import { GoogleGenerativeAI } from '@google/generative-ai';
import { canonicalizeEquipmentName, getAtlasEntityCatalog, getAssociatedArtistsByNode, getBandMembers, getCreditAtlas, getGraphNeighbors, getKnownTags, getPlaylistByCacheKey, getTracksByRecordingCreditEvidence, hasRecordingCreditEvidence, hasRecordingStudioEvidence, isGenericEquipmentName, normalizePromptForCache, saveArtistMembershipEvidence, savePlaylist } from './db.js';
import { backfillCreditFromMusicBrainz, SUPPORTED_MUSICBRAINZ_CREDIT_ROLES } from './evidence-backfill.js';
import { buildArtistCanonicalKey, buildCreditCanonicalKey, buildPersonCanonicalKey, buildStudioCanonicalKey, canonicalizeDisplayName } from './normalize.js';
import { searchTrackWithDiagnostics } from './spotify.js';
import { syncTruthMembershipForBandName } from './truth-layer.js';
import { backfillTruthCreditsFromDiscogs, getTruthCreditCandidates, SUPPORTED_TRUTH_CREDIT_ROLES } from './truth-credit-layer.js';
import { resolvePromptRoute } from './prompt-routing.js';
import { recordRoutingBackfill, recordRoutingCall, recordRoutingFallback, recordRoutingSuccess } from './routing-observability.js';

interface Track {
  artist: string;
  song: string;
  reason: string;
  featured_artists?: string[];
  artist_display?: string;
  spotify_url?: string | null;
  album_image_url?: string | null;
  release_year?: number | null;
}

type PlaylistCurationMode = 'essential' | 'balanced' | 'deep_cuts';

interface InfluenceEdge {
  from: string;
  to: string;
}

interface CreditEntity {
  name: string;
  role: string;
}

interface EquipmentEntity {
  name: string;
  category: string;
}

interface MembershipEntity {
  band: string;
  person: string;
  role: string;
}

interface Playlist {
  title: string;
  description: string;
  tracks: Track[];
  tags?: string[];
  countries?: string[];
  cities?: string[];
  studios?: string[];
  venues?: string[];
  scenes?: string[];
  influences?: InfluenceEdge[];
  credits?: CreditEntity[];
  equipment?: EquipmentEntity[];
}

interface VerificationDetails {
  evidence_before: number;
  evidence_after: number;
  used_auto_backfill: boolean;
  backfill_inserted: number;
  backfill_skipped_reason?: string;
}

interface TruthDetails {
  membership_sync?: {
    band: string;
    attempted: boolean;
    imported: number;
    skipped_reason?: string;
  };
  credit_sync?: {
    name: string;
    role: string;
    source: 'discogs';
    attempted: boolean;
    imported: number;
    skipped_reason?: string;
  };
  curation?: {
    mode: PlaylistCurationMode;
    inferred_from_prompt: boolean;
    top_score_sample?: Array<{
      artist: string;
      song: string;
      final_score: number;
      relevance_to_query: number;
      prominence_score: number;
      artist_canonical_score: number;
      entity_signature_score: number;
      diversity_adjustment: number;
    }>;
    ranking_floor?: {
      applied: boolean;
      dropped_tracks: number;
      floor_score: number;
    };
    ranking_window?: {
      applied: boolean;
      input_tracks: number;
      kept_tracks: number;
      unique_artists: number;
      max_tracks_per_artist: number;
    };
    composition?: {
      selected_tracks: number;
      selected_track_target: number;
      selected_track_target_met: boolean;
      selected_track_gap: number;
      selected_track_coverage: number;
      selection_retention_gap: number;
      selection_retention_coverage: number;
      target_miss_count: number;
      target_miss_reasons: string[];
      unique_artists: number;
      unique_artist_target: number;
      unique_artist_target_met: boolean;
      unique_artist_target_gap: number;
      unique_artist_target_coverage: number;
      unique_decades: number;
      unique_decade_target: number;
      unique_decade_target_met: boolean;
      unique_decade_target_gap: number;
      unique_decade_target_coverage: number;
      max_tracks_per_artist: number;
    };
  };
}

export interface PlaylistResponse {
  playlist: Playlist;
  cached: boolean;
  verification?: VerificationDetails;
  truth?: TruthDetails;
}

const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash';

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');

const GEMINI_RETRY_DELAYS_MS = [0, 1000, 3000];
const MAX_PLAYLIST_TRACKS = 25;
const AUTO_BACKFILL_COOLDOWN_MS = Math.max(
  0,
  Number.isFinite(Number(process.env.MUSICBRAINZ_AUTO_BACKFILL_COOLDOWN_MS || '600000'))
    ? Number(process.env.MUSICBRAINZ_AUTO_BACKFILL_COOLDOWN_MS || '600000')
    : 600000
);
const lastAutoBackfillByCredit = new Map<string, number>();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableGeminiError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const message = error.message.toLowerCase();
  return message.includes('503')
    || message.includes('service unavailable')
    || message.includes('high demand')
    || message.includes('temporarily unavailable')
    || message.includes('overload');
}

async function callGeminiWithRetry<T>(operation: () => Promise<T>): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt < GEMINI_RETRY_DELAYS_MS.length; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;

      if (!isRetryableGeminiError(error)) {
        throw error;
      }

      if (attempt === GEMINI_RETRY_DELAYS_MS.length - 1) {
        break;
      }

      const waitMs = GEMINI_RETRY_DELAYS_MS[attempt + 1];
      console.warn(`[Gemini] Temporary service overload (attempt ${attempt + 1}/${GEMINI_RETRY_DELAYS_MS.length}), retrying in ${waitMs / 1000}s`);
      await sleep(waitMs);
    }
  }

  if (isRetryableGeminiError(lastError)) {
    throw new Error('Gemini is temporarily busy, please try again');
  }

  throw lastError instanceof Error ? lastError : new Error('Gemini request failed');
}

// Translate user prompt to English for consistent storage and display
async function translateToEnglish(text: string): Promise<string> {
  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

  const result = await callGeminiWithRetry(() => model.generateContent(
    `Translate this to English. Only respond with the translated text, nothing else: "${text}"`
  ));

  return result.response.text().trim();
}

// Generate 3-6 short tags for the playlist
async function generateTags(prompt: string): Promise<string[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Generate 3 to 6 short tags for a playlist about "${prompt}". 
       Tags should be lowercase, use hyphens for compound words.
       Examples: studio-history, berlin, post-punk, session-musicians, 60s-rock, folk-pop.
       Return ONLY a JSON array of strings, nothing else.`
    ));
    
    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const tags = JSON.parse(cleaned);
    
    if (Array.isArray(tags)) {
      return tags.slice(0, 6); // Max 6 tags
    }
  } catch (e) {
    console.error('[Gemini] Tag generation failed:', e);
  }
  
  return []; // Return empty array if generation fails
}

async function generateLocationMetadata(prompt: string): Promise<{
  countries: string[];
  cities: string[];
  studios: string[];
  venues: string[];
}> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Generate location metadata for this playlist idea: "${prompt}".

Return ONLY valid JSON in this exact format:
{
  "countries": [],
  "cities": [],
  "studios": [],
  "venues": []
}

Rules:
- countries: 0 to 2 actual country names only
- cities: 0 to 3 actual city names only
- studios: 0 to 3 actual recording studios only (not design studios, labels, collectives, or people)
- venues: 0 to 3 actual live performance venues / concert halls / clubs only
- cities, studios, and venues should only be returned when clearly central to the playlist idea
- avoid weak, broad, or incidental location associations
- never combine studio and city in one string
- never combine venue and city in one string
- never force-fit an entity into a category
- if uncertain, return an empty array for that category
- do NOT put labels, design firms, photographers, producer collectives, people, or similar entities into these fields
- if uncertain, return an empty array
- do not guess`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();

    const parseLocationObject = (input: string): unknown => {
      try {
        return JSON.parse(input);
      } catch {
        const start = input.indexOf('{');
        if (start >= 0) {
          let depth = 0;
          let end = -1;
          for (let i = start; i < input.length; i += 1) {
            const ch = input[i];
            if (ch === '{') depth += 1;
            if (ch === '}') {
              depth -= 1;
              if (depth === 0) {
                end = i;
                break;
              }
            }
          }

          if (end > start) {
            const jsonBlock = input.slice(start, end + 1);
            return JSON.parse(jsonBlock);
          }
        }

        throw new Error('No valid JSON object block found');
      }
    };

    let parsed: unknown;
    try {
      parsed = parseLocationObject(cleaned);
    } catch {
      const normalized = cleaned
        .replace(/[“”]/g, '"')
        .replace(/[‘’]/g, "'");

      try {
        parsed = parseLocationObject(normalized);
      } catch {
        const repaired = await repairLocationMetadataJsonResponse(cleaned);
        if (!repaired) throw new Error('Location metadata JSON repair failed');
        parsed = parseLocationObject(repaired.replace(/```json|```/g, '').trim());
      }
    }

    if (parsed && typeof parsed === 'object') {
      const parsedObj = parsed as Record<string, unknown>;
      const countries = Array.isArray(parsedObj.countries)
        ? parsedObj.countries.filter((item: unknown): item is string => typeof item === 'string' && item.length > 0).slice(0, 2)
        : [];
      const cities = Array.isArray(parsedObj.cities)
        ? parsedObj.cities.filter((item: unknown): item is string => typeof item === 'string' && item.length > 0).slice(0, 3)
        : [];
      const studios = Array.isArray(parsedObj.studios)
        ? parsedObj.studios.filter((item: unknown): item is string => typeof item === 'string' && item.length > 0).slice(0, 3)
        : [];
      const venues = Array.isArray(parsedObj.venues)
        ? parsedObj.venues.filter((item: unknown): item is string => typeof item === 'string' && item.length > 0).slice(0, 3)
        : [];

      return sanitizeLocationMetadata({ countries, cities, studios, venues });
    }
  } catch (e) {
    console.error('[Gemini] Location metadata generation failed:', e);
  }

  return { countries: [], cities: [], studios: [], venues: [] };
}

async function repairLocationMetadataJsonResponse(rawResponse: string): Promise<string | null> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Convert this near-JSON to valid strict JSON in exactly this shape:
{
  "countries": [],
  "cities": [],
  "studios": [],
  "venues": []
}

Rules:
- Fix only JSON syntax/quoting/escaping/array comma issues.
- Keep values as-is as much as possible.
- Return ONLY valid JSON.

Input:
${rawResponse}`
    ));

    return result.response.text().trim();
  } catch {
    return null;
  }
}

async function generateScenes(prompt: string): Promise<string[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Generate 1 to 3 music scenes for this playlist idea: "${prompt}".
       Keep each scene as a short English string (2-4 words), for example: "Berlin experimental" or "Seattle grunge".
       Scenes must be actual music scenes or movements only.
       Do not force-fit entities into scenes.
       Do NOT return people, labels, design firms, photographers, producer collectives, or companies as scenes.
       If uncertain, return an empty array.
       Return ONLY a JSON array of strings, nothing else.`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const scenes = JSON.parse(cleaned);

    if (Array.isArray(scenes)) {
      return sanitizeScenes(
        scenes
        .filter((item): item is string => typeof item === 'string' && item.length > 0)
        .slice(0, 3)
      );
    }
  } catch (e) {
    console.error('[Gemini] Scenes generation failed:', e);
  }

  return [];
}

async function bootstrapScenesFromTracks(prompt: string, tracks: Track[], candidateScenes: string[]): Promise<string[]> {
  if (!Array.isArray(candidateScenes) || candidateScenes.length === 0) return [];
  if (!Array.isArray(tracks) || tracks.length === 0) return [];

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `From the candidate scenes below, keep only scenes that are strongly supported by this track list.
If uncertain, exclude the scene.
Do not invent new scenes.

Prompt: "${prompt}"
Tracks: ${JSON.stringify(tracks.map((t) => ({ artist: t.artist, song: t.song })))}
Candidate scenes: ${JSON.stringify(candidateScenes)}

Return ONLY valid JSON array of strings using only candidate scenes:
["scene"]`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    if (!Array.isArray(parsed)) return [];

    const allowed = new Set(candidateScenes.map((scene) => scene.trim().toLowerCase()).filter((scene) => scene.length > 0));
    const output: string[] = [];
    const dedup = new Set<string>();

    for (const item of parsed) {
      if (typeof item !== 'string') continue;
      const scene = item.trim();
      if (!scene) continue;
      const key = scene.toLowerCase();
      if (!allowed.has(key)) continue;
      if (dedup.has(key)) continue;
      dedup.add(key);
      output.push(scene);
      if (output.length >= 3) break;
    }

    return sanitizeScenes(output);
  } catch {
    return [];
  }
}

async function generateInfluences(prompt: string, tracks: Track[]): Promise<InfluenceEdge[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Based on this playlist prompt and candidate tracks, return up to 5 historically well-known influence relationships.
If uncertain, return an empty array.
Do not invent claims.

Prompt: "${prompt}"
Tracks: ${JSON.stringify(tracks.map((t) => ({ artist: t.artist, song: t.song })))}

Return ONLY valid JSON in this format:
[{"from":"", "to":""}]`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(cleaned);

    if (!Array.isArray(parsed)) return [];

    const dedup = new Set<string>();
    const output: InfluenceEdge[] = [];

    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;
      const from = typeof item.from === 'string' ? item.from.trim() : '';
      const to = typeof item.to === 'string' ? item.to.trim() : '';
      if (!from || !to) continue;
      if (from.toLowerCase() === to.toLowerCase()) continue;

      const key = `${from.toLowerCase()}->${to.toLowerCase()}`;
      if (dedup.has(key)) continue;
      dedup.add(key);
      output.push({ from, to });
      if (output.length >= 5) break;
    }

    return output;
  } catch (e) {
    console.error('[Gemini] Influence generation failed:', e);
    return [];
  }
}

function extractPlaceEntityFromPrompt(prompt: string): string | null {
  const patterns = [
    /\brecorded at the\s+([^.!?,;]+)/i,
    /\brecorded at\s+([^.!?,;]+)/i,
    /\brecorded in the\s+([^.!?,;]+)/i,
    /\brecorded in\s+([^.!?,;]+)/i,
    /\bdone at the\s+([^.!?,;]+)/i,
    /\bdone at\s+([^.!?,;]+)/i,
    /\bdone in the\s+([^.!?,;]+)/i,
    /\bdone in\s+([^.!?,;]+)/i,
    /\btracked at the\s+([^.!?,;]+)/i,
    /\btracked at\s+([^.!?,;]+)/i,
    /\btracked in the\s+([^.!?,;]+)/i,
    /\btracked in\s+([^.!?,;]+)/i,
    /\bcut at the\s+([^.!?,;]+)/i,
    /\bcut at\s+([^.!?,;]+)/i,
    /\bcut in the\s+([^.!?,;]+)/i,
    /\bcut in\s+([^.!?,;]+)/i,
    /\bmade at the\s+([^.!?,;]+)/i,
    /\bmade at\s+([^.!?,;]+)/i,
    /\bmade in the\s+([^.!?,;]+)/i,
    /\bmade in\s+([^.!?,;]+)/i,
    /\b([^.!?,;]+\bstudios?\b)['’]s\s+(?:[a-z]+\s+){0,3}(?:recordings?|songs?|tracks?)\b/i,
    /\b(?:recordings?|songs?|tracks?)\s+from\s+([^.!?,;]+\bstudios?\b)['’]s\b/i,
    /\b(?:recordings?|songs?|tracks?)\s+made\s+(?:in|at)\s+the\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+made\s+(?:in|at)\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+at\s+the\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+at\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+in\s+the\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+in\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+from\s+the\s+([^.!?,;]+)/i,
    /\b(?:recordings?|songs?|tracks?)\s+from\s+([^.!?,;]+)/i,
    /\b(?:recording|studio)\s+history\s+(?:of|at|from)\s+(?:the\s+)?([^.!?,;]+)/i,
    /\b(?:the\s+)?story\s+(?:of|from|behind)\s+(?:the\s+)?([^.!?,;]+\bstudios?\b[^.!?,;]*)/i,
    /\b(?:timeline|evolution|origins?)\s+of\s+(?:the\s+)?([^.!?,;]+\bstudios?\b[^.!?,;]*)/i,
    /\bhistory\s+of\s+([^.!?,;]+\bstudios?\b[^.!?,;]*)/i,
    /\bproduced by\s+.+?\s+at the\s+([^.!?,;]+)/i,
    /\bproduced by\s+.+?\s+at\s+([^.!?,;]+)/i,
    /\bengineered by\s+.+?\s+at the\s+([^.!?,;]+)/i,
    /\bengineered by\s+.+?\s+at\s+([^.!?,;]+)/i,
    /\barranged by\s+.+?\s+at the\s+([^.!?,;]+)/i,
    /\barranged by\s+.+?\s+at\s+([^.!?,;]+)/i,
  ];
  const stopAfter = /\b(?:engineered|produced|arranged|photographed|photography|art direction|cover design|artwork|mixed|mastered|for|with|by|using|featuring|feat\.?|including)\b/i;

  for (const pattern of patterns) {
    const match = prompt.match(pattern);
    if (!match) continue;

    const raw = (match[1] || '').trim();
    if (!raw) continue;

    const cleaned = (raw.split(stopAfter)[0] || '')
      .trim()
      .replace(/\s+in\s+(?:nyc|new york city|new york|los angeles|la|london|berlin|paris|tokyo)\s*$/i, '')
      .replace(/\s+using\s+.+$/i, '')
      .replace(/[.!?,;:]+$/g, '')
      .replace(/\s+/g, ' ');

    const normalizedArticle = cleaned.replace(
      /^the\s+(?=[^,]*\b(?:studio|studios|club|hall|theatre|theater|arena|venue)\b)/i,
      ''
    );

    if (normalizedArticle) return normalizedArticle;
  }

  return null;
}

export function extractPlaceEntityFromPromptForEval(prompt: string): string | null {
  return extractPlaceEntityFromPrompt(prompt);
}

function isValidPlaceEntityName(name: string): boolean {
  const trimmed = name.trim();
  if (!trimmed) return false;
  if (trimmed.length < 2 || trimmed.length > 80) return false;
  if (!/[a-z]/i.test(trimmed)) return false;
  if (/^[^a-z0-9]+$/i.test(trimmed)) return false;
  if (/^(album|albums|record|records|recording|recordings|song|songs|track|tracks)$/i.test(trimmed)) return false;
  return true;
}

async function classifyPlaceEntity(name: string): Promise<'studio' | 'venue' | 'unknown'> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Classify this music place entity as exactly one of: studio, venue, unknown.
Entity: "${name}"
Rules:
- studio = recording studio
- venue = live performance venue/club/hall/theatre/arena
- unknown = if uncertain
- do not guess
Return only one word: studio OR venue OR unknown.`
    ));

    const answer = result.response.text().trim().toLowerCase();
    if (answer === 'studio' || answer === 'venue' || answer === 'unknown') {
      return answer;
    }
    return 'unknown';
  } catch {
    return 'unknown';
  }
}

function inferPlaceTypeHeuristic(name: string): 'studio' | 'venue' | 'unknown' {
  const normalized = name.trim().toLowerCase();
  if (!normalized) return 'unknown';

  if (/\bstudios?\b/.test(normalized)) return 'studio';
  if (/\b(?:club|hall|theatre|theater|arena|ballroom|bowl|stadium|venue)\b/.test(normalized)) return 'venue';

  return 'unknown';
}

const ALLOWED_CREDIT_ROLES = new Set([
  'producer',
  'cover_designer',
  'photographer',
  'art_director',
  'design_studio',
  'engineer',
  'arranger',
]);

const CORE_CREDIT_TRUTH_ROLES = new Set(['producer', 'engineer', 'arranger']);

function getCreditRoleSearchTerm(role: string): string {
  const normalized = role.trim().toLowerCase();
  if (normalized === 'cover_designer') return 'cover design';
  if (normalized === 'art_director') return 'art direction';
  if (normalized === 'design_studio') return 'design';
  if (normalized === 'session_musician') return 'session musician';
  return normalized.replace(/_/g, ' ');
}

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

function isLikelyNonEquipmentEntityName(name: string): boolean {
  const normalized = name.trim().toLowerCase();
  if (!normalized) return true;

  if (/\bstudios?\b|\bvenues?\b|\bnyc\b|\bnew york\b|\blos angeles\b|\blondon\b|\bberlin\b|\bparis\b|\btokyo\b/.test(normalized)) {
    return true;
  }

  if (/\busing\b|\btheir\b|\bfrom\b/.test(normalized) && normalized.split(/\s+/).length >= 4) {
    return true;
  }

  return false;
}

async function generateCredits(prompt: string, tracks: Track[]): Promise<CreditEntity[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Extract up to 5 highly confident music credits for this playlist idea.
If uncertain, return an empty array.
Do not invent or guess.

Prompt: "${prompt}"
Tracks: ${JSON.stringify(tracks.map((t) => ({ artist: t.artist, song: t.song })))}

Allowed roles (must use exact role strings):
- producer
- cover_designer
- photographer
- art_director
- design_studio
- engineer
- arranger

Rules:
- credits must be recording-related credits only
- do not infer group membership or person identity
- if the role is ambiguous, return an empty array
- "name" must be a real person/entity/organization name.
- Never return track labels or artist-title labels as names.
- Reject formats like "Artist - Track", "Artist – Track", "Artist — Track".
- If uncertain, return an empty array.

Return ONLY valid JSON in this format:
[{"name":"", "role":""}]`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(cleaned);

    if (!Array.isArray(parsed)) return [];

    const output: CreditEntity[] = [];
    const dedup = new Set<string>();

    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;

      const name = typeof item.name === 'string' ? canonicalizeDisplayName(item.name) : '';
      const role = typeof item.role === 'string' ? item.role.trim() : '';
      if (!name || !role) continue;
      if (!ALLOWED_CREDIT_ROLES.has(role)) continue;
      if (/\s[-–—]\s/.test(name)) continue;

      const canonicalName = buildCreditCanonicalKey(name);
      if (!canonicalName) continue;
      const key = `${canonicalName}::${role}`;
      if (dedup.has(key)) continue;
      dedup.add(key);
      output.push({ name, role });

      if (output.length >= 5) break;
    }

    return output;
  } catch (e) {
    console.error('[Gemini] Credits generation failed:', e);
    return [];
  }
}

async function generateEquipment(prompt: string, tracks: Track[]): Promise<EquipmentEntity[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Extract up to 8 highly confident music equipment entities for this playlist idea.
If uncertain, return an empty array.
Do not invent or guess.

Prompt: "${prompt}"
Tracks: ${JSON.stringify(tracks.map((t) => ({ artist: t.artist, song: t.song })))}

Allowed categories (must use exact values):
- instrument
- microphone
- synthesizer
- drum_machine
- effect
- amplifier
- console
- tape_machine
- sampler
- other

Rules:
- prefer specific, identifiable equipment entities over generic category labels
- avoid generic labels such as: microphone, console, tape machine, guitar, bass, electric bass, saxophone, keyboard, piano, drums, organ, vocals
- if only generic labels are available, return an empty array

Return ONLY valid JSON in this format:
[{"name":"", "category":""}]`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(cleaned);

    if (!Array.isArray(parsed)) return [];

    const output: EquipmentEntity[] = [];
    const dedup = new Set<string>();

    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;

      const name = typeof item.name === 'string' ? item.name.trim() : '';
      const category = typeof item.category === 'string' ? item.category.trim() : '';
      if (!name || !category) continue;
      if (!ALLOWED_EQUIPMENT_CATEGORIES.has(category)) continue;

      const canonicalName = canonicalizeEquipmentName(name);
      if (!canonicalName) continue;
      if (isGenericEquipmentName(canonicalName)) continue;
      if (isLikelyNonEquipmentEntityName(canonicalName)) continue;

      const key = `${canonicalName.toLowerCase()}::${category}`;
      if (dedup.has(key)) continue;
      dedup.add(key);
      output.push({ name: canonicalName, category });

      if (output.length >= 8) break;
    }

    return output;
  } catch (e) {
    console.error('[Gemini] Equipment generation failed:', e);
    return [];
  }
}

async function generateArtistMemberships(prompt: string, tracks: Track[]): Promise<MembershipEntity[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Extract up to 5 highly confident band membership relations for this playlist idea.
If uncertain, return an empty array.
Do not invent or guess.

Prompt: "${prompt}"
Tracks: ${JSON.stringify(tracks.map((t) => ({ artist: t.artist, song: t.song })))}

Rules:
- Only return membership when the target is clearly a band/group context.
- Do not infer from collaboration, production, or co-occurrence.
- If unsure whether a person is a real member, return an empty array.

Return ONLY valid JSON in this format:
[{"band":"", "person":"", "role":""}]`
    ));

    const text = result.response.text().trim();
    const cleaned = text.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(cleaned);

    if (!Array.isArray(parsed)) return [];

    const output: MembershipEntity[] = [];
    const dedup = new Set<string>();

    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;

      const band = typeof item.band === 'string' ? canonicalizeDisplayName(item.band) : '';
      const person = typeof item.person === 'string' ? canonicalizeDisplayName(item.person) : '';
      const role = typeof item.role === 'string' ? item.role.trim() : '';
      if (!band || !person) continue;

      const bandCanonical = buildArtistCanonicalKey(band);
      const personCanonical = buildPersonCanonicalKey(person);
      if (!bandCanonical || !personCanonical) continue;

      const key = `${bandCanonical}::${personCanonical}`;
      if (dedup.has(key)) continue;
      dedup.add(key);

      output.push({ band, person, role });
      if (output.length >= 5) break;
    }

    return output;
  } catch (e) {
    console.error('[Gemini] Artist memberships generation failed:', e);
    return [];
  }
}

function sanitizeLocationMetadata(input: {
  countries: string[];
  cities: string[];
  studios: string[];
  venues: string[];
}): {
  countries: string[];
  cities: string[];
  studios: string[];
  venues: string[];
} {
  const companyLike = /\b(records|recordings|label|collective|ltd|inc|corp|gmbh|agency|design|photo|photography)\b/i;
  const personLike = /^[A-Z][a-z]+\s+[A-Z][a-z]+$/;

  const countries = input.countries
    .map((v) => v.trim())
    .filter((v) => v.length > 0)
    .filter((v) => !companyLike.test(v))
    .filter((v) => !/\d/.test(v));

  const cities = input.cities
    .map((v) => v.trim())
    .filter((v) => v.length > 0)
    .filter((v) => !companyLike.test(v))
    .filter((v) => !/\d/.test(v));

  const studios = input.studios
    .map((v) => v.trim())
    .filter((v) => v.length > 0)
    .filter((v) => /\b(studio|studios|recorders|recording)\b/i.test(v))
    .filter((v) => !companyLike.test(v))
    .filter((v) => !personLike.test(v));

  const venues = input.venues
    .map((v) => v.trim())
    .filter((v) => v.length > 0)
    .filter((v) => /\b(club|hall|theatre|theater|arena|venue|cbgb)\b/i.test(v))
    .filter((v) => !companyLike.test(v))
    .filter((v) => !personLike.test(v));

  return {
    countries: Array.from(new Set(countries)).slice(0, 2),
    cities: Array.from(new Set(cities)).slice(0, 3),
    studios: Array.from(new Set(studios)).slice(0, 3),
    venues: Array.from(new Set(venues)).slice(0, 3),
  };
}

function sanitizeScenes(scenes: string[]): string[] {
  const banned = /\b(records|label|collective|design|photo|photography|studios?)\b/i;
  const personLike = /^[A-Z][a-z]+\s+[A-Z][a-z]+$/;

  return Array.from(
    new Set(
      scenes
        .map((v) => v.trim())
        .filter((v) => v.length > 0)
        .filter((v) => !banned.test(v))
        .filter((v) => !personLike.test(v))
    )
  ).slice(0, 3);
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

type ConstraintKind = 'artist' | 'studio' | 'venue' | 'scene' | 'credit' | 'unsupported';

interface PromptConstraint {
  kind: ConstraintKind;
  value: string;
  associatedArtists: Set<string>;
  strength: 'strict' | 'medium' | 'soft' | 'skip';
  creditRole?: string;
  creditMembersOfBand?: boolean;
}

function cleanCreditName(raw: string): string {
  const compact = raw
    .trim()
    .replace(/^[-:\s]+/, '')
    .replace(/[.!?,;:]+$/g, '')
    .replace(/\s+/g, ' ');

  if (!compact) return '';

  const firstSegment = compact.split(/\s+(?:and|with|featuring|feat\.?|for|from)\s+/i)[0]?.trim() || '';
  if (!firstSegment) return '';

  const words = firstSegment.split(' ').filter(Boolean);
  if (words.length === 0 || words.length > 6) return '';

  return firstSegment;
}

function isLikelyCreditEntityName(name: string, role: string): boolean {
  const normalized = name.trim().toLowerCase();
  if (!normalized) return false;

  const genericSingleTokens = new Set([
    'producer',
    'producers',
    'engineer',
    'engineers',
    'arranger',
    'arrangers',
    'arrangement',
    'arrangements',
    'production',
    'productions',
    'record',
    'records',
    'recording',
    'recordings',
    'song',
    'songs',
    'track',
    'tracks',
    'music',
    'hit',
    'hits',
    'classic',
    'classics',
    'best',
    'top',
    'swedish',
    'american',
    'british',
    'german',
    'french',
    'italian',
    'japanese',
    'nordic',
    'european',
  ]);

  if (genericSingleTokens.has(normalized)) return false;
  if (/^(?:the\s+)?best\b/.test(normalized)) return false;
  if (/^(?:the\s+)?top\b/.test(normalized)) return false;
  if (/^(?:the\s+)?great(?:est)?\b/.test(normalized)) return false;
  if (/^\d{2,4}s?$/.test(normalized)) return false;

  if (role === 'producer') {
    if (/\b(?:producer|productions?|records?|recordings?|songs?|tracks?)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/\b(?:from|in|at|of|for|with)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  if (role === 'engineer') {
    if (/(?:engineer|engineers|engineering|mix|mixed|recordings?|songs?|tracks?)/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/(?:from|in|at|of|for|with)/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  if (role === 'arranger') {
    if (/(?:arranger|arrangers|arrangement|arrangements|arranged|recordings?|songs?|tracks?)/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/(?:from|in|at|of|for|with)/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  return true;
}

function isLikelyCreditEntityNameV2(name: string, role: string): boolean {
  const normalized = name.trim().toLowerCase();
  if (!normalized) return false;

  const genericSingleTokens = new Set([
    'producer', 'producers', 'engineer', 'engineers', 'arranger', 'arrangers',
    'arrangement', 'arrangements', 'production', 'productions',
    'record', 'records', 'recording', 'recordings',
    'song', 'songs', 'track', 'tracks',
    'producent', 'producenter', 'inspelning', 'inspelningar',
    'låt', 'låtar', 'lat', 'latar', 'spår', 'spar',
    'arrangör', 'arrangor', 'ljudtekniker',
    'mixad', 'mixade', 'arrangerad', 'arrangerade', 'producerad', 'producerade',
    'music', 'hit', 'hits', 'classic', 'classics', 'best', 'top',
    'swedish', 'american', 'british', 'german', 'french', 'italian', 'japanese', 'nordic', 'european',
  ]);

  if (genericSingleTokens.has(normalized)) return false;
  if (/^(?:the\s+)?best\b/.test(normalized)) return false;
  if (/^(?:the\s+)?top\b/.test(normalized)) return false;
  if (/^(?:the\s+)?great(?:est)?\b/.test(normalized)) return false;
  if (/^\d{2,4}s?$/.test(normalized)) return false;

  if (role === 'producer') {
    if (/\b(?:producer|productions?|records?|recordings?|songs?|tracks?|producent|producenter|producerad(?:e)?)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/\b(?:from|in|at|of|for|with|av)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  if (role === 'engineer') {
    if (/\b(?:engineer|engineers|engineering|mix|mixed|recordings?|songs?|tracks?|ljudtekniker|mixade)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/\b(?:from|in|at|of|for|with|av)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  if (role === 'arranger') {
    if (/\b(?:arranger|arrangers|arrangement|arrangements|arranged|recordings?|songs?|tracks?|arrangör|arrangor|arrangerad(?:e)?)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
    if (/\b(?:from|in|at|of|for|with|av)\b/.test(normalized) && normalized.split(' ').length <= 2) {
      return false;
    }
  }

  return true;
}

function detectCreditPrompt(prompt: string): { role: string; name: string; membersOfBand?: boolean } | null {
  const patterns: Array<{ role: string; regex: RegExp; membersOfBand?: boolean }> = [
    { role: 'producer', regex: /\bproduced by members? of\s+(.+)$/i, membersOfBand: true },
    { role: 'producer', regex: /\bproducer\s*:\s*(.+)$/i },
    { role: 'producer', regex: /\bproducer\s*,\s*(.+)$/i },
    { role: 'producer', regex: /\b(?:songs?|tracks?|recordings?)\s+by\s+(?:[a-z]+\s+)*producer\s+(.+)$/i },
    { role: 'producer', regex: /\bproduced by\s+(?:[a-z]+\s+)*producer\s+(.+)$/i },
    { role: 'producer', regex: /\b(.+?)\s*[-–—]\s*produced\s+(?:songs?|tracks?|recordings?)\b/i },
    { role: 'producer', regex: /\b(.+?)['’]s\s+productions?\b/i },
    { role: 'producer', regex: /\bproductions?\s+of\s+(?:[a-z]+\s+)*producer\s+(.+)$/i },
    { role: 'producer', regex: /\bproductions?\s+by\s+(?:[a-z]+\s+)*producer\s+(.+)$/i },
    { role: 'producer', regex: /\bproductions?\s+by\s+(.+)$/i },
    { role: 'producer', regex: /\b(?:the\s+)?work\s+of\s+(?:[a-z]+\s+)*producer\s+(.+)$/i },
    { role: 'producer', regex: /\b(?:the\s+)?work\s+of\s+(.+?)\s+as\s+(?:a\s+|an\s+)?producer\b/i },
    { role: 'producer', regex: /\b(?:[a-z]+\s+)*producer\s+(.+?)\s+productions?\b/i },
    { role: 'producer', regex: /\b(.+?)\s+productions?(?:\s|$)/i },
    { role: 'producer', regex: /\bwith\s+(.+?)\s+as\s+(?:a\s+|an\s+)?producer\b/i },
    { role: 'producer', regex: /\b(.+?)\s+as\s+(?:a\s+|an\s+)?producer\b/i },
    { role: 'producer', regex: /\b(?:låtar|latar|spår|spar|inspelningar)\s+producerade\s+av\s+(.+)$/i },
    { role: 'producer', regex: /\bproduktioner\s+av\s+(.+)$/i },
    { role: 'producer', regex: /\b(?:arbete|arbeten|verk)\s+av\s+(.+?)\s+som\s+producent\b/i },
    { role: 'producer', regex: /\bproducerad(?:e)?\s+av\s+(.+)$/i },
    { role: 'producer', regex: /\bproduced by\s+(.+)$/i },
    { role: 'engineer', regex: /\bengineer\s*:\s*(.+)$/i },
    { role: 'engineer', regex: /\bengineer\s*,\s*(.+)$/i },
    { role: 'engineer', regex: /\b(?:songs?|tracks?|recordings?)\s+by\s+(?:[a-z]+\s+)*engineer\s+(.+)$/i },
    { role: 'engineer', regex: /\bengineered by\s+(?:[a-z]+\s+)*engineer\s+(.+)$/i },
    { role: 'engineer', regex: /\bmixed by\s+(?:[a-z]+\s+)*engineer\s+(.+)$/i },
    { role: 'engineer', regex: /\b(?:the\s+)?work\s+of\s+(?:[a-z]+\s+)*engineer\s+(.+)$/i },
    { role: 'engineer', regex: /\b(?:the\s+)?work\s+of\s+(.+?)\s+as\s+(?:a\s+|an\s+)?engineer\b/i },
    { role: 'engineer', regex: /\b(.+?)\s*[-–—]\s*engineered\s+(?:songs?|tracks?|recordings?)\b/i },
    { role: 'engineer', regex: /\bwith\s+(.+?)\s+as\s+(?:a\s+|an\s+)?engineer\b/i },
    { role: 'engineer', regex: /\b(.+?)\s+as\s+(?:a\s+|an\s+)?engineer\b/i },
    { role: 'engineer', regex: /\b(?:låtar|latar|spår|spar|inspelningar)\s+(?:engineerade|mixade)\s+av\s+(.+)$/i },
    { role: 'engineer', regex: /\b(?:arbete|arbeten|verk)\s+av\s+(.+?)\s+som\s+(?:ljudtekniker|engineer)\b/i },
    { role: 'engineer', regex: /\b(?:mixad(?:e)?|engineerad(?:e)?)\s+av\s+(.+)$/i },
    { role: 'engineer', regex: /\bmixed by\s+(.+)$/i },
    { role: 'engineer', regex: /\bengineered by\s+(.+)$/i },
    { role: 'arranger', regex: /\barranger\s*:\s*(.+)$/i },
    { role: 'arranger', regex: /\barranger\s*,\s*(.+)$/i },
    { role: 'arranger', regex: /\b(?:songs?|tracks?|recordings?)\s+by\s+(?:[a-z]+\s+)*arranger\s+(.+)$/i },
    { role: 'arranger', regex: /\barranged by\s+(?:[a-z]+\s+)*arranger\s+(.+)$/i },
    { role: 'arranger', regex: /\b(?:the\s+)?work\s+of\s+(?:[a-z]+\s+)*arranger\s+(.+)$/i },
    { role: 'arranger', regex: /\b(?:the\s+)?work\s+of\s+(.+?)\s+as\s+(?:a\s+|an\s+)?arranger\b/i },
    { role: 'arranger', regex: /\b(.+?)\s*[-–—]\s*arranged\s+(?:songs?|tracks?|recordings?)\b/i },
    { role: 'arranger', regex: /\bwith\s+(.+?)\s+as\s+(?:a\s+|an\s+)?arranger\b/i },
    { role: 'arranger', regex: /\b(.+?)\s+as\s+(?:a\s+|an\s+)?arranger\b/i },
    { role: 'arranger', regex: /\b(?:låtar|latar|spår|spar|inspelningar)\s+arrangerade\s+av\s+(.+)$/i },
    { role: 'arranger', regex: /\b(?:arbete|arbeten|verk)\s+av\s+(.+?)\s+som\s+arrang(?:ö|o)r\b/i },
    { role: 'arranger', regex: /\barrangerad(?:e)?\s+av\s+(.+)$/i },
    { role: 'cover_designer', regex: /\b(?:cover\s+art|artwork|cover\s+design|sleeve\s+design)\s+created by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\b(?:records? with\s+)?covers? designed by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\balbums? designed by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bdesigned by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bcover design by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bsleeve design by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bsleeve art by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bcover art by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\balbum art by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bartwork by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\billustrated by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\billustration by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bcover illustration by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\balbum illustration by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bvisual design by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bpackaging design by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bcover artist\s+(.+)$/i },
    { role: 'cover_designer', regex: /\bsleeve designer\s+(.+)$/i },
    { role: 'art_director', regex: /\bvisual identity by\s+(.+)$/i },
    { role: 'cover_designer', regex: /\b(?:cover design|artwork) by\s+(.+)$/i },
    { role: 'photographer', regex: /\b(?:photographed|photography) by\s+(.+)$/i },
    { role: 'photographer', regex: /\bcover photo by\s+(.+)$/i },
    { role: 'art_director', regex: /\bart direction by\s+(.+)$/i },
    { role: 'arranger', regex: /\barranged by\s+(.+)$/i },
  ];

  for (const pattern of patterns) {
    const match = prompt.match(pattern.regex);
    if (!match) continue;
    const name = cleanCreditName(match[1] || '');
    if (!isLikelyCreditEntityNameV2(name, pattern.role)) continue;
    return { role: pattern.role, name, membersOfBand: pattern.membersOfBand };
  }

  return null;
}

export function detectCreditPromptForEval(prompt: string): { role: string; name: string; membersOfBand?: boolean } | null {
  return detectCreditPrompt(prompt);
}

export function creditTitleMatchForEval(left: string, right: string): boolean {
  return titlesLikelySameForCreditEvidence(left, right);
}

export function creditArtistFoldForEval(value: string): string {
  return foldForCreditEvidenceMatch(value);
}

export function refineCreditTrackReasonsForEval(
  tracks: Array<{ artist: string; song: string; reason: string }>,
  role: string,
  name: string
): Array<{ artist: string; song: string; reason: string }> {
  const refined = refineCreditTrackReasons(
    tracks.map((track) => ({ ...track })),
    {
      kind: 'credit',
      value: name,
      creditRole: role,
      associatedArtists: new Set<string>(),
      strength: 'strict',
    }
  );

  return refined.map((track) => ({
    artist: track.artist,
    song: track.song,
    reason: track.reason,
  }));
}

function normalize(value: string): string {
  return value.trim().toLowerCase();
}

function foldForCreditEvidenceMatch(value: string): string {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[’']/g, "'")
    .replace(/&/g, ' and ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeTitleForCreditEvidenceMatch(value: string, dropParenthetical = false): string {
  let next = foldForCreditEvidenceMatch(value);
  if (dropParenthetical) {
    next = next.replace(/\([^)]*\)/g, ' ');
  }

  return next
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function titleTokenSet(value: string): Set<string> {
  const tokens = normalizeTitleForCreditEvidenceMatch(value)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length >= 2);
  return new Set(tokens);
}

function titlesLikelySameForCreditEvidence(left: string, right: string): boolean {
  const exactLeft = normalizeTitleForCreditEvidenceMatch(left);
  const exactRight = normalizeTitleForCreditEvidenceMatch(right);
  if (!exactLeft || !exactRight) return false;
  if (exactLeft === exactRight) return true;

  const noParenLeft = normalizeTitleForCreditEvidenceMatch(left, true);
  const noParenRight = normalizeTitleForCreditEvidenceMatch(right, true);
  if (noParenLeft && noParenRight && noParenLeft === noParenRight) return true;

  if (noParenLeft.length >= 12 && noParenRight.length >= 12) {
    if (noParenLeft.includes(noParenRight) || noParenRight.includes(noParenLeft)) {
      return true;
    }
  }

  const leftTokens = titleTokenSet(left);
  const rightTokens = titleTokenSet(right);
  if (leftTokens.size < 3 || rightTokens.size < 3) return false;

  let overlap = 0;
  for (const token of leftTokens) {
    if (rightTokens.has(token)) overlap += 1;
  }

  const union = new Set<string>([...leftTokens, ...rightTokens]).size;
  if (union === 0) return false;
  const jaccard = overlap / union;
  return jaccard >= 0.85;
}

function shouldAttemptAutoBackfill(creditName: string, creditRole: string): { allowed: boolean; reason?: string } {
  const key = `${normalize(creditRole)}::${foldForCreditEvidenceMatch(creditName)}`;
  const now = Date.now();
  const last = lastAutoBackfillByCredit.get(key) || 0;
  if (last > 0 && now - last < AUTO_BACKFILL_COOLDOWN_MS) {
    return { allowed: false, reason: 'cooldown_active' };
  }

  lastAutoBackfillByCredit.set(key, now);
  return { allowed: true };
}

function hasStudioIntent(prompt: string): boolean {
  return /\bstudio\b|\bstudios\b|\brecorded at\b|\brecorded in\b|\btracked at\b|\btracked in\b|\bcut at\b|\bcut in\b|\bmade at\b|\bmade in\b|\bdone at\b|\bdone in\b/.test(normalize(prompt));
}

function dedupeStudiosByCanonical(studios: string[]): string[] {
  const deduped = new Map<string, string>();
  for (const raw of studios) {
    const studio = raw.trim();
    if (!studio) continue;
    const canonical = buildStudioCanonicalKey(studio);
    if (!canonical) continue;

    const existing = deduped.get(canonical);
    if (existing) {
      deduped.set(canonical, studio.length > existing.length ? studio : existing);
    } else {
      deduped.set(canonical, studio);
    }
  }
  return Array.from(deduped.values());
}

function isHistoryOrStoryPrompt(prompt: string): boolean {
  return /\b(?:history|story|timeline|chronolog(?:ical|y)|through the years|evolution|origins?)\b/i.test(prompt);
}

function buildHistoryOrderingSuffix(prompt: string): string {
  if (!isHistoryOrStoryPrompt(prompt)) return '';
  return 'For this history/story request, order tracks chronologically from oldest release to newest whenever possible.';
}

async function sortTracksChronologicallyIfNeeded(prompt: string, tracks: Track[]): Promise<Track[]> {
  if (!isHistoryOrStoryPrompt(prompt) || tracks.length < 3) {
    return tracks;
  }

  const withMeta = await Promise.all(
    tracks.map(async (track, index) => {
      try {
        const info = await searchTrackWithDiagnostics(track.artist, track.song, prompt);
        const year = typeof info.release_year === 'number' ? info.release_year : null;
        return { ...track, release_year: year, __index: index };
      } catch {
        return { ...track, release_year: null, __index: index };
      }
    })
  );

  const tracksWithYear = withMeta.filter((track) => typeof track.release_year === 'number');
  if (tracksWithYear.length < 3) {
    return tracks;
  }

  withMeta.sort((a, b) => {
    const aYear = typeof a.release_year === 'number' ? a.release_year : Number.POSITIVE_INFINITY;
    const bYear = typeof b.release_year === 'number' ? b.release_year : Number.POSITIVE_INFINITY;
    if (aYear !== bYear) return aYear - bYear;
    return a.__index - b.__index;
  });

  return withMeta.map(({ __index, ...track }) => track);
}

function normalizeTagForComparison(tag: string): string {
  return tag
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function canonicalizeGeneratedTags(generatedTags: string[]): string[] {
  const existingTags = getKnownTags();
  const existingByKey = new Map<string, string>();

  for (const tag of existingTags) {
    const key = normalizeTagForComparison(tag);
    if (!key) continue;
    if (!existingByKey.has(key)) {
      existingByKey.set(key, tag);
    }
  }

  const result: string[] = [];
  const seen = new Set<string>();

  for (const tag of generatedTags) {
    const trimmed = tag.trim();
    if (!trimmed) continue;

    const key = normalizeTagForComparison(trimmed);
    if (!key) continue;

    const canonical = existingByKey.get(key) || trimmed;
    const canonicalKey = normalizeTagForComparison(canonical);
    if (seen.has(canonicalKey)) continue;

    seen.add(canonicalKey);
    result.push(canonical);
  }

  return result;
}

function findLongestIncluded(prompt: string, candidates: string[]): string | null {
  const promptLower = normalize(prompt);
  const sorted = [...candidates].sort((a, b) => b.length - a.length);

  for (const candidate of sorted) {
    const candidateLower = normalize(candidate);
    if (!candidateLower) continue;
    if (promptLower.includes(candidateLower)) {
      return candidate;
    }
  }

  return null;
}

function detectPromptConstraint(prompt: string): PromptConstraint | null {
  const catalog = getAtlasEntityCatalog();
  const lowerPrompt = normalize(prompt);
  const credit = detectCreditPrompt(prompt);
  const hasStudioCue = /\bstudio\b|\bstudios\b/.test(lowerPrompt);
  const hasVenueCue = /\bvenue\b|\bclub\b|\bhall\b|\btheatre\b|\btheater\b|\barena\b|\bcbgb\b/.test(lowerPrompt);
  const hasSceneCue = /\bscene\b|\bmovement\b/.test(lowerPrompt);
  const hasArtistCue = /\bsongs? by\b|\btracks? by\b|\bartist\b|\bby\b/.test(lowerPrompt);

  if (credit) {
    let associatedArtists: Set<string>;

    if (credit.membersOfBand) {
      const members = getBandMembers(credit.name);
      associatedArtists = new Set<string>();
      for (const member of members) {
        const memberAtlas = getCreditAtlas(member);
        for (const artistName of memberAtlas.relatedArtists) {
          associatedArtists.add(normalize(artistName));
        }
      }
    } else {
      const creditAtlas = getCreditAtlas(credit.name);
      associatedArtists = new Set(creditAtlas.relatedArtists.map(normalize));
    }

    return {
      kind: 'credit',
      value: credit.name,
      associatedArtists,
      strength: 'strict',
      creditRole: credit.role,
      creditMembersOfBand: Boolean(credit.membersOfBand),
    };
  }

  if (hasStudioCue) {
    const studio = findLongestIncluded(prompt, catalog.studios);
    if (studio) {
      return {
        kind: 'studio',
        value: studio,
        associatedArtists: new Set(getAssociatedArtistsByNode('studio', studio).map(normalize)),
        strength: 'medium',
      };
    }
    const extractedPlace = extractPlaceEntityFromPrompt(prompt);
    if (extractedPlace && isValidPlaceEntityName(extractedPlace)) {
      return {
        kind: 'studio',
        value: extractedPlace,
        associatedArtists: new Set(getAssociatedArtistsByNode('studio', extractedPlace).map(normalize)),
        strength: 'medium',
      };
    }
    return { kind: 'studio', value: '', associatedArtists: new Set(), strength: 'medium' };
  }

  if (hasVenueCue) {
    const venue = findLongestIncluded(prompt, catalog.venues);
    if (venue) {
      return {
        kind: 'venue',
        value: venue,
        associatedArtists: new Set(getAssociatedArtistsByNode('venue', venue).map(normalize)),
        strength: 'medium',
      };
    }
    const extractedPlace = extractPlaceEntityFromPrompt(prompt);
    if (extractedPlace && isValidPlaceEntityName(extractedPlace) && inferPlaceTypeHeuristic(extractedPlace) === 'venue') {
      return {
        kind: 'venue',
        value: extractedPlace,
        associatedArtists: new Set(getAssociatedArtistsByNode('venue', extractedPlace).map(normalize)),
        strength: 'medium',
      };
    }
    return { kind: 'venue', value: '', associatedArtists: new Set(), strength: 'medium' };
  }

  if (hasSceneCue) {
    const scene = findLongestIncluded(prompt, catalog.scenes);
    if (scene) {
      return {
        kind: 'scene',
        value: scene,
        associatedArtists: new Set(getAssociatedArtistsByNode('scene', scene).map(normalize)),
        strength: 'soft',
      };
    }
    return { kind: 'scene', value: '', associatedArtists: new Set(), strength: 'soft' };
  }

  if (hasArtistCue) {
    const artist = findLongestIncluded(prompt, catalog.artists);
    if (artist) {
      return {
        kind: 'artist',
        value: artist,
        associatedArtists: new Set([normalize(artist)]),
        strength: 'strict',
      };
    }
    return { kind: 'artist', value: '', associatedArtists: new Set(), strength: 'strict' };
  }

  return null;
}

function detectGraphHintSeed(prompt: string): { type: 'artist' | 'equipment' | 'scene' | 'studio' | 'city'; value: string } | null {
  const lowerPrompt = normalize(prompt);
  const catalog = getAtlasEntityCatalog();

  const cityOrStudioMentions = [
    ...catalog.studios.map((value) => ({ type: 'studio' as const, value })),
  ];

  const matches: Array<{ type: 'artist' | 'equipment' | 'scene' | 'studio' | 'city'; value: string }> = [];

  for (const artist of catalog.artists) {
    if (lowerPrompt.includes(normalize(artist))) {
      matches.push({ type: 'artist', value: artist });
    }
  }

  for (const scene of catalog.scenes) {
    if (lowerPrompt.includes(normalize(scene))) {
      matches.push({ type: 'scene', value: scene });
    }
  }

  for (const studio of cityOrStudioMentions) {
    if (lowerPrompt.includes(normalize(studio.value))) {
      matches.push(studio);
    }
  }

  const equipmentCandidates = [
    'mellotron',
    'mellotron mk i',
    'tr-808',
    'roland tr-808',
    'minimoog',
    'neumann u47',
    'neve 80 series console',
  ];
  for (const equipment of equipmentCandidates) {
    if (lowerPrompt.includes(normalize(equipment))) {
      matches.push({ type: 'equipment', value: canonicalizeEquipmentName(equipment) });
    }
  }

  const cityCandidates = ['new york city', 'los angeles', 'san francisco', 'london', 'berlin'];
  for (const city of cityCandidates) {
    if (lowerPrompt.includes(normalize(city))) {
      matches.push({ type: 'city', value: city });
    }
  }

  if (matches.length !== 1) {
    return null;
  }

  return matches[0];
}

function buildGraphHintText(
  neighbors: Array<{ nodeType: string; nodeName: string; relationType: string; score: number }>
): string {
  if (neighbors.length === 0) return '';
  const formatted = neighbors.slice(0, 5).map((item) => `${item.nodeName} (${item.nodeType})`);
  return `Graph hints: ${formatted.join(', ')}`;
}

function filterTracksByConstraint(
  tracks: Track[],
  constraint: PromptConstraint | null,
  promptText = '',
  creditEvidenceTracks: Array<{ artist: string; title: string }> = []
): Track[] {
  if (!constraint || constraint.strength === 'skip') {
    return tracks;
  }

  const isStudioOrVenueConstraint = constraint.kind === 'studio' || constraint.kind === 'venue';
  if (!constraint.value) {
    if (constraint.strength === 'soft') {
      return tracks;
    }
    for (const track of tracks) {
      console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="insufficient association data"`);
    }
    return [];
  }

  if (!isStudioOrVenueConstraint && constraint.kind !== 'credit' && constraint.strength !== 'soft' && constraint.associatedArtists.size === 0) {
    for (const track of tracks) {
      console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="insufficient association data"`);
    }
    return [];
  }

  if (constraint.kind === 'scene' && constraint.strength === 'soft') {
    if (constraint.associatedArtists.size === 0) {
      return tracks;
    }

    const preferred: Track[] = [];
    const fallback: Track[] = [];

    for (const track of tracks) {
      const artist = normalize(track.artist);
      if (constraint.associatedArtists.has(artist)) {
        preferred.push(track);
      } else {
        fallback.push(track);
      }
    }

    return [...preferred, ...fallback];
  }

  if (constraint.kind === 'studio') {
    const targetStudio = constraint.value.trim();
    if (!targetStudio) return [];

    return tracks.filter((track) => {
      const keep = hasRecordingStudioEvidence(track.artist, track.song, targetStudio);
      if (!keep) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="no recording-level studio evidence"`);
      }
      return keep;
    });
  }

  if (constraint.kind === 'venue') {
    const MIN_SCORE = 2;
    const promptLower = normalize(promptText);
    const supportFromScene = new Set<string>();
    const supportFromLocation = new Set<string>();
    const supportFromGraph = new Set<string>();

    try {
      const graphNeighbors = getGraphNeighbors(constraint.kind, constraint.value, 20);
      for (const neighbor of graphNeighbors) {
        if (neighbor.nodeType === 'artist') {
          supportFromGraph.add(normalize(neighbor.nodeName));
        }
      }
    } catch {
      // Skip graph support quietly.
    }

    try {
      const catalog = getAtlasEntityCatalog();
      for (const scene of catalog.scenes) {
        if (!promptLower.includes(normalize(scene))) continue;
        for (const artist of getAssociatedArtistsByNode('scene', scene)) {
          supportFromScene.add(normalize(artist));
        }
      }
    } catch {
      // Skip scene support quietly.
    }

    try {
      const cityCandidates = ['new york city', 'los angeles', 'san francisco', 'london', 'berlin', 'philadelphia', 'muscle shoals'];
      const countryCandidates = ['united states', 'usa', 'united kingdom', 'uk', 'england', 'france', 'germany', 'italy', 'japan', 'canada'];

      for (const city of cityCandidates) {
        if (!promptLower.includes(normalize(city))) continue;
        const neighbors = getGraphNeighbors('city', city, 20);
        for (const neighbor of neighbors) {
          if (neighbor.nodeType === 'artist') {
            supportFromLocation.add(normalize(neighbor.nodeName));
          }
        }
      }

      for (const country of countryCandidates) {
        if (!promptLower.includes(normalize(country))) continue;
        const neighbors = getGraphNeighbors('country', country, 20);
        for (const neighbor of neighbors) {
          if (neighbor.nodeType === 'artist') {
            supportFromLocation.add(normalize(neighbor.nodeName));
          }
        }
      }
    } catch {
      // Skip location support quietly.
    }

    return tracks.filter((track) => {
      const artist = normalize(track.artist);
      if (!artist) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="missing artist"`);
        return false;
      }

      let score = 0;
      if (constraint.associatedArtists.has(artist)) score += 3;
      if (supportFromScene.has(artist)) score += 1;
      if (supportFromLocation.has(artist)) score += 1;
      if (supportFromGraph.has(artist)) score += 1;

      const keep = score >= MIN_SCORE;
      if (!keep) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="low verification score (${score} < ${MIN_SCORE})"`);
      }
      return keep;
    });
  }

  return tracks.filter((track) => {
    const artist = normalize(track.artist);
    if (!artist) {
      console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="missing artist"`);
      return false;
    }

    if (constraint.kind === 'artist') {
      const keep = artist === normalize(constraint.value);
      if (!keep) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="not matching artist constraint"`);
      }
      return keep;
    }

    if (constraint.kind === 'credit') {
      const role = (constraint.creditRole || '').trim();
      const creditName = (constraint.value || '').trim();
      if (!role || !creditName) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="missing credit constraint details"`);
        return false;
      }

      const exactEvidenceMatch = hasRecordingCreditEvidence(track.artist, track.song, creditName, role);
      const foldedArtist = foldForCreditEvidenceMatch(track.artist);
      const fuzzyEvidenceMatch = !exactEvidenceMatch && creditEvidenceTracks.some((evidenceTrack) => {
        const evidenceArtist = foldForCreditEvidenceMatch(evidenceTrack.artist);
        if (!foldedArtist || !evidenceArtist || foldedArtist !== evidenceArtist) {
          return false;
        }
        return titlesLikelySameForCreditEvidence(track.song, evidenceTrack.title);
      });

      const keep = exactEvidenceMatch || fuzzyEvidenceMatch;
      if (!keep) {
        console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="no recording-level credit evidence"`);
      }
      return keep;
    }

    console.log(`[verification] dropped track "${track.song} - ${track.artist}" reason="unsupported constraint"`);
    return false;
  });
}

function dedupeTracks(tracks: Track[]): Track[] {
  const byKey = new Map<string, Track>();
  const genericEvidenceReason = /^verified\s+.+\s+(?:credit\s+evidence|truth\s+claim(?:\s+from\s+.+)?)\s+for\s+.+$/i;

  for (const track of tracks) {
    const key = `${normalize(track.artist)}::${normalize(track.song)}`;
    if (!key) continue;

    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, track);
      continue;
    }

    const existingReason = (existing.reason || '').trim();
    const nextReason = (track.reason || '').trim();
    const existingIsGeneric = genericEvidenceReason.test(existingReason);
    const nextIsGeneric = genericEvidenceReason.test(nextReason);

    if (existingIsGeneric && !nextIsGeneric) {
      byKey.set(key, { ...track });
      continue;
    }

    if (existing.spotify_url && !track.spotify_url) {
      continue;
    }
    if (existing.album_image_url && !track.album_image_url) {
      continue;
    }

    if (nextReason.length > existingReason.length) {
      byKey.set(key, { ...existing, ...track });
    }
  }

  return Array.from(byKey.values());
}

function getCreditEvidenceTrackCandidates(constraint: PromptConstraint | null, limit = 120): Array<{ artist: string; title: string }> {
  if (!constraint || constraint.kind !== 'credit') return [];

  const creditName = (constraint.value || '').trim();
  const creditRole = (constraint.creditRole || '').trim();
  if (!creditName || !creditRole) return [];

  const evidenceTracks = getTracksByRecordingCreditEvidence(creditName, creditRole, limit);
  const truthTracks = getTruthCreditCandidates(creditName, creditRole, limit)
    .map((row) => ({ artist: row.artist, title: row.title }));

  const dedupe = new Map<string, { artist: string; title: string }>();
  for (const row of [...truthTracks, ...evidenceTracks]) {
    const key = `${normalize(row.artist)}::${normalize(row.title)}`;
    if (!key) continue;
    if (!dedupe.has(key)) {
      dedupe.set(key, row);
    }
  }
  return Array.from(dedupe.values());
}

function rankVerifiedCreditTracksByProminence(
  tracks: Track[],
  constraint: PromptConstraint | null,
  preferredArtists: string[] = [],
  preferredWorks: Array<{ artist: string; song: string }> = [],
  mode: PlaylistCurationMode = 'balanced'
): {
  tracks: Track[];
  topScoreSample: Array<{
    artist: string;
    song: string;
    final_score: number;
    relevance_to_query: number;
    prominence_score: number;
    artist_canonical_score: number;
    entity_signature_score: number;
    diversity_adjustment: number;
  }>;
  rankingFloor: {
    applied: boolean;
    droppedTracks: number;
    floorScore: number;
  };
} {
  if (!constraint || constraint.kind !== 'credit' || tracks.length <= 1) {
    return { tracks, topScoreSample: [], rankingFloor: { applied: false, droppedTracks: 0, floorScore: 0 } };
  }

  const creditName = (constraint.value || '').trim();
  const creditRole = (constraint.creditRole || '').trim();
  if (!creditName || !creditRole) {
    return { tracks, topScoreSample: [], rankingFloor: { applied: false, droppedTracks: 0, floorScore: 0 } };
  }

  const truthCandidates = getTruthCreditCandidates(creditName, creditRole, 500);
  const evidenceCandidates = getTracksByRecordingCreditEvidence(creditName, creditRole, 500);

  const truthArtistProminence = new Map<string, number>();
  for (const row of truthCandidates) {
    const key = foldForCreditEvidenceMatch(row.artist);
    if (!key) continue;
    const confidence = typeof row.confidence === 'number' && Number.isFinite(row.confidence)
      ? Math.max(0, Math.min(100, row.confidence))
      : 0;
    truthArtistProminence.set(key, (truthArtistProminence.get(key) || 0) + confidence);
  }

  const evidenceArtistProminence = new Map<string, number>();
  for (const row of evidenceCandidates) {
    const key = foldForCreditEvidenceMatch(row.artist);
    if (!key) continue;
    const evidenceCount = typeof row.evidence_count === 'number' && Number.isFinite(row.evidence_count)
      ? Math.max(1, Math.floor(row.evidence_count))
      : 1;
    evidenceArtistProminence.set(key, (evidenceArtistProminence.get(key) || 0) + evidenceCount);
  }

  const preferredArtistKeys = new Set(
    preferredArtists.map((value) => foldForCreditEvidenceMatch(value)).filter((value) => value.length > 0)
  );

  const artistTrackCounts = new Map<string, number>();
  for (const track of tracks) {
    const key = foldForCreditEvidenceMatch(track.artist);
    if (!key) continue;
    artistTrackCounts.set(key, (artistTrackCounts.get(key) || 0) + 1);
  }

  const decadeTrackCounts = new Map<number, number>();
  for (const track of tracks) {
    const decade = getTrackDecade(track);
    if (decade === null) continue;
    decadeTrackCounts.set(decade, (decadeTrackCounts.get(decade) || 0) + 1);
  }

  const modeWeights: Record<PlaylistCurationMode, {
    relevance: number;
    prominence: number;
    canonical: number;
    signature: number;
    diversity: number;
  }> = {
    essential: { relevance: 1.05, prominence: 1.45, canonical: 1.05, signature: 1.25, diversity: 0.5 },
    balanced: { relevance: 1.1, prominence: 1.2, canonical: 1.0, signature: 1.0, diversity: 0.85 },
    deep_cuts: { relevance: 1.15, prominence: 0.55, canonical: 0.9, signature: 0.95, diversity: 1.2 },
  };
  const weights = modeWeights[mode] || modeWeights.balanced;

  const scoreTrack = (track: Track, index: number): {
    final_score: number;
    relevance_to_query: number;
    prominence_score: number;
    artist_canonical_score: number;
    entity_signature_score: number;
    diversity_adjustment: number;
  } => {
    const foldedArtist = foldForCreditEvidenceMatch(track.artist);
    if (!foldedArtist) {
      return {
        final_score: -index,
        relevance_to_query: 0,
        prominence_score: 0,
        artist_canonical_score: 0,
        entity_signature_score: 0,
        diversity_adjustment: 0,
      };
    }

    let relevanceToQuery = 0;
    let prominenceScore = 0;
    let artistCanonicalScore = 0;
    let entitySignatureScore = 0;
    let diversityAdjustment = 0;

    if (preferredArtistKeys.has(foldedArtist)) {
      relevanceToQuery += 40;
    }

    const truthProminence = truthArtistProminence.get(foldedArtist) || 0;
    if (truthProminence > 0) {
      prominenceScore += Math.min(180, truthProminence * 0.6);
      artistCanonicalScore += 20;
    }

    const evidenceProminence = evidenceArtistProminence.get(foldedArtist) || 0;
    if (evidenceProminence > 0) {
      prominenceScore += Math.min(120, evidenceProminence * 12);
      artistCanonicalScore += 15;
    }

    for (const work of preferredWorks) {
      if (foldForCreditEvidenceMatch(work.artist) !== foldedArtist) continue;
      if (!titlesLikelySameForCreditEvidence(track.song, work.song)) continue;
      entitySignatureScore += 90;
    }

    for (const row of truthCandidates) {
      if (foldForCreditEvidenceMatch(row.artist) !== foldedArtist) continue;
      if (!titlesLikelySameForCreditEvidence(track.song, row.title)) continue;
      relevanceToQuery += 65;
      entitySignatureScore += 70;
      prominenceScore += Math.max(0, Math.min(100, row.confidence || 0));
    }

    for (const row of evidenceCandidates) {
      if (foldForCreditEvidenceMatch(row.artist) !== foldedArtist) continue;
      if (!titlesLikelySameForCreditEvidence(track.song, row.title)) continue;
      const evidenceCount = typeof row.evidence_count === 'number' && Number.isFinite(row.evidence_count)
        ? Math.max(1, Math.floor(row.evidence_count))
        : 1;
      relevanceToQuery += 60;
      entitySignatureScore += 35;
      prominenceScore += 90 + Math.min(120, evidenceCount * 15);
    }

    const artistCount = artistTrackCounts.get(foldedArtist) || 1;
    if (artistCount > 1) {
      diversityAdjustment -= (artistCount - 1) * 10;
    }

    const decade = getTrackDecade(track);
    if (decade !== null) {
      const decadeCount = decadeTrackCounts.get(decade) || 1;
      if (mode === 'deep_cuts') {
        diversityAdjustment += Math.max(-16, 14 - decadeCount * 3);
      } else if (mode === 'essential') {
        diversityAdjustment += Math.max(-8, 6 - decadeCount);
      } else {
        diversityAdjustment += Math.max(-12, 10 - decadeCount * 2);
      }
    }

    if (mode === 'deep_cuts') {
      if (prominenceScore < 90) {
        diversityAdjustment += 18;
      } else if (prominenceScore > 180) {
        diversityAdjustment -= 12;
      }
    }

    if (mode === 'essential') {
      if (prominenceScore > 140) {
        prominenceScore += 25;
      } else if (prominenceScore < 70) {
        prominenceScore -= 15;
      }
    }

    const finalScore = (
      (relevanceToQuery * weights.relevance)
      + (prominenceScore * weights.prominence)
      + (artistCanonicalScore * weights.canonical)
      + (entitySignatureScore * weights.signature)
      + (diversityAdjustment * weights.diversity)
    ) - index * 0.001;

    return {
      final_score: finalScore,
      relevance_to_query: relevanceToQuery,
      prominence_score: prominenceScore,
      artist_canonical_score: artistCanonicalScore,
      entity_signature_score: entitySignatureScore,
      diversity_adjustment: diversityAdjustment,
    };
  };

  const scored = tracks
    .map((track, index) => ({ track, index, components: scoreTrack(track, index) }))
    .sort((a, b) => {
      if (b.components.final_score !== a.components.final_score) return b.components.final_score - a.components.final_score;
      return a.index - b.index;
    });

  let filteredScored = scored;
  let rankingFloorApplied = false;
  let rankingFloorScore = 0;
  let rankingDroppedTracks = 0;

  if (mode !== 'deep_cuts' && scored.length > 12) {
    const topScore = scored[0]?.components.final_score ?? 0;
    if (Number.isFinite(topScore) && topScore > 0) {
      const ratio = mode === 'essential' ? 0.52 : 0.42;
      rankingFloorScore = topScore * ratio;
      const tentative = scored.filter((entry) => entry.components.final_score >= rankingFloorScore);
      const minimumPreserved = Math.min(12, scored.length);
      if (tentative.length >= minimumPreserved) {
        filteredScored = tentative;
        rankingFloorApplied = true;
        rankingDroppedTracks = scored.length - tentative.length;
      }
    }
  }

  return {
    tracks: filteredScored.map((entry) => entry.track),
    topScoreSample: filteredScored.slice(0, 10).map((entry) => ({
      artist: entry.track.artist,
      song: entry.track.song,
      final_score: Number(entry.components.final_score.toFixed(3)),
      relevance_to_query: Number(entry.components.relevance_to_query.toFixed(3)),
      prominence_score: Number(entry.components.prominence_score.toFixed(3)),
      artist_canonical_score: Number(entry.components.artist_canonical_score.toFixed(3)),
      entity_signature_score: Number(entry.components.entity_signature_score.toFixed(3)),
      diversity_adjustment: Number(entry.components.diversity_adjustment.toFixed(3)),
    })),
    rankingFloor: {
      applied: rankingFloorApplied,
      droppedTracks: rankingDroppedTracks,
      floorScore: Number(rankingFloorScore.toFixed(3)),
    },
  };
}

async function rerankVerifiedCreditTracksWithGemini(
  userPrompt: string,
  constraint: PromptConstraint | null,
  tracks: Track[]
): Promise<Track[]> {
  if (!constraint || constraint.kind !== 'credit' || tracks.length < 6) return tracks;
  if (process.env.ENABLE_GEMINI_CREDIT_CURATION === 'false') return tracks;

  const creditName = (constraint.value || '').trim();
  const creditRole = (constraint.creditRole || '').trim().toLowerCase();
  if (!creditName || !creditRole) return tracks;

  const MAX_CURATION_CANDIDATES = 80;
  const candidates = tracks.slice(0, MAX_CURATION_CANDIDATES).map((track, index) => ({
    id: index + 1,
    artist: track.artist,
    song: track.song,
    year: typeof track.release_year === 'number' ? track.release_year : null,
  }));

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `You are curating a factual credit playlist from already verified songs.

Rules:
- Return ONLY JSON in this format: {"ordered_ids":[1,2,3]}
- Use only IDs from the provided candidate list.
- Do NOT add, invent, or rename songs.
- Prioritize tracks that are most prominent, representative, and culturally significant for the requested credit relationship.
- Prefer well-known signature works when available, while keeping stylistic breadth.
- The order should be best-first for the prompt.

Prompt: ${userPrompt}
Credit: ${creditRole} -> ${creditName}

Candidates:
${JSON.stringify(candidates)}`
    ));

    let text = result.response.text().trim();
    if (text.startsWith('```json')) text = text.slice(7);
    else if (text.startsWith('```')) text = text.slice(3);
    if (text.endsWith('```')) text = text.slice(0, -3);

    const parsed = JSON.parse(text.trim()) as { ordered_ids?: number[] };
    const orderedIds = Array.isArray(parsed.ordered_ids)
      ? parsed.ordered_ids.filter((value) => Number.isInteger(value) && value >= 1 && value <= candidates.length)
      : [];

    if (orderedIds.length === 0) return tracks;

    const orderedUniqueIds: number[] = [];
    const seen = new Set<number>();
    for (const id of orderedIds) {
      if (seen.has(id)) continue;
      seen.add(id);
      orderedUniqueIds.push(id);
    }

    const curatedTop: Track[] = [];
    for (const id of orderedUniqueIds) {
      const track = tracks[id - 1];
      if (!track) continue;
      curatedTop.push(track);
    }

    const curatedKeys = new Set(curatedTop.map((track) => `${normalize(track.artist)}::${normalize(track.song)}`));
    const remainder = tracks.filter((track) => !curatedKeys.has(`${normalize(track.artist)}::${normalize(track.song)}`));
    return [...curatedTop, ...remainder];
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.log(`[Gemini] credit curation rerank skipped: ${message}`);
    return tracks;
  }
}

async function getCreditExpansionArtistsFromGemini(
  userPrompt: string,
  creditName: string,
  creditRole: string
): Promise<string[]> {
  if (process.env.ENABLE_GEMINI_CREDIT_EXPANSION_HINTS === 'false') return [];

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Return ONLY JSON in this format: {"artists":["Artist A","Artist B"]}

Goal:
- Suggest up to 4 major artists strongly associated with ${creditName} as ${creditRole}.
- Prefer prominent, representative collaborations over deep cuts.
- Keep artist names concise.
- Do not include ${creditName}.

Prompt context: ${userPrompt}`
    ));

    let text = result.response.text().trim();
    if (text.startsWith('```json')) text = text.slice(7);
    else if (text.startsWith('```')) text = text.slice(3);
    if (text.endsWith('```')) text = text.slice(0, -3);

    const parsed = JSON.parse(text.trim()) as { artists?: string[] };
    if (!Array.isArray(parsed.artists)) return [];

    const seen = new Set<string>();
    const artists: string[] = [];
    for (const value of parsed.artists) {
      if (typeof value !== 'string') continue;
      const artist = value.trim();
      if (!artist) continue;
      const key = normalize(artist);
      if (!key || key === normalize(creditName) || seen.has(key)) continue;
      seen.add(key);
      artists.push(artist);
      if (artists.length >= 4) break;
    }
    return artists;
  } catch {
    return [];
  }
}

async function getCreditCanonicalWorksFromGemini(
  userPrompt: string,
  creditName: string,
  creditRole: string,
  seedArtists: string[]
): Promise<Array<{ artist: string; song: string }>> {
  if (process.env.ENABLE_GEMINI_CREDIT_EXPANSION_HINTS === 'false') return [];

  const seedList = seedArtists.slice(0, 6).join(', ');

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Return ONLY JSON in this format: {"works":[{"artist":"Artist A","song":"Song X"}]}

Goal:
- Suggest up to 4 canonical works likely associated with ${creditName} as ${creditRole}.
- Prioritize major, representative songs over deep cuts.
- Use concise artist and song names.
- Artist should preferably come from this list when relevant: ${seedList || 'none'}
- Do not include entries where artist equals ${creditName}.

Prompt context: ${userPrompt}`
    ));

    let text = result.response.text().trim();
    if (text.startsWith('```json')) text = text.slice(7);
    else if (text.startsWith('```')) text = text.slice(3);
    if (text.endsWith('```')) text = text.slice(0, -3);

    const parsed = JSON.parse(text.trim()) as { works?: Array<{ artist?: string; song?: string }> };
    if (!Array.isArray(parsed.works)) return [];

    const works: Array<{ artist: string; song: string }> = [];
    const seen = new Set<string>();
    for (const row of parsed.works) {
      if (!row || typeof row !== 'object') continue;
      const artist = typeof row.artist === 'string' ? row.artist.trim() : '';
      const song = typeof row.song === 'string' ? row.song.trim() : '';
      if (!artist || !song) continue;
      if (normalize(artist) === normalize(creditName)) continue;
      const key = `${normalize(artist)}::${normalize(song)}`;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      works.push({ artist, song });
      if (works.length >= 4) break;
    }
    return works;
  } catch {
    return [];
  }
}

function bootstrapCreditEvidenceTracks(constraint: PromptConstraint | null): Track[] {
  if (!constraint || constraint.kind !== 'credit') return [];

  const creditName = (constraint.value || '').trim();
  const creditRole = (constraint.creditRole || '').trim();
  if (!creditName || !creditRole) return [];

  const evidenceTracks = getCreditEvidenceTrackCandidates(constraint, 120);
  if (evidenceTracks.length === 0) return [];

  const roleLabel = creditRole.replace(/_/g, ' ');
  return evidenceTracks.map((row) => ({
    artist: row.artist,
    song: row.title,
    reason: `Verified ${roleLabel} credit evidence for ${creditName}`,
  }));
}

function applyCreditOnlyArtistDiversity(prompt: string, tracks: Track[]): Track[] {
  void prompt;

  const desiredCount = Math.min(MAX_PLAYLIST_TRACKS, tracks.length);
  const chosen: Track[] = [];
  const chosenKeys = new Set<string>();
  const artistCounts = new Map<string, number>();
  const albumCounts = new Map<string, number>();

  const getTrackKey = (track: Track): string => `${normalize(track.artist)}::${normalize(track.song)}`;
  const getAlbumKey = (track: Track): string => normalize(track.album_image_url || '');

  const addWithCaps = (artistCap: number, albumCap: number): void => {
    for (const track of tracks) {
      if (chosen.length >= desiredCount) break;

      const trackKey = getTrackKey(track);
      if (!trackKey || chosenKeys.has(trackKey)) continue;

      const artistKey = normalize(track.artist);
      if (!artistKey) continue;
      const artistCount = artistCounts.get(artistKey) || 0;
      if (artistCount >= artistCap) continue;

      const albumKey = getAlbumKey(track);
      if (albumKey) {
        const albumCount = albumCounts.get(albumKey) || 0;
        if (albumCount >= albumCap) continue;
      }

      chosen.push(track);
      chosenKeys.add(trackKey);
      artistCounts.set(artistKey, artistCount + 1);
      if (albumKey) {
        albumCounts.set(albumKey, (albumCounts.get(albumKey) || 0) + 1);
      }
    }
  };

  addWithCaps(1, 1);
  if (chosen.length < desiredCount) addWithCaps(2, 2);
  if (chosen.length < desiredCount) addWithCaps(3, 3);
  if (chosen.length < desiredCount) addWithCaps(4, 4);

  return chosen.length > 0 ? chosen : tracks;
}

function getTrackDecade(track: Track): number | null {
  const year = typeof track.release_year === 'number' && Number.isFinite(track.release_year)
    ? Math.floor(track.release_year)
    : null;
  if (!year || year < 1900 || year > 2100) return null;
  return Math.floor(year / 10) * 10;
}

function getModeUniqueArtistTarget(mode: PlaylistCurationMode, desiredCount: number): number {
  const ratioByMode: Record<PlaylistCurationMode, number> = {
    essential: 0.5,
    balanced: 0.65,
    deep_cuts: 0.75,
  };
  const ratio = ratioByMode[mode] || ratioByMode.balanced;
  return Math.min(desiredCount, Math.max(3, Math.floor(desiredCount * ratio)));
}

function getModeUniqueDecadeTarget(mode: PlaylistCurationMode, desiredCount: number): number {
  const targetByMode: Record<PlaylistCurationMode, number> = {
    essential: 2,
    balanced: 3,
    deep_cuts: 4,
  };
  const target = targetByMode[mode] || targetByMode.balanced;
  return Math.min(desiredCount, Math.max(1, target));
}

function getModeRankingWindowSize(mode: PlaylistCurationMode): number {
  if (mode === 'essential') return 16;
  if (mode === 'balanced') return 22;
  return MAX_PLAYLIST_TRACKS;
}

function buildDiverseRankingWindow(
  rankedTracks: Track[],
  mode: PlaylistCurationMode,
  windowSize: number
): Track[] {
  if (rankedTracks.length <= windowSize) return rankedTracks;

  const perArtistCapByMode: Record<PlaylistCurationMode, number> = {
    essential: 4,
    balanced: 3,
    deep_cuts: 2,
  };
  const perArtistCap = perArtistCapByMode[mode] || perArtistCapByMode.balanced;

  const selected: Track[] = [];
  const selectedKeys = new Set<string>();
  const artistCounts = new Map<string, number>();

  const trackKey = (track: Track): string => `${normalize(track.artist)}::${normalize(track.song)}`;

  for (const track of rankedTracks) {
    if (selected.length >= windowSize) break;
    const key = trackKey(track);
    if (!key || selectedKeys.has(key)) continue;

    const artistKey = normalize(track.artist);
    if (!artistKey) continue;
    const count = artistCounts.get(artistKey) || 0;
    if (count >= perArtistCap) continue;

    selected.push(track);
    selectedKeys.add(key);
    artistCounts.set(artistKey, count + 1);
  }

  if (selected.length < windowSize) {
    for (const track of rankedTracks) {
      if (selected.length >= windowSize) break;
      const key = trackKey(track);
      if (!key || selectedKeys.has(key)) continue;
      selected.push(track);
      selectedKeys.add(key);
    }
  }

  return selected;
}

function getMaxTracksPerArtist(tracks: Track[]): number {
  const counts = new Map<string, number>();
  let max = 0;
  for (const track of tracks) {
    const key = normalize(track.artist);
    if (!key) continue;
    const next = (counts.get(key) || 0) + 1;
    counts.set(key, next);
    if (next > max) max = next;
  }
  return max;
}

function composeCreditTracksByMode(
  tracks: Track[],
  mode: PlaylistCurationMode,
  maxTracks: number
): Track[] {
  if (tracks.length <= 1) return tracks;

  const desiredCount = Math.min(maxTracks, tracks.length);
  const selected: Track[] = [];
  const selectedKeys = new Set<string>();
  const artistCounts = new Map<string, number>();
  const decadeCounts = new Map<number, number>();

  const modeCaps: Record<PlaylistCurationMode, { artistCapPrimary: number; decadeCapPrimary: number }> = {
    essential: { artistCapPrimary: 3, decadeCapPrimary: 6 },
    balanced: { artistCapPrimary: 2, decadeCapPrimary: 4 },
    deep_cuts: { artistCapPrimary: 2, decadeCapPrimary: 3 },
  };
  const hardArtistCapByMode: Record<PlaylistCurationMode, number> = {
    essential: 4,
    balanced: 3,
    deep_cuts: 2,
  };
  const caps = modeCaps[mode] || modeCaps.balanced;
  const hardArtistCap = hardArtistCapByMode[mode] || hardArtistCapByMode.balanced;
  const uniqueArtistTarget = getModeUniqueArtistTarget(mode, desiredCount);
  const uniqueDecadeTarget = getModeUniqueDecadeTarget(mode, desiredCount);

  const trackKey = (track: Track): string => `${normalize(track.artist)}::${normalize(track.song)}`;
  const canUseTrack = (track: Track, artistCap: number, decadeCap: number): boolean => {
    const key = trackKey(track);
    if (!key || selectedKeys.has(key)) return false;

    const artistKey = normalize(track.artist);
    if (!artistKey) return false;
    const artistCount = artistCounts.get(artistKey) || 0;
    if (artistCount >= artistCap) return false;

    const decade = getTrackDecade(track);
    if (decade !== null) {
      const decadeCount = decadeCounts.get(decade) || 0;
      if (decadeCount >= decadeCap) return false;
    }

    return true;
  };

  const addTrack = (track: Track): void => {
    const key = trackKey(track);
    if (!key || selectedKeys.has(key)) return;

    selected.push(track);
    selectedKeys.add(key);

    const artistKey = normalize(track.artist);
    if (artistKey) {
      artistCounts.set(artistKey, (artistCounts.get(artistKey) || 0) + 1);
    }

    const decade = getTrackDecade(track);
    if (decade !== null) {
      decadeCounts.set(decade, (decadeCounts.get(decade) || 0) + 1);
    }
  };

  const addPass = (artistCap: number, decadeCap: number): void => {
    for (const track of tracks) {
      if (selected.length >= desiredCount) break;
      if (!canUseTrack(track, artistCap, decadeCap)) continue;
      addTrack(track);
    }
  };

  if (uniqueArtistTarget > 1) {
    for (const track of tracks) {
      if (selected.length >= desiredCount) break;
      if (artistCounts.size >= uniqueArtistTarget) break;
      const artistKey = normalize(track.artist);
      if (!artistKey || artistCounts.has(artistKey)) continue;
      if (!canUseTrack(track, 1, caps.decadeCapPrimary + 1)) continue;
      addTrack(track);
    }
  }

  if (uniqueDecadeTarget > 1) {
    for (const track of tracks) {
      if (selected.length >= desiredCount) break;
      if (decadeCounts.size >= uniqueDecadeTarget) break;
      const decade = getTrackDecade(track);
      if (decade === null || decadeCounts.has(decade)) continue;
      if (!canUseTrack(track, caps.artistCapPrimary + 1, caps.decadeCapPrimary + 1)) continue;
      addTrack(track);
    }
  }

  addPass(caps.artistCapPrimary, caps.decadeCapPrimary);
  if (selected.length < desiredCount) addPass(caps.artistCapPrimary + 1, caps.decadeCapPrimary + 2);
  if (selected.length < desiredCount) addPass(caps.artistCapPrimary + 2, caps.decadeCapPrimary + 4);

  if (selected.length < desiredCount) {
    for (const track of tracks) {
      if (selected.length >= desiredCount) break;
      if (!canUseTrack(track, hardArtistCap, caps.decadeCapPrimary + 6)) continue;
      addTrack(track);
    }
  }

  if (selected.length < desiredCount) {
    for (const track of tracks) {
      if (selected.length >= desiredCount) break;
      addTrack(track);
    }
  }

  return selected.length > 0 ? selected.slice(0, desiredCount) : tracks.slice(0, desiredCount);
}

function applyGeneralArtistDiversity(tracks: Track[]): Track[] {
  if (tracks.length <= 1) return tracks;

  const desiredCount = Math.min(MAX_PLAYLIST_TRACKS, tracks.length);
  const chosen: Track[] = [];
  const chosenKeys = new Set<string>();
  const artistCounts = new Map<string, number>();
  const albumCounts = new Map<string, number>();

  const getTrackKey = (track: Track): string => `${normalize(track.artist)}::${normalize(track.song)}`;
  const getAlbumKey = (track: Track): string => normalize(track.album_image_url || '');

  const addWithCaps = (artistCap: number, albumCap: number): void => {
    for (const track of tracks) {
      if (chosen.length >= desiredCount) break;

      const trackKey = getTrackKey(track);
      if (!trackKey || chosenKeys.has(trackKey)) continue;

      const artistKey = normalize(track.artist);
      if (!artistKey) continue;

      const artistCount = artistCounts.get(artistKey) || 0;
      if (artistCount >= artistCap) continue;

      const albumKey = getAlbumKey(track);
      if (albumKey) {
        const albumCount = albumCounts.get(albumKey) || 0;
        if (albumCount >= albumCap) continue;
      }

      chosen.push(track);
      chosenKeys.add(trackKey);
      artistCounts.set(artistKey, artistCount + 1);
      if (albumKey) {
        albumCounts.set(albumKey, (albumCounts.get(albumKey) || 0) + 1);
      }
    }
  };

  addWithCaps(1, 1);
  if (chosen.length < desiredCount) addWithCaps(2, 2);
  if (chosen.length < desiredCount) addWithCaps(3, 3);

  return chosen.length > 0 ? chosen : tracks;
}

function staggerArtistsForFlow(tracks: Track[]): Track[] {
  if (tracks.length < 3) return tracks;

  const buckets = new Map<string, Track[]>();
  const artistOrder: string[] = [];
  for (const track of tracks) {
    const artistKey = normalize(track.artist);
    if (!artistKey) continue;
    if (!buckets.has(artistKey)) {
      buckets.set(artistKey, []);
      artistOrder.push(artistKey);
    }
    buckets.get(artistKey)?.push(track);
  }

  if (artistOrder.length < 2) return tracks;

  const result: Track[] = [];
  let lastArtist: string | null = null;
  let lastAlbum: string | null = null;

  while (result.length < tracks.length) {
    let selectedArtist: string | null = null;
    let selectedCount = -1;
    let selectedAvoidedAlbum = false;

    for (const artistKey of artistOrder) {
      const queue = buckets.get(artistKey);
      const remaining = queue?.length || 0;
      if (remaining <= 0) continue;
      if (artistKey === lastArtist) continue;
      const nextTrack = queue?.[0];
      const nextAlbum = normalize(nextTrack?.album_image_url || '');
      const avoidsAlbumRepeat = !lastAlbum || !nextAlbum || nextAlbum !== lastAlbum;
      if (
        remaining > selectedCount
        || (remaining === selectedCount && avoidsAlbumRepeat && !selectedAvoidedAlbum)
      ) {
        selectedArtist = artistKey;
        selectedCount = remaining;
        selectedAvoidedAlbum = avoidsAlbumRepeat;
      }
    }

    if (!selectedArtist) {
      for (const artistKey of artistOrder) {
        const queue = buckets.get(artistKey);
        const remaining = queue?.length || 0;
        if (remaining <= 0) continue;
        if (remaining > selectedCount) {
          selectedArtist = artistKey;
          selectedCount = remaining;
        }
      }
    }

    if (!selectedArtist) break;

    const nextTrack = buckets.get(selectedArtist)?.shift();
    if (!nextTrack) break;
    result.push(nextTrack);
    lastArtist = selectedArtist;
    const currentAlbum = normalize(nextTrack.album_image_url || '');
    lastAlbum = currentAlbum || null;
  }

  return result.length === tracks.length ? result : tracks;
}

function promptAllowsCreditVariants(prompt: string): boolean {
  const text = normalize(prompt);
  if (!text) return false;
  return /\bremix(?:es)?\b|\bmix(?:es)?\b|\brework(?:s)?\b|\bdub(?:s)?\b|\bedit(?:s)?\b|\bversion(?:s)?\b/.test(text);
}

function detectPlaylistCurationMode(prompt: string): { mode: PlaylistCurationMode; inferredFromPrompt: boolean } {
  const text = normalize(prompt);
  if (!text) {
    return { mode: 'balanced', inferredFromPrompt: false };
  }

  const deepCutsIntent = /\bdeep\s*cuts?\b|\bunknown\b|\bobscure\b|\bunderrated\b|\bhidden\s*gems?\b|\bnon[-\s]?hits?\b|\bb[-\s]?sides?\b/.test(text);
  if (deepCutsIntent) {
    return { mode: 'deep_cuts', inferredFromPrompt: true };
  }

  const essentialIntent = /\bessential\b|\bdefinitive\b|\bbest\b|\bgreatest\b|\bmost\s+iconic\b|\bbest\s+known\b|\bsignature\b/.test(text);
  if (essentialIntent) {
    return { mode: 'essential', inferredFromPrompt: true };
  }

  return { mode: 'balanced', inferredFromPrompt: false };
}

function isVariantTrackTitle(title: string): boolean {
  const text = normalize(title);
  if (!text) return false;
  return /\bremix\b|\bmix\b|\bedit\b|\bdub\b|\brework\b|\bversion\b|\balternative\b|\balternate\b|\bextended\b|\bclub\b|\bacappella\b|\ba cappella\b|\binstrumental\b|\bremaster\b/.test(text);
}

function buildBaseTrackTitleKey(title: string): string {
  return normalize(title)
    .replace(/\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/\s+-\s+.*$/g, ' ')
    .replace(/\b(remix|mix|edit|dub|rework|version|alternative|alternate|extended|club|acappella|a cappella|instrumental|remaster)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function preferCanonicalCreditTrackVersions(
  tracks: Track[],
  prompt: string,
  minimumTracks: number
): Track[] {
  if (tracks.length <= 1) return tracks;

  const allowVariants = promptAllowsCreditVariants(prompt);
  const deduped: Track[] = [];
  const dedupedVariantFlags: boolean[] = [];
  const indexByKey = new Map<string, number>();

  for (const track of tracks) {
    const artistKey = normalize(track.artist);
    const baseTitleKey = buildBaseTrackTitleKey(track.song);
    const trackKey = `${artistKey}::${baseTitleKey || normalize(track.song)}`;
    const isVariant = isVariantTrackTitle(track.song);

    const existingIndex = indexByKey.get(trackKey);
    if (existingIndex === undefined) {
      indexByKey.set(trackKey, deduped.length);
      deduped.push(track);
      dedupedVariantFlags.push(isVariant);
      continue;
    }

    if (dedupedVariantFlags[existingIndex] && !isVariant) {
      deduped[existingIndex] = track;
      dedupedVariantFlags[existingIndex] = false;
    }
  }

  if (allowVariants) return deduped;

  const withoutVariants = deduped.filter((track) => !isVariantTrackTitle(track.song));
  if (withoutVariants.length >= Math.min(minimumTracks, deduped.length)) {
    return withoutVariants;
  }

  return deduped;
}

function refineCreditTrackReasons(tracks: Track[], constraint: PromptConstraint | null): Track[] {
  if (!constraint || constraint.kind !== 'credit') return tracks;

  const creditName = (constraint.value || '').trim();
  const roleLabel = (constraint.creditRole || 'credit').replace(/_/g, ' ').trim();
  if (!creditName || !roleLabel) return tracks;

  const genericEvidenceReason = /^verified\s+.+\s+(?:credit(?:\s+evidence)?|truth\s+claim(?:\s+from\s+.+)?)\s+for\s+.+$/i;

  const rolePhrases: Record<string, string[]> = {
    engineer: [
      'shows a clear emphasis on mix depth and sonic detail',
      'balances impact and clarity with a polished studio feel',
      'highlights controlled dynamics and textural precision',
      'captures a focused blend of weight, space, and definition',
    ],
    producer: [
      'reflects a distinct production vision and arrangement identity',
      'shows consistent direction in tone, pacing, and structure',
      'captures a cohesive production style across performance and sound',
      'leans into a recognizable balance of hooks, mood, and craft',
    ],
    arranger: [
      'stands out for intentional arrangement and sectional movement',
      'uses structure and instrumentation to shape a clear narrative arc',
      'shows careful layering choices that guide momentum and tension',
      'balances motif development with strong arrangement pacing',
    ],
    session_musician: [
      'features instrumental work that strongly supports the track identity',
      'shows performance details that add character without crowding the song',
      'highlights supportive musicianship that sharpens groove and feel',
      'captures expressive playing that reinforces the song structure',
    ],
    cover_designer: [
      'sits within the same visual era and art-direction language',
      'matches the design sensibility associated with this body of work',
      'aligns with a recognizable sleeve aesthetic and presentation style',
      'fits the visual identity connected to this release context',
    ],
    art_director: [
      'fits a coherent visual direction tied to the credited creative lead',
      'reflects a consistent art-direction approach in release presentation',
      'aligns with the broader aesthetic framing of this catalog period',
      'matches the visual storytelling style seen across related releases',
    ],
    photographer: [
      'aligns with the photographic mood associated with the credited work',
      'fits the visual tone and era context linked to this credit',
      'matches the image language seen across related release materials',
      'reflects a consistent visual atmosphere connected to this catalog',
    ],
    design_studio: [
      'fits a recognizable design-system style for related release artwork',
      'aligns with a coherent visual grammar across connected releases',
      'matches the presentation language associated with this design credit',
      'reflects consistent graphic direction and release framing',
    ],
  };

  const defaultPhrases = [
    'fits the same creative context and release ecosystem',
    'aligns with the credited relationship requested in this prompt',
    'matches the catalog context connected to this credit',
    'supports the same verified credit relationship in scope',
  ];

  const roleKey = (constraint.creditRole || '').trim().toLowerCase();
  const stylisticPhrases = rolePhrases[roleKey] || defaultPhrases;

  const roleTonePhrases: Record<string, string[]> = {
    engineer: [
      'supports an evidence-first engineering narrative',
      'stays aligned with the verified engineering constraint',
      'reinforces a precision-focused studio context',
      'keeps the focus on technical execution and clarity',
    ],
    producer: [
      'supports an evidence-first production narrative',
      'stays aligned with the verified production constraint',
      'reinforces the curated producer-focused scope',
      'keeps the focus on production craft and direction',
    ],
    arranger: [
      'supports an evidence-first arrangement narrative',
      'stays aligned with the verified arrangement constraint',
      'reinforces the curated arranger-focused scope',
      'keeps the focus on arrangement craft',
    ],
  };

  const defaultTonePhrases = [
    'supports an evidence-first credit narrative',
    'stays aligned with the verified credit constraint',
    'reinforces the curated credit-focused scope',
    'balances evidence clarity with musical cohesion',
  ];

  const tonePhrases = roleTonePhrases[roleKey] || defaultTonePhrases;

  const hashTrackKey = (artist: string, song: string): number => {
    const raw = `${normalize(artist)}::${normalize(song)}`;
    let hash = 0;
    for (let i = 0; i < raw.length; i += 1) {
      hash = (hash * 31 + raw.charCodeAt(i)) >>> 0;
    }
    return hash;
  };

  const usedReasons = new Set<string>();
  const artistCounts = new Map<string, number>();
  for (const track of tracks) {
    const key = normalize(track.artist);
    if (!key) continue;
    artistCounts.set(key, (artistCounts.get(key) || 0) + 1);
  }

  const catalogPhrases = [
    `highlights a verified ${roleLabel} relationship within this artist catalog`,
    `sits inside the same verified ${roleLabel} context for this artist`,
    `reinforces the verified ${roleLabel} thread across this artist's recordings`,
    `fits the same verified ${roleLabel} scope for this artist body of work`,
  ];

  return tracks.map((track, index) => {
    const reason = (track.reason || '').trim();
    if (!genericEvidenceReason.test(reason)) return track;

    const hash = hashTrackKey(track.artist, track.song);
    const style = stylisticPhrases[(hash + index) % stylisticPhrases.length];
    const tone = tonePhrases[(hash + index * 3) % tonePhrases.length];
    const artistKey = normalize(track.artist);
    const artistCount = artistCounts.get(artistKey) || 0;
    const catalogClause = artistCount >= 4
      ? ` It ${catalogPhrases[(hash + index * 5) % catalogPhrases.length]}.`
      : '';

    const connectors = ['and', 'while it', 'with emphasis on', 'and it'];
    let nextReason = '';
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const styleVariant = stylisticPhrases[(hash + index + attempt) % stylisticPhrases.length];
      const toneVariant = tonePhrases[(hash + index * 3 + attempt) % tonePhrases.length];
      const connector = connectors[(hash + attempt + index) % connectors.length];
      const candidate = `Verified ${roleLabel} credit for ${creditName}; this track ${styleVariant} ${connector} ${toneVariant}.${catalogClause}`;
      if (!usedReasons.has(candidate)) {
        nextReason = candidate;
        break;
      }
      nextReason = candidate;
    }

    usedReasons.add(nextReason);

    return {
      ...track,
      reason: nextReason,
    };
  });
}

function normalizeReasonForComparison(reason: string): string {
  return reason
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function refineGeneralTrackReasons(tracks: Track[]): Track[] {
  if (tracks.length < 2) return tracks;

  const reasonCounts = new Map<string, number>();
  for (const track of tracks) {
    const normalized = normalizeReasonForComparison(track.reason || '');
    if (!normalized) continue;
    reasonCounts.set(normalized, (reasonCounts.get(normalized) || 0) + 1);
  }

  const variantPhrases = [
    'keeps the playlist moving with a distinct tonal angle',
    'adds contrast while staying aligned with the request',
    'reinforces the core vibe with a different texture',
    'supports flow by widening the sonic perspective',
  ];

  return tracks.map((track, index) => {
    const reason = (track.reason || '').trim();
    const normalized = normalizeReasonForComparison(reason);
    const duplicateCount = normalized ? (reasonCounts.get(normalized) || 0) : 0;
    if (duplicateCount <= 1) {
      return track;
    }

    const hashSeed = `${normalize(track.artist)}::${normalize(track.song)}::${index}`;
    let hash = 0;
    for (let i = 0; i < hashSeed.length; i += 1) {
      hash = (hash * 33 + hashSeed.charCodeAt(i)) >>> 0;
    }
    const phrase = variantPhrases[hash % variantPhrases.length];

    return {
      ...track,
      reason: `${track.song} by ${track.artist} ${phrase}.`,
    };
  });
}

function getMinimumVerifiedTracksForCreditConstraint(constraint: PromptConstraint | null): number {
  if (!constraint || constraint.kind !== 'credit') return 8;

  const creditName = (constraint.value || '').trim();
  const creditRole = (constraint.creditRole || '').trim();
  if (!creditName || !creditRole) return 8;

  const evidenceCount = getCreditEvidenceTrackCandidates(constraint, 500).length;
  if (evidenceCount >= 8) return 8;
  if (evidenceCount >= 4) return evidenceCount;
  return 8;
}

function getCombinedCreditEvidenceCount(creditName: string, creditRole: string): number {
  const syntheticConstraint: PromptConstraint = {
    kind: 'credit',
    value: creditName,
    associatedArtists: new Set<string>(),
    strength: 'strict',
    creditRole,
  };
  return getCreditEvidenceTrackCandidates(syntheticConstraint, 500).length;
}

function countUniqueTrackArtists(tracks: Track[]): number {
  const artists = new Set<string>();
  for (const track of tracks) {
    const key = normalize(track.artist);
    if (!key) continue;
    artists.add(key);
  }
  return artists.size;
}

async function generateAdditionalTrackCandidates(prompt: string, count: number): Promise<Track[]> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `${SYSTEM_PROMPT}\n\nUser prompt: ${prompt}\n\nReturn ONLY a JSON array with ${count} additional track candidates in this format:\n[{"artist":"","song":"","reason":""}]`
    ));

    let text = result.response.text().trim();
    if (text.startsWith('```json')) text = text.slice(7);
    else if (text.startsWith('```')) text = text.slice(3);
    if (text.endsWith('```')) text = text.slice(0, -3);

    const parsed = JSON.parse(text.trim());
    if (!Array.isArray(parsed)) return [];

    return parsed
      .filter((item): item is Track => {
        return !!item
          && typeof item === 'object'
          && typeof item.artist === 'string'
          && typeof item.song === 'string'
          && typeof item.reason === 'string'
          && item.artist.trim().length > 0
          && item.song.trim().length > 0
          && item.reason.trim().length > 0;
      })
      .slice(0, Math.max(0, count));
  } catch (e) {
    console.error('[Gemini] Additional candidate generation failed:', e);
    return [];
  }
}

async function repairPlaylistJsonResponse(rawResponse: string): Promise<string | null> {
  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const result = await callGeminiWithRetry(() => model.generateContent(
      `Convert the following near-JSON into valid strict JSON for this exact schema:
{
  "title": "Playlist title",
  "description": "2-3 sentence description in English",
  "tracks": [
    {
      "artist": "Artist name",
      "song": "Song title",
      "reason": "Why this song was chosen"
    }
  ]
}

Rules:
- Keep the same content as much as possible.
- Fix only JSON syntax/escaping/quoting issues.
- Return ONLY valid JSON, no markdown.

Input:
${rawResponse}`
    ));

    return result.response.text().trim();
  } catch {
    return null;
  }
}

const RESPONSE_FORMAT_INSTRUCTIONS = `The response MUST be valid JSON in this exact format:

{
  "title": "Playlist title",
  "description": "2-3 sentence description in English",
  "tracks": [
    {
      "artist": "Artist name",
      "song": "Song title",
      "reason": "Why this song was chosen"
    }
  ]
}

IMPORTANT: Return ONLY the JSON, no markdown formatting, no explanations, no text before or after.`;

const SYSTEM_PROMPT = `You are a knowledgeable and passionate music curator. Your task is to create a unique, curated playlist based on the user's request.

Requirements:
- The playlist title must be catchy and memorable
- The description must be in English and explain the theme
- Include 10-15 tracks
- Each track must have: artist name, song title, and a short explanation (1-2 sentences) of WHY this song was chosen
- Always use the full primary artist name (never shortened first-name-only forms).
- If a track has guest artists, keep the main artist in 'artist' and put guests in the song title as '(feat. Guest Name)'.
- Make the playlist feel nerdy, curated, and musically interesting - avoid generic choices
- Show your music knowledge by selecting interesting artists and songs
- Safety rule: never invent factual credit claims.
- Do not claim producers, studios, session musicians, cover designers, photographers, or other credits unless explicitly verified in trusted app metadata.
- This app does not provide trusted credits metadata, so avoid those factual credit claims.
- Reasons should focus on stylistic fit, era fit, thematic fit, cultural fit, and sonic character.
- If uncertain, avoid the claim rather than guessing.
- Treat specific named entities in the user prompt as strict constraints (e.g. studios, producers, venues, scenes, artist relationships).
- Do not reinterpret named entities metaphorically or as a loose vibe.
- If the prompt says tracks should be connected to a specific studio/producer/venue/scene/relationship, include only tracks that fit that relation with confidence.
- If confidence is low for a track, exclude it rather than guessing.
- For constraint-heavy prompts, prioritize factual relation over vibe.

${RESPONSE_FORMAT_INSTRUCTIONS}`;

const CREDIT_SYSTEM_PROMPT = `You are a careful music curator for credit-focused requests.

Requirements:
- Build a playlist that follows the user's credit constraint (who + role) as closely as possible.
- Include 10-15 tracks.
- Return the exact same playlist JSON schema as the normal mode.
- Be conservative: if unsure that a track matches the credit constraint, exclude it.
- Do not invent factual claims.
- Keep reasons short and focused on why the track fits the requested credit relationship.

${RESPONSE_FORMAT_INSTRUCTIONS}`;

export async function generatePlaylist(userPrompt: string): Promise<PlaylistResponse> {
  // Translate prompt to English for consistent storage and display
  console.log(`[Gemini] Translating prompt to English: "${userPrompt}"`);
  const translatedPrompt = await translateToEnglish(userPrompt);
  console.log(`[Gemini] Translated prompt: "${translatedPrompt}"`);

  const routeDecision = resolvePromptRoute(translatedPrompt);
  const curationMode = detectPlaylistCurationMode(translatedPrompt);
  const routeCreditSuffix = routeDecision.credit
    ? ` credit_role=${routeDecision.credit.role}${routeDecision.credit.name ? ` credit_name="${routeDecision.credit.name}"` : ''}`
    : '';
  console.log(
    `[routing] intent=${routeDecision.intent} mode=${routeDecision.mode} confidence=${routeDecision.confidence} reason_code=${routeDecision.reasonCode} reason="${routeDecision.reason}"${routeCreditSuffix}`
  );
  console.log(`[curation] mode=${curationMode.mode} inferred=${curationMode.inferredFromPrompt}`);
  recordRoutingCall(routeDecision);

  const truth: TruthDetails = {};
  const detectedCreditPrompt = detectCreditPrompt(translatedPrompt);
  if (detectedCreditPrompt?.membersOfBand) {
    const truthSync = await syncTruthMembershipForBandName(detectedCreditPrompt.name);
    truth.membership_sync = {
      band: detectedCreditPrompt.name,
      attempted: truthSync.attempted,
      imported: truthSync.imported,
      skipped_reason: truthSync.skippedReason,
    };
    if (truthSync.attempted) {
      console.log(`[truth] membership sync band="${detectedCreditPrompt.name}" imported=${truthSync.imported}${truthSync.skippedReason ? ` reason=${truthSync.skippedReason}` : ''}`);
    } else if (truthSync.skippedReason) {
      console.log(`[truth] membership sync skipped reason=${truthSync.skippedReason}`);
    }
  }

  // Check cache using translated prompt (English)
  const isCreditPrompt = Boolean(detectedCreditPrompt);
  const cacheKey = normalizePromptForCache(translatedPrompt);
  const cached = getPlaylistByCacheKey(cacheKey);
  
  if (cached && !isCreditPrompt) {
    const cachedTracks = JSON.parse(cached.tracks).map((t: Track) => ({
      ...t,
      spotify_url: t.spotify_url || null,
      album_image_url: t.album_image_url || null,
      release_year: typeof t.release_year === 'number' ? t.release_year : null,
    }));
    const sortedCachedTracks = await sortTracksChronologicallyIfNeeded(translatedPrompt, cachedTracks);

    recordRoutingSuccess(true);
    return {
      playlist: {
        title: cached.title,
        description: cached.description,
        tracks: sortedCachedTracks,
        tags: parseStringArray(cached.tags),
        countries: (() => {
          const fromNew = parseStringArray(cached.countries);
          if (fromNew.length > 0) return fromNew;
          return getLegacyLocationFallback(cached.places, cached.place).countries;
        })(),
        cities: (() => {
          const fromNew = parseStringArray(cached.cities);
          if (fromNew.length > 0) return fromNew;
          return getLegacyLocationFallback(cached.places, cached.place).cities;
        })(),
        studios: (() => {
          const fromNew = parseStringArray(cached.studios);
          if (fromNew.length > 0) return fromNew;
          return getLegacyLocationFallback(cached.places, cached.place).studios;
        })(),
        venues: (() => {
          const fromNew = parseStringArray(cached.venues);
          if (fromNew.length > 0) return fromNew;
          return getLegacyLocationFallback(cached.places, cached.place).venues;
        })(),
        scenes: (() => {
          const fromNew = parseStringArray(cached.scenes);
          if (fromNew.length > 0) return fromNew;
          return cached.scene ? [cached.scene] : [];
        })(),
        influences: (() => {
          if (!cached.influences) return [];
          try {
            const parsed = JSON.parse(cached.influences);
            if (!Array.isArray(parsed)) return [];
            return parsed
              .filter((item) => item && typeof item === 'object' && typeof item.from === 'string' && typeof item.to === 'string')
              .map((item) => ({ from: item.from, to: item.to }));
          } catch {
            return [];
          }
        })(),
        credits: (() => {
          if (!cached.credits) return [];
          try {
            const parsed = JSON.parse(cached.credits);
            if (!Array.isArray(parsed)) return [];
            return parsed
              .filter(
                (item) => item
                  && typeof item === 'object'
                  && typeof item.name === 'string'
                  && typeof item.role === 'string'
                  && ALLOWED_CREDIT_ROLES.has(item.role)
              )
              .map((item) => ({ name: item.name, role: item.role }));
          } catch {
            return [];
          }
        })(),
        equipment: (() => {
          if (!cached.equipment) return [];
          try {
            const parsed = JSON.parse(cached.equipment);
            if (!Array.isArray(parsed)) return [];
            const dedup = new Set<string>();
            return parsed
              .filter(
                (item) => item
                  && typeof item === 'object'
                  && typeof item.name === 'string'
                  && item.name.trim().length > 0
                  && typeof item.category === 'string'
                  && ALLOWED_EQUIPMENT_CATEGORIES.has(item.category)
              )
              .map((item) => ({ name: canonicalizeEquipmentName(item.name), category: item.category }))
              .filter((item) => item.name.length > 0 && !isGenericEquipmentName(item.name))
              .filter((item) => {
                if (!item.name) return false;
                const key = `${item.name.toLowerCase()}::${item.category}`;
                if (dedup.has(key)) return false;
                dedup.add(key);
                return true;
              });
          } catch {
            return [];
          }
        })()
      },
      cached: true,
      truth
    };
  }

  if (cached && isCreditPrompt) {
    console.log('[cache] bypassed cached playlist for credit-focused prompt');
  }

  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
  const constraint = detectPromptConstraint(translatedPrompt);
  const truthFirstCreditMode = Boolean(
    constraint?.kind === 'credit'
      && routeDecision.intent === 'credit'
      && routeDecision.mode === 'truth-first'
  );
  if (truthFirstCreditMode) {
    console.log('[routing] enforcing truth-first candidate selection for credit prompt');
  }
  let graphHintText = '';
  let graphHintSeed: { type: 'artist' | 'equipment' | 'scene' | 'studio' | 'city'; value: string } | null = null;
  let graphHintNeighborArtists: string[] = [];
  let creditPreferredArtistsForCuration: string[] = [];
  let creditPreferredWorksForCuration: Array<{ artist: string; song: string }> = [];

  try {
    graphHintSeed = detectGraphHintSeed(translatedPrompt);
    if (graphHintSeed) {
      const neighbors = getGraphNeighbors(graphHintSeed.type, graphHintSeed.value, 5);
      graphHintText = buildGraphHintText(neighbors);
      graphHintNeighborArtists = neighbors
        .filter((neighbor) => neighbor.nodeType === 'artist')
        .map((neighbor) => neighbor.nodeName)
        .filter((value) => value.trim().length > 0);
      if (constraint?.kind === 'credit' && graphHintNeighborArtists.length > 0) {
        creditPreferredArtistsForCuration = Array.from(new Set(graphHintNeighborArtists));
      }
    }
  } catch {
    graphHintText = '';
    graphHintSeed = null;
    graphHintNeighborArtists = [];
  }

  try {
    if (graphHintSeed) {
      console.log(`[graph-hint] matched seed: ${graphHintSeed.value} (${graphHintSeed.type})`);
    }

    if (graphHintText) {
      const concise = graphHintText.replace(/^Graph hints:\s*/i, '');
      console.log(`[graph-hint] injected hints: ${concise}`);
    } else {
      console.log('[graph-hint] no graph hint injected');
    }
  } catch {
    // Debug logging is best-effort only.
  }

  const generationSystemPrompt = constraint?.kind === 'credit' ? CREDIT_SYSTEM_PROMPT : SYSTEM_PROMPT;
  const historyOrderingSuffix = buildHistoryOrderingSuffix(translatedPrompt);
  const generationUserPrompt = constraint?.kind === 'credit'
    ? `${translatedPrompt}\n\nCredit role: ${constraint.creditRole || ''}\nCredit name: ${constraint.value || ''}`
    : translatedPrompt;
  const generationUserPromptWithOrdering = historyOrderingSuffix
    ? `${generationUserPrompt}\n\n${historyOrderingSuffix}`
    : generationUserPrompt;
  const generationPromptWithHints = graphHintText
    ? `${generationUserPromptWithOrdering}\n\n${graphHintText}`
    : generationUserPromptWithOrdering;

  const result = await callGeminiWithRetry(() => model.generateContent([
    generationSystemPrompt,
    generationPromptWithHints
  ]));

  const text = result.response.text();

  let playlist: Playlist;
  try {
    playlist = parsePlaylistResponse(text);
  } catch (error) {
    console.error('[Gemini] Failed to parse playlist response. Raw response:', text);
    const repairedText = await repairPlaylistJsonResponse(text);
    if (!repairedText) {
      throw error;
    }

    try {
      playlist = parsePlaylistResponse(repairedText);
      console.log('[Gemini] Repaired malformed playlist JSON response.');
    } catch {
      throw error;
    }
  }

  let truthCreditCandidates = constraint?.kind === 'credit'
    ? getTruthCreditCandidates((constraint.value || '').trim(), (constraint.creditRole || '').trim(), 220)
    : [];
  const truthCreditSeedTracks: Track[] = truthCreditCandidates.map((row) => ({
    artist: row.artist,
    song: row.title,
    reason: `Verified ${(constraint?.creditRole || 'credit').replace(/_/g, ' ')} truth claim from ${row.source} for ${(constraint?.value || '').trim()}`,
  }));

  let creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
  const creditEvidenceSeedTracks = bootstrapCreditEvidenceTracks(constraint);
  let evidenceFirstCreditMode = constraint?.kind === 'credit' && (truthFirstCreditMode || creditEvidenceSeedTracks.length > 0);
  let candidateTracks = evidenceFirstCreditMode
    ? dedupeTracks([...truthCreditSeedTracks, ...creditEvidenceSeedTracks])
    : dedupeTracks([...truthCreditSeedTracks, ...creditEvidenceSeedTracks, ...playlist.tracks]);
  if (constraint?.strength === 'skip') {
    console.log(`[verification] skipped strict verification for mode: "${constraint.kind}"`);
  }

  let verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);

  const verification: VerificationDetails | undefined = constraint?.kind === 'credit'
    ? {
        evidence_before: getCombinedCreditEvidenceCount((constraint.value || '').trim(), (constraint.creditRole || '').trim()),
        evidence_after: 0,
        used_auto_backfill: false,
        backfill_inserted: 0,
      }
    : undefined;

  const MIN_VERIFIED_TRACKS = getMinimumVerifiedTracksForCreditConstraint(constraint);
  const MAX_EXTRA_ATTEMPTS = 2;

  if (constraint && constraint.strength !== 'skip' && verifiedTracks.length < MIN_VERIFIED_TRACKS) {
    if (constraint.kind === 'credit') {
      console.log('[verification] skipped Gemini candidate expansion for credit prompt; using evidence/truth-first flow');
    } else {
      for (let attempt = 0; attempt < MAX_EXTRA_ATTEMPTS; attempt += 1) {
        const needed = MIN_VERIFIED_TRACKS - verifiedTracks.length;
        if (needed <= 0) break;

        const extra = await generateAdditionalTrackCandidates(translatedPrompt, needed + 4);
        if (extra.length === 0) break;

        candidateTracks = dedupeTracks([...candidateTracks, ...extra]);

        verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
      }
    }
  }

  if (
    constraint?.kind === 'credit'
    && verifiedTracks.length < MIN_VERIFIED_TRACKS
  ) {
    const creditName = (constraint.value || '').trim();
    const creditRole = (constraint.creditRole || '').trim().toLowerCase();
    const creditRoleSearchTerm = getCreditRoleSearchTerm(creditRole);
    const isCoreCreditRole = CORE_CREDIT_TRUTH_ROLES.has(creditRole);
    const initialDiscogsBackfillLimit = isCoreCreditRole ? 100 : 12;

    const truthCreditEnabled = process.env.ENABLE_TRUTH_CREDIT_BACKFILL !== 'false';
    const canUseTruthCredit = creditName.length > 0 && SUPPORTED_TRUTH_CREDIT_ROLES.has(creditRole);
    if (truthCreditEnabled && canUseTruthCredit) {
      const truthBackfillResult = await backfillTruthCreditsFromDiscogs({
        creditName,
        creditRole,
        query: `${creditName} ${creditRoleSearchTerm}`,
        limit: initialDiscogsBackfillLimit,
      });
      truth.credit_sync = {
        name: creditName,
        role: creditRole,
        source: 'discogs',
        attempted: truthBackfillResult.attempted,
        imported: truthBackfillResult.imported,
        skipped_reason: truthBackfillResult.skippedReason,
      };

      if (truthBackfillResult.attempted) {
        recordRoutingBackfill('discogs', truthBackfillResult.imported > 0);
        console.log(`[truth] credit sync name="${creditName}" role="${creditRole}" imported=${truthBackfillResult.imported}${truthBackfillResult.skippedReason ? ` reason=${truthBackfillResult.skippedReason}` : ''}`);
      } else if (truthBackfillResult.skippedReason) {
        console.log(`[truth] credit sync skipped reason=${truthBackfillResult.skippedReason}`);
      }

      if (truthBackfillResult.imported > 0) {
        truthCreditCandidates = getTruthCreditCandidates(creditName, creditRole, 220);
        creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
        const refreshedCreditEvidenceTracks = bootstrapCreditEvidenceTracks(constraint);
        evidenceFirstCreditMode = truthFirstCreditMode || refreshedCreditEvidenceTracks.length > 0;
        const refreshedTruthSeedTracks: Track[] = truthCreditCandidates.map((row) => ({
          artist: row.artist,
          song: row.title,
          reason: `Verified ${creditRole.replace(/_/g, ' ')} truth claim from ${row.source} for ${creditName}`,
        }));
        candidateTracks = evidenceFirstCreditMode
          ? dedupeTracks([...refreshedTruthSeedTracks, ...refreshedCreditEvidenceTracks])
          : dedupeTracks([...refreshedTruthSeedTracks, ...refreshedCreditEvidenceTracks, ...candidateTracks]);
        verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
      }

      if (!isCoreCreditRole && verifiedTracks.length < MIN_VERIFIED_TRACKS) {
        const truthRetryResult = await backfillTruthCreditsFromDiscogs({
          creditName,
          creditRole,
          query: creditName,
          limit: 80,
          force: true,
        });
        truth.credit_sync = {
          name: creditName,
          role: creditRole,
          source: 'discogs',
          attempted: truthRetryResult.attempted,
          imported: truthBackfillResult.imported + truthRetryResult.imported,
          skipped_reason: truthRetryResult.skippedReason || truthBackfillResult.skippedReason,
        };

        if (truthRetryResult.attempted) {
          recordRoutingBackfill('discogs', truthRetryResult.imported > 0);
          console.log(`[truth] credit retry name="${creditName}" role="${creditRole}" imported=${truthRetryResult.imported}${truthRetryResult.skippedReason ? ` reason=${truthRetryResult.skippedReason}` : ''}`);
        } else if (truthRetryResult.skippedReason) {
          console.log(`[truth] credit retry skipped reason=${truthRetryResult.skippedReason}`);
        }

        if (truthRetryResult.imported > 0) {
          truthCreditCandidates = getTruthCreditCandidates(creditName, creditRole, 220);
          creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
          const refreshedCreditEvidenceTracks = bootstrapCreditEvidenceTracks(constraint);
          const refreshedTruthSeedTracks: Track[] = truthCreditCandidates.map((row) => ({
            artist: row.artist,
            song: row.title,
            reason: `Verified ${creditRole.replace(/_/g, ' ')} truth claim from ${row.source} for ${creditName}`,
          }));
          candidateTracks = dedupeTracks([...refreshedTruthSeedTracks, ...refreshedCreditEvidenceTracks]);
          verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
        }
      }
    }

    if (verifiedTracks.length < MIN_VERIFIED_TRACKS) {
      const autoBackfillEnabled = process.env.ENABLE_MUSICBRAINZ_AUTO_BACKFILL !== 'false';
      const canUseMusicBrainz = creditName.length > 0 && SUPPORTED_MUSICBRAINZ_CREDIT_ROLES.has(creditRole);

      if (!autoBackfillEnabled && verification) {
        verification.backfill_skipped_reason = 'disabled';
      }

      if (autoBackfillEnabled && canUseMusicBrainz) {
        const backfillWindow = shouldAttemptAutoBackfill(creditName, creditRole);
        if (!backfillWindow.allowed) {
          if (verification) verification.backfill_skipped_reason = backfillWindow.reason || 'cooldown_active';
          console.log(`[musicbrainz] auto backfill skipped: ${backfillWindow.reason || 'cooldown_active'}`);
        } else {
          try {
            const backfillResult = await backfillCreditFromMusicBrainz({
              name: creditName,
              role: creditRole,
              limit: 120,
            });
            recordRoutingBackfill('musicbrainz', backfillResult.insertedEvidence > 0);
            if (verification) {
              verification.used_auto_backfill = true;
              verification.backfill_inserted = backfillResult.insertedEvidence;
            }
            console.log(
              `[musicbrainz] auto backfill name="${backfillResult.name}" role="${backfillResult.role}" inserted_evidence=${backfillResult.insertedEvidence}`
            );

            if (backfillResult.insertedEvidence > 0 || backfillResult.mbCandidates > 0) {
              creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
              const refreshedCreditEvidenceTracks = bootstrapCreditEvidenceTracks(constraint);
              candidateTracks = dedupeTracks([...refreshedCreditEvidenceTracks, ...candidateTracks]);
              verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (verification && !verification.backfill_skipped_reason) {
              verification.backfill_skipped_reason = `error:${message.slice(0, 120)}`;
            }
            console.log(`[musicbrainz] auto backfill skipped: ${message}`);
          }
        }
      } else if (verification && !canUseMusicBrainz) {
        verification.backfill_skipped_reason = canUseTruthCredit
          ? 'discogs_insufficient_evidence'
          : 'unsupported_role';
      }
    }
  }

  if (constraint?.kind === 'credit' && verifiedTracks.length > 0) {
    const creditName = (constraint.value || '').trim();
    const creditRole = (constraint.creditRole || '').trim().toLowerCase();
    const creditRoleSearchTerm = getCreditRoleSearchTerm(creditRole);
    const isCoreCreditRole = CORE_CREDIT_TRUTH_ROLES.has(creditRole);
    let uniqueArtistCount = countUniqueTrackArtists(verifiedTracks);
    const BREADTH_TARGET = 8;
    const truthCreditEnabled = process.env.ENABLE_TRUTH_CREDIT_BACKFILL !== 'false';
    const canUseTruthCredit = creditName.length > 0 && SUPPORTED_TRUTH_CREDIT_ROLES.has(creditRole);
    const autoBackfillEnabled = process.env.ENABLE_MUSICBRAINZ_AUTO_BACKFILL !== 'false';
    const canUseMusicBrainz = creditName.length > 0 && SUPPORTED_MUSICBRAINZ_CREDIT_ROLES.has(creditRole);

    if (uniqueArtistCount < BREADTH_TARGET && truthCreditEnabled && canUseTruthCredit && isCoreCreditRole) {
      try {
        const geminiExpansionArtists = truthFirstCreditMode
          ? []
          : await getCreditExpansionArtistsFromGemini(translatedPrompt, creditName, creditRole);
        const canonicalWorks = truthFirstCreditMode
          ? []
          : await getCreditCanonicalWorksFromGemini(
              translatedPrompt,
              creditName,
              creditRole,
              [...graphHintNeighborArtists, ...geminiExpansionArtists]
            );
        if (truthFirstCreditMode) {
          console.log('[routing] skipped Gemini breadth discovery for truth-first credit mode');
        }
        if (geminiExpansionArtists.length > 0) {
          creditPreferredArtistsForCuration = Array.from(new Set([
            ...creditPreferredArtistsForCuration,
            ...geminiExpansionArtists,
          ]));
        }
        if (canonicalWorks.length > 0) {
          creditPreferredWorksForCuration = canonicalWorks;
        }
        const expansionQueries = new Set<string>();
        expansionQueries.add(`${creditName} ${creditRoleSearchTerm}`);
        expansionQueries.add(creditName);
        for (const artist of graphHintNeighborArtists) {
          if (expansionQueries.size >= 6) break;
          if (normalize(artist) === normalize(creditName)) continue;
          expansionQueries.add(`${creditName} ${artist} ${creditRole}`);
        }
        for (const artist of geminiExpansionArtists) {
          if (expansionQueries.size >= 8) break;
          if (normalize(artist) === normalize(creditName)) continue;
          expansionQueries.add(`${creditName} ${artist} ${creditRole}`);
        }
        for (const work of canonicalWorks) {
          if (expansionQueries.size >= 10) break;
          expansionQueries.add(`${creditName} ${work.artist} ${work.song} ${creditRole}`);
        }

        let totalImported = 0;
        let lastSkippedReason = '';
        let attemptIndex = 0;

        for (const query of expansionQueries) {
          if (uniqueArtistCount >= BREADTH_TARGET) break;
          const truthBreadthSync = await backfillTruthCreditsFromDiscogs({
            creditName,
            creditRole,
            query,
            limit: 120,
            force: attemptIndex > 0,
          });
          attemptIndex += 1;
          totalImported += truthBreadthSync.imported;
          if (truthBreadthSync.skippedReason) {
            lastSkippedReason = truthBreadthSync.skippedReason;
          }

          if (truthBreadthSync.imported > 0) {
            truthCreditCandidates = getTruthCreditCandidates(creditName, creditRole, 220);
            creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
            const refreshedCreditEvidenceTracks = bootstrapCreditEvidenceTracks(constraint);
            const refreshedTruthSeedTracks: Track[] = truthCreditCandidates.map((row) => ({
              artist: row.artist,
              song: row.title,
              reason: `Verified ${creditRole.replace(/_/g, ' ')} truth claim from ${row.source} for ${creditName}`,
            }));
            candidateTracks = dedupeTracks([...refreshedTruthSeedTracks, ...refreshedCreditEvidenceTracks, ...candidateTracks]);
            verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
            uniqueArtistCount = countUniqueTrackArtists(verifiedTracks);
          }
        }

        console.log(
          `[truth] breadth sync name="${creditName}" role="${creditRole}" imported=${totalImported} unique_artists_after=${uniqueArtistCount}${lastSkippedReason ? ` reason=${lastSkippedReason}` : ''}`
        );
        if (attemptIndex > 0) {
          recordRoutingBackfill('discogs', totalImported > 0);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`[truth] breadth sync skipped: ${message}`);
      }
    }

    if (uniqueArtistCount < BREADTH_TARGET && autoBackfillEnabled && canUseMusicBrainz) {
      try {
        const breadthBackfill = await backfillCreditFromMusicBrainz({
          name: creditName,
          role: creditRole,
          limit: 120,
        });
        recordRoutingBackfill('musicbrainz', breadthBackfill.insertedEvidence > 0);
        if (breadthBackfill.insertedEvidence > 0 || breadthBackfill.mbCandidates > 0) {
          creditEvidenceTracks = getCreditEvidenceTrackCandidates(constraint, 220);
          const refreshedCreditEvidenceTracks = bootstrapCreditEvidenceTracks(constraint);
          candidateTracks = dedupeTracks([...refreshedCreditEvidenceTracks, ...candidateTracks]);
          verifiedTracks = filterTracksByConstraint(candidateTracks, constraint, translatedPrompt, creditEvidenceTracks);
        }
        console.log(
          `[musicbrainz] breadth backfill name="${breadthBackfill.name}" role="${breadthBackfill.role}" inserted_evidence=${breadthBackfill.insertedEvidence} unique_artists_before=${uniqueArtistCount}`
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.log(`[musicbrainz] breadth backfill skipped: ${message}`);
      }
    }
  }

  if (
    constraint
    && verifiedTracks.length === 0
    && candidateTracks.length > 0
    && (constraint.kind === 'venue' || constraint.kind === 'scene' || constraint.kind === 'unsupported')
  ) {
    console.log('[verification] no high-confidence matches; falling back to Spotify-matched candidate tracks');
    recordRoutingFallback('verification_no_high_confidence_matches');
    verifiedTracks = candidateTracks;
  }

  if (constraint?.kind === 'credit' && verifiedTracks.length > 0) {
    const retrievedVerifiedTracks = preferCanonicalCreditTrackVersions(verifiedTracks, translatedPrompt, MIN_VERIFIED_TRACKS);

    const prominenceRanking = rankVerifiedCreditTracksByProminence(
      retrievedVerifiedTracks,
      constraint,
      creditPreferredArtistsForCuration,
      creditPreferredWorksForCuration,
      curationMode.mode
    );

    const rankingWindowSize = getModeRankingWindowSize(curationMode.mode);
    const rankingWindowInputCount = prominenceRanking.tracks.length;
    const rankingWindowTracks = buildDiverseRankingWindow(prominenceRanking.tracks, curationMode.mode, rankingWindowSize);
    const rankingWindowApplied = rankingWindowTracks.length < rankingWindowInputCount;

    let composedCreditTracks = await rerankVerifiedCreditTracksWithGemini(
      translatedPrompt,
      constraint,
      rankingWindowTracks
    );
    composedCreditTracks = applyCreditOnlyArtistDiversity(translatedPrompt, composedCreditTracks);
    composedCreditTracks = composeCreditTracksByMode(composedCreditTracks, curationMode.mode, MAX_PLAYLIST_TRACKS);
    composedCreditTracks = refineCreditTrackReasons(composedCreditTracks, constraint);
    verifiedTracks = composedCreditTracks;

    const uniqueDecades = new Set<number>();
    for (const track of composedCreditTracks) {
      const decade = getTrackDecade(track);
      if (decade !== null) uniqueDecades.add(decade);
    }
    const uniqueArtists = countUniqueTrackArtists(composedCreditTracks);
    const uniqueArtistTarget = getModeUniqueArtistTarget(curationMode.mode, composedCreditTracks.length);
    const uniqueDecadeTarget = getModeUniqueDecadeTarget(curationMode.mode, composedCreditTracks.length);
    const selectedTrackTarget = MAX_PLAYLIST_TRACKS;
    const selectedTrackGap = Math.max(0, selectedTrackTarget - composedCreditTracks.length);
    const selectedTrackCoverage = selectedTrackTarget > 0
      ? Math.min(1, composedCreditTracks.length / selectedTrackTarget)
      : 1;
    const selectionRetentionGap = Math.max(0, rankingWindowTracks.length - composedCreditTracks.length);
    const selectionRetentionCoverage = rankingWindowTracks.length > 0
      ? Math.min(1, composedCreditTracks.length / rankingWindowTracks.length)
      : 1;
    const uniqueArtistTargetGap = Math.max(0, uniqueArtistTarget - uniqueArtists);
    const uniqueDecadeTargetGap = Math.max(0, uniqueDecadeTarget - uniqueDecades.size);
    const uniqueArtistTargetCoverage = uniqueArtistTarget > 0
      ? Math.min(1, uniqueArtists / uniqueArtistTarget)
      : 1;
    const uniqueDecadeTargetCoverage = uniqueDecadeTarget > 0
      ? Math.min(1, uniqueDecades.size / uniqueDecadeTarget)
      : 1;
    const targetMissReasons: string[] = [];
    if (selectedTrackGap > 0) targetMissReasons.push('size');
    if (selectionRetentionGap > 0) targetMissReasons.push('retention');
    if (uniqueArtistTargetGap > 0) targetMissReasons.push('artist');
    if (uniqueDecadeTargetGap > 0) targetMissReasons.push('decade');

    truth.curation = {
      mode: curationMode.mode,
      inferred_from_prompt: curationMode.inferredFromPrompt,
      top_score_sample: prominenceRanking.topScoreSample,
      ranking_floor: {
        applied: prominenceRanking.rankingFloor.applied,
        dropped_tracks: prominenceRanking.rankingFloor.droppedTracks,
        floor_score: prominenceRanking.rankingFloor.floorScore,
      },
      ranking_window: {
        applied: rankingWindowApplied,
        input_tracks: rankingWindowInputCount,
        kept_tracks: rankingWindowTracks.length,
        unique_artists: countUniqueTrackArtists(rankingWindowTracks),
        max_tracks_per_artist: getMaxTracksPerArtist(rankingWindowTracks),
      },
      composition: {
        selected_tracks: composedCreditTracks.length,
        selected_track_target: selectedTrackTarget,
        selected_track_target_met: selectedTrackGap === 0,
        selected_track_gap: selectedTrackGap,
        selected_track_coverage: selectedTrackCoverage,
        selection_retention_gap: selectionRetentionGap,
        selection_retention_coverage: selectionRetentionCoverage,
        target_miss_count: targetMissReasons.length,
        target_miss_reasons: targetMissReasons,
        unique_artists: uniqueArtists,
        unique_artist_target: uniqueArtistTarget,
        unique_artist_target_met: uniqueArtistTargetGap === 0,
        unique_artist_target_gap: uniqueArtistTargetGap,
        unique_artist_target_coverage: uniqueArtistTargetCoverage,
        unique_decades: uniqueDecades.size,
        unique_decade_target: uniqueDecadeTarget,
        unique_decade_target_met: uniqueDecadeTargetGap === 0,
        unique_decade_target_gap: uniqueDecadeTargetGap,
        unique_decade_target_coverage: uniqueDecadeTargetCoverage,
        max_tracks_per_artist: getMaxTracksPerArtist(composedCreditTracks),
      },
    };
  }

  if (verification && constraint?.kind === 'credit') {
    verification.evidence_after = getCombinedCreditEvidenceCount((constraint.value || '').trim(), (constraint.creditRole || '').trim());
  }

  playlist.tracks = constraint ? verifiedTracks : candidateTracks;
  if (!constraint || (constraint.kind !== 'artist' && constraint.kind !== 'credit')) {
    playlist.tracks = applyGeneralArtistDiversity(playlist.tracks);
  }
  playlist.tracks = await sortTracksChronologicallyIfNeeded(translatedPrompt, playlist.tracks);
  if (!isHistoryOrStoryPrompt(translatedPrompt) && (!constraint || (constraint.kind !== 'artist' && constraint.kind !== 'credit'))) {
    playlist.tracks = staggerArtistsForFlow(playlist.tracks);
  }
  if (playlist.tracks.length > MAX_PLAYLIST_TRACKS) {
    playlist.tracks = playlist.tracks.slice(0, MAX_PLAYLIST_TRACKS);
  }
  if (!constraint || constraint.kind !== 'credit') {
    playlist.tracks = refineGeneralTrackReasons(playlist.tracks);
  }

  if (playlist.tracks.length === 0) {
    if (constraint?.kind === 'credit') {
      const creditName = (constraint.value || '').trim();
      const creditRole = (constraint.creditRole || '').trim();
      const evidenceCount = creditName && creditRole
        ? getCombinedCreditEvidenceCount(creditName, creditRole)
        : 0;
      if (evidenceCount === 0) {
        throw new Error(`No verified ${creditRole || 'credit'} evidence exists yet for ${creditName || 'this name'}. Generate a few related playlists that explicitly include this credit, or backfill evidence first.`);
      }
      throw new Error('Not enough verified credit evidence yet for this prompt. Try a broader prompt, or generate a few related playlists first to build evidence.');
    }
    throw new Error('Could not generate a non-empty playlist for this prompt. Please try a broader prompt.');
  }

  if (constraint?.kind === 'credit' && playlist.tracks.length < MIN_VERIFIED_TRACKS) {
    throw new Error(`Only ${playlist.tracks.length} strongly verified tracks were found for this credit constraint. Try a broader prompt, or generate a few related playlists first to build evidence.`);
  }

  // Store with translated prompt
  const rawTags = await generateTags(translatedPrompt);
  const tags = canonicalizeGeneratedTags(rawTags);
  const locations = await generateLocationMetadata(translatedPrompt);
  const promptStudios: string[] = [];
  const promptVenues: string[] = [];
  const extractedPlace = extractPlaceEntityFromPrompt(translatedPrompt);
  if (extractedPlace && isValidPlaceEntityName(extractedPlace)) {
    const heuristicType = inferPlaceTypeHeuristic(extractedPlace);
    const placeType = heuristicType === 'unknown'
      ? await classifyPlaceEntity(extractedPlace)
      : heuristicType;
    if (placeType === 'studio') {
      promptStudios.push(extractedPlace);
    } else if (placeType === 'venue') {
      promptVenues.push(extractedPlace);
    }
  }
  const mergedStudios = hasStudioIntent(translatedPrompt)
    ? dedupeStudiosByCanonical([...locations.studios, ...promptStudios]).slice(0, 3)
    : [];
  const mergedVenues = Array.from(new Set([...locations.venues, ...promptVenues])).slice(0, 3);
  const mergedLocations = { ...locations, studios: mergedStudios, venues: mergedVenues };
  const generatedScenes = await generateScenes(translatedPrompt);
  const scenes = constraint?.kind === 'credit'
    ? []
    : await bootstrapScenesFromTracks(translatedPrompt, playlist.tracks, generatedScenes);
  const influences = constraint?.kind === 'credit'
    ? []
    : await generateInfluences(translatedPrompt, playlist.tracks);
  const generatedCredits = constraint?.kind === 'credit'
    ? []
    : await generateCredits(translatedPrompt, playlist.tracks);
  const equipment = await generateEquipment(translatedPrompt, playlist.tracks);
  const memberships = await generateArtistMemberships(translatedPrompt, playlist.tracks);
  const truthRoutePersistedCredits = constraint?.kind === 'credit'
    && typeof constraint.value === 'string'
    && constraint.value.trim().length > 0
    && typeof constraint.creditRole === 'string'
    && ALLOWED_CREDIT_ROLES.has(constraint.creditRole)
    ? [{ name: constraint.value.trim(), role: constraint.creditRole }]
    : [];
  const explicitCreditForMixedPrompt = detectCreditPrompt(translatedPrompt);
  const mixedPromptPersistedCredits = explicitCreditForMixedPrompt
    && explicitCreditForMixedPrompt.name.trim().length > 0
    && ALLOWED_CREDIT_ROLES.has(explicitCreditForMixedPrompt.role)
    ? [{ name: explicitCreditForMixedPrompt.name, role: explicitCreditForMixedPrompt.role }]
    : [];
  const shouldPersistMemberships = Boolean(constraint?.creditMembersOfBand)
    || Boolean(explicitCreditForMixedPrompt)
    || /\bmembers?\s+of\b|\bband members?\b/i.test(translatedPrompt);
  const persistedCredits = constraint?.kind === 'credit'
    ? truthRoutePersistedCredits
    : mixedPromptPersistedCredits;
  console.log('[Gemini] Generated tags:', tags);
  console.log('[Gemini] Generated countries:', locations.countries);
  console.log('[Gemini] Generated cities:', locations.cities);
  console.log('[Gemini] Generated studios:', mergedLocations.studios);
  console.log('[Gemini] Generated venues:', locations.venues);
  console.log('[Gemini] Generated scenes:', scenes);
  console.log('[Gemini] Generated influences:', influences);
  console.log('[Gemini] Generated credits:', generatedCredits);
  console.log('[Gemini] Persisted credits:', persistedCredits);
  console.log('[Gemini] Generated equipment:', equipment);
  console.log('[Gemini] Generated memberships:', memberships);

  const saved = savePlaylist(
    translatedPrompt,
    playlist.title,
    playlist.description,
    JSON.stringify(playlist.tracks),
    JSON.stringify(tags),
    null,
    null,
    null,
    JSON.stringify(scenes),
    JSON.stringify(mergedLocations.countries),
    JSON.stringify(mergedLocations.cities),
    JSON.stringify(mergedLocations.studios),
    JSON.stringify(mergedLocations.venues),
    JSON.stringify(influences),
    JSON.stringify(persistedCredits),
    JSON.stringify(equipment)
  );

  if (shouldPersistMemberships) {
    try {
      saveArtistMembershipEvidence(saved.id, memberships);
    } catch {
      // Membership evidence is best-effort; playlist save should still succeed.
    }
  }

  recordRoutingSuccess(false);
  return {
    playlist: {
      ...playlist,
      tags,
      countries: mergedLocations.countries,
      cities: mergedLocations.cities,
      studios: mergedLocations.studios,
      venues: mergedLocations.venues,
      scenes,
      influences,
      credits: persistedCredits,
      equipment
    },
    cached: false,
    verification,
    truth
  };
}

function parsePlaylistResponse(text: string): Playlist {
  let jsonStr = text.trim();

  if (jsonStr.startsWith('```json')) {
    jsonStr = jsonStr.slice(7);
  } else if (jsonStr.startsWith('```')) {
    jsonStr = jsonStr.slice(3);
  }
  
  if (jsonStr.endsWith('```')) {
    jsonStr = jsonStr.slice(0, -3);
  }
  
  jsonStr = jsonStr.trim();

  const hasArtistDescriptorKeyword = (value: string): boolean => {
    const textValue = value.trim().toLowerCase();
    if (!textValue) return false;
    return /\bera\b|\bperiod\b|\bphase\b|\byears?\b|\bbest\s+of\b|\bgreatest\b|\btrilogy\b|\blive\b|\bversion\b|\bmix\b|\bedit\b|\bremaster\b/.test(textValue);
  };

  const sanitizeGeneratedArtistName = (value: string): string => {
    let next = value.trim();
    next = next.replace(/^artist\s*[:\-]\s*/i, '').trim();

    const parenthetical = next.match(/^(.+?)\s*\(([^)]+)\)\s*$/);
    if (parenthetical) {
      const base = parenthetical[1].trim();
      const label = parenthetical[2].trim();
      if (base && label.length <= 80 && hasArtistDescriptorKeyword(label)) {
        next = base;
      }
    }

    return next.replace(/\s+/g, ' ').trim();
  };

  const splitCollaboratorNames = (value: string): string[] => {
    return value
      .split(/\s*(?:,|&|\band\b|\boch\b|\bx\b|\+)\s*/i)
      .map((item) => canonicalizeDisplayName(item))
      .map((item) => item.replace(/^[-:\s]+/, '').replace(/[.!?,;:]+$/g, '').trim())
      .filter((item) => item.length > 0)
      .filter((item) => item.length <= 80);
  };

  const normalizeGeneratedArtistEntry = (value: string): { mainArtist: string; featuredArtists: string[]; displayArtist: string } => {
    const displayArtist = sanitizeGeneratedArtistName(value);
    if (!displayArtist) return { mainArtist: '', featuredArtists: [], displayArtist: '' };

    const normalizedDisplay = displayArtist
      .replace(/[“”]/g, '"')
      .replace(/\s+/g, ' ')
      .trim();

    const markerMatch = normalizedDisplay.match(/^(.*?)\s*(?:,?\s*(?:feat\.?|featuring|ft\.?|duett\s+med|duet\s+with|with)\s+)(.+)$/i);
    if (!markerMatch) {
      const delimiterParts = splitCollaboratorNames(normalizedDisplay);
      if (delimiterParts.length >= 2) {
        const mainArtist = delimiterParts[0];
        const featuredArtists = delimiterParts.slice(1)
          .filter((name) => normalize(name) !== normalize(mainArtist));
        const mainWordCount = mainArtist.split(/\s+/g).filter(Boolean).length;
        const mainLength = mainArtist.replace(/\s+/g, '').length;
        const looksLikeSafePrimarySplit = mainWordCount >= 2 && mainLength >= 6;
        if (looksLikeSafePrimarySplit) {
          return {
            mainArtist,
            featuredArtists: Array.from(new Set(featuredArtists.map((name) => canonicalizeDisplayName(name)))),
            displayArtist: normalizedDisplay,
          };
        }
      }

      return { mainArtist: normalizedDisplay, featuredArtists: [], displayArtist: normalizedDisplay };
    }

    const mainArtist = sanitizeGeneratedArtistName(markerMatch[1] || '');
    const featuredArtists = splitCollaboratorNames(markerMatch[2] || '');
    const dedupedFeatured = Array.from(new Set(featuredArtists.map((name) => canonicalizeDisplayName(name))))
      .filter((name) => normalize(name) !== normalize(mainArtist));

    return {
      mainArtist,
      featuredArtists: dedupedFeatured,
      displayArtist: normalizedDisplay,
    };
  };

  const sanitizeGeneratedSongTitle = (value: string): string => {
    return value
      .trim()
      .replace(/^(song|track)\s*[:\-]\s*/i, '')
      .replace(/\s+/g, ' ')
      .trim();
  };

  const extractFeaturedArtistsFromSongTitle = (value: string): { cleanSong: string; featuredArtists: string[] } => {
    const source = sanitizeGeneratedSongTitle(value);
    if (!source) return { cleanSong: '', featuredArtists: [] };

    const featuredNames: string[] = [];
    let cleanSong = source;
    const featureRegex = /\((?:feat\.?|featuring|ft\.?)\s+([^)]{1,120})\)/ig;
    cleanSong = cleanSong.replace(featureRegex, (_match, group: string) => {
      featuredNames.push(...splitCollaboratorNames(group || ''));
      return '';
    }).replace(/\s+/g, ' ').trim();

    const duetRegex = /\((?:duett\s+med|duet\s+with|with|med)\s+([^)]{1,120})\)/ig;
    cleanSong = cleanSong.replace(duetRegex, (_match, group: string) => {
      featuredNames.push(...splitCollaboratorNames(group || ''));
      return '';
    }).replace(/\s+/g, ' ').trim();

    cleanSong = cleanSong.replace(/\s*[-–—]\s*(?:feat\.?|featuring|ft\.?)\s+(.+)$/i, (_match, group: string) => {
      featuredNames.push(...splitCollaboratorNames(group || ''));
      return '';
    }).replace(/\s+/g, ' ').trim();

    cleanSong = cleanSong.replace(/\s*(?:[-–—]|,)\s*(?:duett\s+med|duet\s+with|with|med)\s+(.+)$/i, (_match, group: string) => {
      featuredNames.push(...splitCollaboratorNames(group || ''));
      return '';
    }).replace(/\s+/g, ' ').trim();

    const dedupedFeatured = Array.from(new Set(featuredNames.map((name) => canonicalizeDisplayName(name)))).filter(Boolean);
    return { cleanSong, featuredArtists: dedupedFeatured };
  };

  const tryParse = (input: string): Playlist => {
    const parsed = JSON.parse(input);

    if (!parsed.title || !parsed.description || !Array.isArray(parsed.tracks)) {
      throw new Error('Invalid playlist format: missing required fields');
    }

    for (const track of parsed.tracks) {
      if (!track || typeof track !== 'object') {
        throw new Error('Invalid track format: track must be an object');
      }

      const row = track as { artist?: unknown; song?: unknown; reason?: unknown };
      const artistRow = typeof row.artist === 'string' ? normalizeGeneratedArtistEntry(row.artist) : { mainArtist: '', featuredArtists: [], displayArtist: '' };
      const songRow = typeof row.song === 'string' ? extractFeaturedArtistsFromSongTitle(row.song) : { cleanSong: '', featuredArtists: [] };
      const artist = artistRow.mainArtist;
      const song = songRow.cleanSong;
      const reason = typeof row.reason === 'string' ? row.reason.trim() : '';
      const featuredArtists = Array.from(new Set([...artistRow.featuredArtists, ...songRow.featuredArtists]))
        .map((name) => canonicalizeDisplayName(name))
        .filter((name) => name.length > 0 && normalize(name) !== normalize(artist));

      (track as { artist: string }).artist = artist;
      (track as { song: string }).song = song;
      (track as { reason: string }).reason = reason;
      if (artistRow.displayArtist && normalize(artistRow.displayArtist) !== normalize(artist)) {
        (track as { artist_display?: string }).artist_display = artistRow.displayArtist;
      }
      if (featuredArtists.length > 0) {
        (track as { featured_artists?: string[] }).featured_artists = featuredArtists;
      }

      if (!artist || !song || !reason) {
        throw new Error('Invalid track format: missing required fields');
      }
    }

    return parsed as Playlist;
  };

  try {
    return tryParse(jsonStr);
  } catch {
    // Fallback for common Gemini near-JSON issues (smart quotes, missing object close between track items).
    const repairedBase = jsonStr
      .replace(/[“”]/g, '"')
      .replace(/[‘’]/g, "'")
      .replace(/"\s*,\s*\n\s*\{/g, '"\n    },\n    {');

    const repaired = repairedBase.replace(
      /("(?:artist|song|reason|title|description)"\s*:\s*")(.*)(")\s*([,}])/g,
      (_match, prefix: string, value: string, _closingQuote: string, tail: string) => {
        const escapedValue = value.replace(/(?<!\\)"/g, '\\"');
        return `${prefix}${escapedValue}"${tail}`;
      }
    );

    return tryParse(repaired);
  }
}

export function parsePlaylistResponseForEval(text: string): Playlist {
  return parsePlaylistResponse(text);
}

export function detectPlaylistCurationModeForEval(prompt: string): { mode: PlaylistCurationMode; inferredFromPrompt: boolean } {
  return detectPlaylistCurationMode(prompt);
}
