import Database from 'better-sqlite3';
import path from 'path';
import {
  buildArtistCanonicalKey,
  buildCreditCanonicalKey,
  buildPersonCanonicalKey,
  buildStudioCanonicalKey,
  canonicalizeDisplayName,
} from './normalize.js';

const dbPath = path.resolve(process.cwd(), 'playlists.db');
const db = new Database(dbPath);

db.exec(`
  CREATE TABLE IF NOT EXISTS playlists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    tracks TEXT NOT NULL,
    tags TEXT,
    place TEXT,
    scene TEXT,
    places TEXT,
    scenes TEXT,
    countries TEXT,
    cities TEXT,
    studios TEXT,
    venues TEXT,
    influences TEXT,
    credits TEXT,
    equipment TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS recordings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artist TEXT NOT NULL,
    title TEXT NOT NULL,
    canonical_key TEXT UNIQUE NOT NULL,
    isrc TEXT,
    spotify_uri TEXT,
    spotify_url TEXT,
    duration_ms INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS recording_equipment_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_id INTEGER NOT NULL,
    equipment_name TEXT NOT NULL,
    equipment_category TEXT NOT NULL,
    source_playlist_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (recording_id) REFERENCES recordings(id),
    FOREIGN KEY (source_playlist_id) REFERENCES playlists(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS recording_studio_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_id INTEGER NOT NULL,
    studio_name TEXT NOT NULL,
    studio_name_canonical TEXT,
    source_playlist_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (recording_id) REFERENCES recordings(id),
    FOREIGN KEY (source_playlist_id) REFERENCES playlists(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS recording_credit_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_id INTEGER NOT NULL,
    credit_name TEXT NOT NULL,
    credit_name_canonical TEXT,
    credit_role TEXT NOT NULL,
    source_playlist_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (recording_id) REFERENCES recordings(id),
    FOREIGN KEY (source_playlist_id) REFERENCES playlists(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS artist_membership_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    band_name TEXT NOT NULL,
    band_name_canonical TEXT,
    person_name TEXT NOT NULL,
    person_name_canonical TEXT,
    member_role TEXT,
    source_playlist_id INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_playlist_id) REFERENCES playlists(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS atlas_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_type TEXT NOT NULL,
    from_name TEXT NOT NULL,
    to_type TEXT NOT NULL,
    to_name TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    strength INTEGER NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
  )
`);

db.exec('CREATE INDEX IF NOT EXISTS idx_atlas_edges_from ON atlas_edges(from_type, from_name)');
db.exec('CREATE INDEX IF NOT EXISTS idx_atlas_edges_to ON atlas_edges(to_type, to_name)');

db.exec(`
  CREATE TABLE IF NOT EXISTS atlas_node_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_type TEXT NOT NULL,
    node_name TEXT NOT NULL,
    edge_count INTEGER NOT NULL DEFAULT 0,
    weighted_strength INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT
  )
`);

db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_atlas_node_stats_unique ON atlas_node_stats(node_type, node_name)');

const columns = db.prepare("PRAGMA table_info(playlists)").all() as Array<{ name: string }>;
const hasTagsColumn = columns.some((column) => column.name === 'tags');
const hasPlaceColumn = columns.some((column) => column.name === 'place');
const hasSceneColumn = columns.some((column) => column.name === 'scene');
const hasPlacesColumn = columns.some((column) => column.name === 'places');
const hasScenesColumn = columns.some((column) => column.name === 'scenes');
const hasCountriesColumn = columns.some((column) => column.name === 'countries');
const hasCitiesColumn = columns.some((column) => column.name === 'cities');
const hasStudiosColumn = columns.some((column) => column.name === 'studios');
const hasVenuesColumn = columns.some((column) => column.name === 'venues');
const hasInfluencesColumn = columns.some((column) => column.name === 'influences');
const hasCreditsColumn = columns.some((column) => column.name === 'credits');
const hasEquipmentColumn = columns.some((column) => column.name === 'equipment');

if (!hasTagsColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN tags TEXT');
}

if (!hasPlaceColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN place TEXT');
}

if (!hasSceneColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN scene TEXT');
}

if (!hasPlacesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN places TEXT');
}

if (!hasScenesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN scenes TEXT');
}

if (!hasCountriesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN countries TEXT');
}

if (!hasCitiesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN cities TEXT');
}

if (!hasStudiosColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN studios TEXT');
}

if (!hasVenuesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN venues TEXT');
}

if (!hasInfluencesColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN influences TEXT');
}

if (!hasCreditsColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN credits TEXT');
}

if (!hasEquipmentColumn) {
  db.exec('ALTER TABLE playlists ADD COLUMN equipment TEXT');
}

const studioEvidenceColumns = db.prepare('PRAGMA table_info(recording_studio_evidence)').all() as Array<{ name: string }>;
const hasStudioCanonicalColumn = studioEvidenceColumns.some((column) => column.name === 'studio_name_canonical');
if (!hasStudioCanonicalColumn) {
  db.exec('ALTER TABLE recording_studio_evidence ADD COLUMN studio_name_canonical TEXT');
}

const creditEvidenceColumns = db.prepare('PRAGMA table_info(recording_credit_evidence)').all() as Array<{ name: string }>;
const hasCreditCanonicalColumn = creditEvidenceColumns.some((column) => column.name === 'credit_name_canonical');
if (!hasCreditCanonicalColumn) {
  db.exec('ALTER TABLE recording_credit_evidence ADD COLUMN credit_name_canonical TEXT');
}

const membershipEvidenceColumns = db.prepare('PRAGMA table_info(artist_membership_evidence)').all() as Array<{ name: string }>;
const hasBandCanonicalColumn = membershipEvidenceColumns.some((column) => column.name === 'band_name_canonical');
const hasPersonCanonicalColumn = membershipEvidenceColumns.some((column) => column.name === 'person_name_canonical');

if (!hasBandCanonicalColumn) {
  db.exec('ALTER TABLE artist_membership_evidence ADD COLUMN band_name_canonical TEXT');
}

if (!hasPersonCanonicalColumn) {
  db.exec('ALTER TABLE artist_membership_evidence ADD COLUMN person_name_canonical TEXT');
}

const recordingsColumns = db.prepare('PRAGMA table_info(recordings)').all() as Array<{ name: string }>;
const hasRecordingIsrcColumn = recordingsColumns.some((column) => column.name === 'isrc');
const hasRecordingSpotifyUriColumn = recordingsColumns.some((column) => column.name === 'spotify_uri');
const hasRecordingSpotifyUrlColumn = recordingsColumns.some((column) => column.name === 'spotify_url');
const hasRecordingDurationMsColumn = recordingsColumns.some((column) => column.name === 'duration_ms');

if (!hasRecordingIsrcColumn) {
  db.exec('ALTER TABLE recordings ADD COLUMN isrc TEXT');
}

if (!hasRecordingSpotifyUriColumn) {
  db.exec('ALTER TABLE recordings ADD COLUMN spotify_uri TEXT');
}

if (!hasRecordingSpotifyUrlColumn) {
  db.exec('ALTER TABLE recordings ADD COLUMN spotify_url TEXT');
}

if (!hasRecordingDurationMsColumn) {
  db.exec('ALTER TABLE recordings ADD COLUMN duration_ms INTEGER');
}

db.exec(`
  UPDATE recording_studio_evidence
  SET studio_name_canonical = lower(trim(studio_name))
  WHERE (studio_name_canonical IS NULL OR trim(studio_name_canonical) = '')
    AND studio_name IS NOT NULL
`);

try {
  const studioRows = db.prepare('SELECT id, studio_name, studio_name_canonical FROM recording_studio_evidence').all() as Array<{
    id: number;
    studio_name: string | null;
    studio_name_canonical: string | null;
  }>;
  const updateStudioCanonicalStmt = db.prepare('UPDATE recording_studio_evidence SET studio_name_canonical = ? WHERE id = ?');

  for (const row of studioRows) {
    const studioName = typeof row.studio_name === 'string' ? row.studio_name : '';
    const recanonicalized = buildStudioCanonicalKey(studioName);
    if (!recanonicalized) continue;

    const existingCanonical = typeof row.studio_name_canonical === 'string'
      ? row.studio_name_canonical.trim().toLowerCase()
      : '';
    if (existingCanonical === recanonicalized) continue;

    updateStudioCanonicalStmt.run(recanonicalized, row.id);
  }
} catch {
  // Re-canonicalization is best-effort for existing evidence rows.
}

db.exec(`
  UPDATE recording_credit_evidence
  SET credit_name_canonical = lower(trim(credit_name))
  WHERE (credit_name_canonical IS NULL OR trim(credit_name_canonical) = '')
    AND credit_name IS NOT NULL
`);

db.exec(`
  UPDATE artist_membership_evidence
  SET band_name_canonical = lower(trim(band_name))
  WHERE (band_name_canonical IS NULL OR trim(band_name_canonical) = '')
    AND band_name IS NOT NULL
`);

db.exec(`
  UPDATE artist_membership_evidence
  SET person_name_canonical = lower(trim(person_name))
  WHERE (person_name_canonical IS NULL OR trim(person_name_canonical) = '')
    AND person_name IS NOT NULL
`);

db.exec('CREATE INDEX IF NOT EXISTS idx_recording_studio_evidence_canonical ON recording_studio_evidence(studio_name_canonical)');
db.exec('CREATE INDEX IF NOT EXISTS idx_recording_credit_evidence_canonical ON recording_credit_evidence(credit_name_canonical, credit_role)');
db.exec('CREATE INDEX IF NOT EXISTS idx_artist_membership_person_canonical ON artist_membership_evidence(person_name_canonical)');
db.exec('CREATE INDEX IF NOT EXISTS idx_artist_membership_band_canonical ON artist_membership_evidence(band_name_canonical)');

db.exec(`
  CREATE TABLE IF NOT EXISTS truth_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    canonical_key TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, canonical_key)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS truth_external_ids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, external_id),
    UNIQUE(entity_id, source),
    FOREIGN KEY (entity_id) REFERENCES truth_entities(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS truth_memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_entity_id INTEGER NOT NULL,
    group_entity_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_ref TEXT,
    member_role TEXT,
    confidence INTEGER NOT NULL DEFAULT 100,
    valid_from TEXT,
    valid_to TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(person_entity_id, group_entity_id, source),
    FOREIGN KEY (person_entity_id) REFERENCES truth_entities(id),
    FOREIGN KEY (group_entity_id) REFERENCES truth_entities(id)
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS truth_import_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    subject_type TEXT,
    subject_value TEXT,
    status TEXT NOT NULL,
    stats_json TEXT,
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS truth_credit_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_canonical_key TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    recording_title TEXT NOT NULL,
    credit_entity_id INTEGER,
    credit_name TEXT NOT NULL,
    credit_role TEXT NOT NULL,
    source TEXT NOT NULL,
    source_ref TEXT,
    confidence INTEGER NOT NULL DEFAULT 100,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(recording_canonical_key, credit_name, credit_role, source),
    FOREIGN KEY (credit_entity_id) REFERENCES truth_entities(id)
  )
`);

db.exec('CREATE INDEX IF NOT EXISTS idx_truth_entities_type_key ON truth_entities(entity_type, canonical_key)');
db.exec('CREATE INDEX IF NOT EXISTS idx_truth_external_ids_source_external ON truth_external_ids(source, external_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_truth_memberships_group ON truth_memberships(group_entity_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_truth_memberships_person ON truth_memberships(person_entity_id)');
db.exec('CREATE INDEX IF NOT EXISTS idx_truth_credit_claims_recording ON truth_credit_claims(recording_canonical_key)');
db.exec('CREATE INDEX IF NOT EXISTS idx_truth_credit_claims_name_role ON truth_credit_claims(lower(trim(credit_name)), lower(trim(credit_role)))');

export interface PlaylistRow {
  id: number;
  prompt: string;
  title: string;
  description: string;
  tracks: string;
  tags: string | null;
  place: string | null;
  scene: string | null;
  places: string | null;
  scenes: string | null;
  countries: string | null;
  cities: string | null;
  studios: string | null;
  venues: string | null;
  influences: string | null;
  credits: string | null;
  equipment: string | null;
  created_at: string;
}

function parseStringArray(raw: string | null | undefined): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((item): item is string => typeof item === 'string' && item.length > 0);
  } catch {
    return [];
  }
}

function normalizeRecordingToken(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function buildRecordingCanonicalKey(artist: string, title: string): string {
  return `${normalizeRecordingToken(artist)}::${normalizeRecordingToken(title)}`;
}

function normalizeIsrc(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function ensureRecordingId(artist: string, title: string): number | null {
  const artistValue = canonicalizeDisplayName(artist || '');
  const titleValue = String(title || '').trim();
  if (!artistValue || !titleValue) return null;

  const canonicalKey = buildRecordingCanonicalKey(artistValue, titleValue);
  if (!canonicalKey) return null;

  const existing = db.prepare('SELECT id FROM recordings WHERE canonical_key = ? LIMIT 1').get(canonicalKey) as { id: number } | undefined;
  if (existing?.id) return existing.id;

  const inserted = db.prepare('INSERT INTO recordings (artist, title, canonical_key) VALUES (?, ?, ?)').run(artistValue, titleValue, canonicalKey);
  return Number(inserted.lastInsertRowid || 0) || null;
}

function upsertDirectedAtlasEdge(
  fromType: string,
  fromName: string,
  toType: string,
  toName: string,
  relationType: string,
  strength: number
): void {
  const now = new Date().toISOString();
  const selectStmt = db.prepare(`
    SELECT id, evidence_count
    FROM atlas_edges
    WHERE from_type = ? AND from_name = ? AND to_type = ? AND to_name = ? AND relation_type = ?
  `);
  const existing = selectStmt.get(fromType, fromName, toType, toName, relationType) as { id: number; evidence_count: number } | undefined;

  if (existing) {
    const updateStmt = db.prepare('UPDATE atlas_edges SET evidence_count = ?, updated_at = ? WHERE id = ?');
    updateStmt.run(existing.evidence_count + 1, now, existing.id);
    return;
  }

  const insertStmt = db.prepare(`
    INSERT INTO atlas_edges (from_type, from_name, to_type, to_name, relation_type, strength, evidence_count, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
  `);
  insertStmt.run(fromType, fromName, toType, toName, relationType, strength, now, now);
}

function incrementAtlasNodeStats(nodeType: string, nodeName: string, strength: number): void {
  const typeValue = nodeType.trim();
  const nameValue = nodeName.trim();
  if (!typeValue || !nameValue) return;

  const safeStrength = Number.isFinite(strength) ? Math.max(1, Math.floor(strength)) : 1;
  const now = new Date().toISOString();

  const selectStmt = db.prepare('SELECT id, edge_count, weighted_strength FROM atlas_node_stats WHERE node_type = ? AND node_name = ?');
  const existing = selectStmt.get(typeValue, nameValue) as { id: number; edge_count: number; weighted_strength: number } | undefined;

  if (existing) {
    const updateStmt = db.prepare('UPDATE atlas_node_stats SET edge_count = ?, weighted_strength = ?, updated_at = ? WHERE id = ?');
    updateStmt.run(existing.edge_count + 1, existing.weighted_strength + safeStrength, now, existing.id);
    return;
  }

  const insertStmt = db.prepare(`
    INSERT INTO atlas_node_stats (node_type, node_name, edge_count, weighted_strength, updated_at)
    VALUES (?, ?, 1, ?, ?)
  `);
  insertStmt.run(typeValue, nameValue, safeStrength, now);
}

function insertAtlasEdgeEvidence(
  fromType: string,
  fromName: string,
  toType: string,
  toName: string,
  relationType: string,
  strength: number
): void {
  upsertDirectedAtlasEdge(fromType, fromName, toType, toName, relationType, strength);
  upsertDirectedAtlasEdge(toType, toName, fromType, fromName, relationType, strength);

  try {
    incrementAtlasNodeStats(fromType, fromName, strength);
    incrementAtlasNodeStats(toType, toName, strength);
  } catch {
    // Node stats updates are best-effort.
  }
}

function displayCasingScore(value: string): number {
  const trimmed = value.trim();
  if (!trimmed) return 0;

  let score = 0;
  const words = trimmed.split(/\s+/);
  for (const word of words) {
    if (/^[A-Z][a-z]/.test(word)) score += 2;
    if (/^[A-Z]{2,}$/.test(word)) score += 2;
  }

  if (/\bStudios?\b/.test(trimmed)) score += 2;
  return score;
}

function choosePreferredDisplayValue(current: string, incoming: string): string {
  const currentScore = displayCasingScore(current);
  const incomingScore = displayCasingScore(incoming);
  if (incomingScore > currentScore) return incoming;
  return current;
}

const STUDIO_REGEX = /\b(studio|studios|recorders|recording)\b/i;
const VENUE_REGEX = /\b(club|hall|theatre|theater|arena|venue|cbgb)\b/i;
const COUNTRY_NAMES = new Set([
  'usa', 'united states', 'united kingdom', 'uk', 'england', 'france', 'germany', 'italy', 'spain',
  'sweden', 'norway', 'denmark', 'finland', 'japan', 'canada', 'australia', 'brazil', 'mexico', 'ireland'
]);

const CITY_ALIAS_MAP = new Map<string, string>([
  ['new york', 'New York City'],
  ['nyc', 'New York City'],
  ['la', 'Los Angeles'],
  ['sf', 'San Francisco'],
]);

const EQUIPMENT_ALIAS_MAP = new Map<string, string>([
  ['mellotron mk 1', 'Mellotron Mk I'],
  ['tr 808', 'TR-808'],
  ['roland tr 808', 'Roland TR-808'],
]);

const GENERIC_EQUIPMENT_LABELS = new Set([
  'bass',
  'bass guitar',
  'drums',
  'electric guitar',
  'electric bass',
  'guitar',
  'keyboard',
  'organ',
  'piano',
  'mixing console',
  'microphone',
  'saxophone',
  'tape machine',
  'trombone',
  'trumpet',
  'console',
  'instrument',
  'synthesizer',
  'amplifier',
  'effect',
  'sampler',
  'drum machine',
  'vocals',
]);

function canonicalizeCityName(name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return '';

  const normalizedKey = trimmed.toLowerCase().replace(/\s+/g, ' ');
  return CITY_ALIAS_MAP.get(normalizedKey) || trimmed;
}

export function canonicalizeEquipmentName(name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return '';

  const normalizedKey = trimmed.toLowerCase().replace(/\s+/g, ' ');
  return EQUIPMENT_ALIAS_MAP.get(normalizedKey) || trimmed;
}

export function isGenericEquipmentName(name: string): boolean {
  const normalized = name.trim().toLowerCase().replace(/\s+/g, ' ');
  if (!normalized) return false;
  return GENERIC_EQUIPMENT_LABELS.has(normalized);
}

function sanitizeStudioName(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return '';

  const withoutContext = trimmed
    .replace(/\s+(?:in|during|from|at)\b.*$/i, '')
    .trim();

  return withoutContext;
}

const AMBIGUOUS_STUDIO_BASE_NAMES = new Map<string, string>([
  ['record plant', 'Record Plant'],
]);

function disambiguateStudioNameWithCity(studioName: string, normalizedCities: string[]): string {
  const trimmed = studioName.trim();
  if (!trimmed) return '';

  const key = trimmed.toLowerCase().replace(/\s+/g, ' ');
  const canonicalBase = AMBIGUOUS_STUDIO_BASE_NAMES.get(key);
  if (!canonicalBase) return trimmed;

  if (normalizedCities.length === 1) {
    const city = normalizedCities[0]?.trim();
    if (city) {
      return `${canonicalBase} (${city})`;
    }
  }

  return trimmed;
}

function getLegacyPlaceEntries(row: Pick<PlaylistRow, 'place' | 'places'>): string[] {
  const fromArray = parseStringArray(row.places);
  if (fromArray.length > 0) return fromArray;
  if (row.place && row.place.trim().length > 0) return [row.place.trim()];
  return [];
}

function getFallbackLocationsFromLegacy(row: Pick<PlaylistRow, 'place' | 'places'>): {
  countries: string[];
  cities: string[];
  studios: string[];
  venues: string[];
} {
  const countries = new Set<string>();
  const cities = new Set<string>();
  const studios = new Set<string>();
  const venues = new Set<string>();

  const entries = getLegacyPlaceEntries(row);

  for (const rawEntry of entries) {
    const entry = rawEntry.trim();
    if (!entry) continue;

    const parts = entry.split(',').map((part) => part.trim()).filter(Boolean);
    const head = parts[0] || '';
    const tail = parts.length > 1 ? parts[parts.length - 1] : '';
    const headLower = head.toLowerCase();
    const tailLower = tail.toLowerCase();

    if (STUDIO_REGEX.test(head) || STUDIO_REGEX.test(entry)) {
      const studioName = sanitizeStudioName(head || entry);
      const contextCities = tail && !COUNTRY_NAMES.has(tailLower) ? [canonicalizeCityName(tail)] : [];
      const disambiguatedStudio = disambiguateStudioNameWithCity(studioName, contextCities);
      if (disambiguatedStudio) studios.add(disambiguatedStudio);
      if (tail && !COUNTRY_NAMES.has(tailLower)) cities.add(tail);
      if (tail && COUNTRY_NAMES.has(tailLower)) countries.add(tail);
      continue;
    }

    if (VENUE_REGEX.test(head) || VENUE_REGEX.test(entry)) {
      if (head) venues.add(head);
      if (tail && !COUNTRY_NAMES.has(tailLower)) cities.add(tail);
      if (tail && COUNTRY_NAMES.has(tailLower)) countries.add(tail);
      continue;
    }

    if (parts.length === 2 && COUNTRY_NAMES.has(tailLower)) {
      cities.add(head);
      countries.add(tail);
      continue;
    }

    if (COUNTRY_NAMES.has(headLower)) {
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

function getNormalizedPlaces(row: Pick<PlaylistRow, 'places' | 'place' | 'countries' | 'cities' | 'studios' | 'venues'>): string[] {
  const countries = parseStringArray(row.countries);
  const cities = parseStringArray(row.cities);
  const studios = parseStringArray(row.studios);
  const venues = parseStringArray(row.venues);
  const combined = [...countries, ...cities, ...studios, ...venues];
  if (combined.length > 0) return combined;

  const places = parseStringArray(row.places);
  if (places.length > 0) return places;
  if (row.place && row.place.length > 0) return [row.place];
  return [];
}

function getNormalizedCountries(row: Pick<PlaylistRow, 'countries' | 'place' | 'places'>): string[] {
  const countries = parseStringArray(row.countries);
  if (countries.length > 0) return countries;
  return getFallbackLocationsFromLegacy(row).countries;
}

function getNormalizedCities(row: Pick<PlaylistRow, 'cities' | 'place' | 'places'>): string[] {
  const rawCities = (() => {
    const cities = parseStringArray(row.cities);
    if (cities.length > 0) return cities;
    return getFallbackLocationsFromLegacy(row).cities;
  })();

  const deduped = new Map<string, string>();
  for (const city of rawCities) {
    const canonical = canonicalizeCityName(city);
    if (!canonical) continue;
    const key = canonical.toLowerCase();
    if (!deduped.has(key)) {
      deduped.set(key, canonical);
    }
  }

  return Array.from(deduped.values());
}

function getNormalizedStudios(row: Pick<PlaylistRow, 'studios' | 'place' | 'places' | 'cities'>): string[] {
  const normalizedCities = getNormalizedCities({ cities: row.cities, place: row.place, places: row.places });
  const studios = parseStringArray(row.studios);
  const dedupeByCanonical = (values: string[]): string[] => {
    const deduped = new Map<string, string>();
    for (const value of values) {
      const normalizedValue = sanitizeStudioName(value);
      if (!normalizedValue) continue;
      const disambiguated = disambiguateStudioNameWithCity(normalizedValue, normalizedCities);
      if (!disambiguated) continue;

      const canonical = buildStudioCanonicalKey(disambiguated);
      if (!canonical) continue;

      const existing = deduped.get(canonical);
      if (existing) {
        deduped.set(canonical, choosePreferredDisplayValue(existing, disambiguated));
      } else {
        deduped.set(canonical, disambiguated);
      }
    }
    return Array.from(deduped.values());
  };

  if (studios.length > 0) {
    return dedupeByCanonical(studios);
  }
  return dedupeByCanonical(getFallbackLocationsFromLegacy(row).studios);
}

function isValidStudioEvidenceName(name: string): boolean {
  const trimmed = name.trim();
  if (!trimmed) return false;
  const normalized = trimmed.toLowerCase();
  if (!/[a-z]/i.test(trimmed)) return false;
  if (trimmed.length < 3 || trimmed.length > 80) return false;
  if (/^(album|albums|song|songs|track|tracks|record|records|recording|recordings)$/i.test(trimmed)) return false;
  if (/^(albums?|songs?|tracks?|records?|recordings?)\b/.test(normalized)) return false;
  return true;
}

function normalizeStudioMatchText(value: string): string {
  return normalizePromptForCache(value)
    .replace(/\brecording\s+studios?\b/g, 'studio')
    .replace(/\bstudios\b/g, 'studio')
    .replace(/\s+/g, ' ')
    .trim();
}

function getNormalizedVenues(row: Pick<PlaylistRow, 'venues' | 'place' | 'places'>): string[] {
  const venues = parseStringArray(row.venues);
  if (venues.length > 0) return venues;
  return getFallbackLocationsFromLegacy(row).venues;
}

function getNormalizedEquipmentNames(row: Pick<PlaylistRow, 'equipment'>): string[] {
  if (!row.equipment) return [];
  try {
    const parsed = JSON.parse(row.equipment);
    if (!Array.isArray(parsed)) return [];
    const deduped = new Map<string, string>();
    for (const item of parsed) {
      if (!item || typeof item !== 'object' || typeof item.name !== 'string') continue;
      const canonical = canonicalizeEquipmentName(item.name);
      if (!canonical) continue;
      if (isGenericEquipmentName(canonical)) continue;
      const key = canonical.toLowerCase();
      if (!deduped.has(key)) {
        deduped.set(key, canonical);
      }
    }
    return Array.from(deduped.values());
  } catch {
    return [];
  }
}

function parseCanonicalEquipmentEntries(raw: string | null | undefined): Array<{ name: string; category: string }> {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];

    const entries: Array<{ name: string; category: string }> = [];
    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;
      if (typeof item.name !== 'string' || typeof item.category !== 'string') continue;
      const canonicalName = canonicalizeEquipmentName(item.name);
      const category = item.category.trim();
      if (!canonicalName || !category) continue;
      if (isGenericEquipmentName(canonicalName)) continue;
      entries.push({ name: canonicalName, category });
    }

    return entries;
  } catch {
    return [];
  }
}

function getDirectStudiosForCity(
  row: Pick<PlaylistRow, 'place' | 'places' | 'cities' | 'studios'>,
  city: string
): string[] {
  const target = city.trim().toLowerCase();
  if (!target) return [];

  const direct = new Set<string>();
  const entries = getLegacyPlaceEntries({ place: row.place, places: row.places });
  for (const rawEntry of entries) {
    const parts = rawEntry.split(',').map((part) => part.trim()).filter(Boolean);
    if (parts.length < 2) continue;
    const head = parts[0] || '';
    const tail = parts[parts.length - 1] || '';
    if (tail.toLowerCase() !== target) continue;
    if (!STUDIO_REGEX.test(head) && !STUDIO_REGEX.test(rawEntry)) continue;
    const studioName = sanitizeStudioName(head || rawEntry);
    if (studioName) direct.add(studioName);
  }

  if (direct.size > 0) {
    return Array.from(direct);
  }

  const normalizedCities = getNormalizedCities({ cities: row.cities, place: row.place, places: row.places });
  if (normalizedCities.length === 1 && normalizedCities[0].toLowerCase() === target) {
    return getNormalizedStudios({ studios: row.studios, place: row.place, places: row.places, cities: row.cities });
  }

  return [];
}

function getDirectVenuesForCity(
  row: Pick<PlaylistRow, 'place' | 'places' | 'cities' | 'venues'>,
  city: string
): string[] {
  const target = city.trim().toLowerCase();
  if (!target) return [];

  const direct = new Set<string>();
  const entries = getLegacyPlaceEntries({ place: row.place, places: row.places });
  for (const rawEntry of entries) {
    const parts = rawEntry.split(',').map((part) => part.trim()).filter(Boolean);
    if (parts.length < 2) continue;
    const head = parts[0] || '';
    const tail = parts[parts.length - 1] || '';
    if (tail.toLowerCase() !== target) continue;
    if (!VENUE_REGEX.test(head) && !VENUE_REGEX.test(rawEntry)) continue;
    if (head) direct.add(head);
  }

  if (direct.size > 0) {
    return Array.from(direct);
  }

  const normalizedCities = getNormalizedCities({ cities: row.cities, place: row.place, places: row.places });
  if (normalizedCities.length === 1 && normalizedCities[0].toLowerCase() === target) {
    return getNormalizedVenues({ venues: row.venues, place: row.place, places: row.places });
  }

  return [];
}

function getTrackArtists(row: Pick<PlaylistRow, 'tracks'>): string[] {
  try {
    const tracks = JSON.parse(row.tracks);
    if (!Array.isArray(tracks)) return [];

    const artists = new Set<string>();
    for (const track of tracks) {
      if (!track || typeof track !== 'object' || typeof track.artist !== 'string') continue;
      const name = track.artist.trim();
      if (!name) continue;
      artists.add(name);
    }

    return Array.from(artists);
  } catch {
    return [];
  }
}

export interface AtlasEntityCatalog {
  artists: string[];
  studios: string[];
  venues: string[];
  scenes: string[];
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

export function getAtlasEntityCatalog(): AtlasEntityCatalog {
  const stmt = db.prepare(`
    SELECT tracks, place, places, scene, scenes, cities, studios, venues
    FROM playlists
  `);

  const rows = stmt.all() as Array<
    Pick<PlaylistRow, 'tracks' | 'place' | 'places' | 'scene' | 'scenes' | 'cities' | 'studios' | 'venues'>
  >;

  const artists = new Set<string>();
  const studios = new Set<string>();
  const venues = new Set<string>();
  const scenes = new Set<string>();

  for (const row of rows) {
    for (const artist of getTrackArtists({ tracks: row.tracks })) {
      artists.add(artist);
    }

    for (const studio of getNormalizedStudios({
      studios: row.studios,
      place: row.place,
      places: row.places,
      cities: row.cities,
    })) {
      studios.add(studio);
    }

    for (const venue of getNormalizedVenues({
      venues: row.venues,
      place: row.place,
      places: row.places,
    })) {
      venues.add(venue);
    }

    for (const scene of getNormalizedScenes({ scenes: row.scenes, scene: row.scene })) {
      scenes.add(scene);
    }
  }

  try {
    const membershipRows = db.prepare(`
      SELECT band_name, person_name
      FROM artist_membership_evidence
    `).all() as Array<{ band_name: string | null; person_name: string | null }>;

    for (const row of membershipRows) {
      const bandName = typeof row.band_name === 'string' ? row.band_name.trim() : '';
      const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
      if (bandName) artists.add(bandName);
      if (personName) artists.add(personName);
    }
  } catch {
    // Membership evidence is optional.
  }

  return {
    artists: Array.from(artists),
    studios: Array.from(studios),
    venues: Array.from(venues),
    scenes: Array.from(scenes),
  };
}

export function getAssociatedArtistsByNode(
  type: 'studio' | 'venue' | 'scene',
  value: string,
  minPlaylistCount = 2,
  minTrackCount = 3
): string[] {
  let playlists: Omit<PlaylistRow, 'tracks'>[] = [];

  if (type === 'studio') {
    playlists = getStudioAtlas(value).playlists;
  } else if (type === 'venue') {
    playlists = getVenueAtlas(value).playlists;
  } else {
    playlists = getPlaylistsByScene(value);
  }

  const trackCounts = new Map<string, { name: string; count: number }>();
  const playlistCounts = new Map<string, Set<number>>();

  for (const playlist of playlists) {
    const full = getPlaylistById(playlist.id);
    if (!full) continue;

    const artists = getTrackArtists({ tracks: full.tracks });
    for (const artist of artists) {
      const key = artist.toLowerCase();

      const existing = trackCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        trackCounts.set(key, { name: artist, count: 1 });
      }

      const inPlaylists = playlistCounts.get(key) ?? new Set<number>();
      inPlaylists.add(playlist.id);
      playlistCounts.set(key, inPlaylists);
    }
  }

  return Array.from(trackCounts.entries())
    .filter(([key, valueData]) => {
      const playlistCount = playlistCounts.get(key)?.size ?? 0;
      return playlistCount >= minPlaylistCount || valueData.count >= minTrackCount;
    })
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 20)
    .map(([, valueData]) => valueData.name);
}

function getLocationAtlas(
  nodeValue: string,
  getValues: (row: PlaylistRow) => string[],
  nodeType: 'country' | 'city' | 'studio' | 'venue'
): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  relatedArtists: string[];
  relatedCountries: string[];
  relatedCities: string[];
  relatedStudios: string[];
  relatedVenues: string[];
} {
  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, created_at
    FROM playlists
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as PlaylistRow[];
  const target = nodeValue.trim().toLowerCase();

  const matched = rows.filter((row) => getValues(row).some((item) => item.toLowerCase() === target));

  const sceneCounts = new Map<string, number>();
  const artistCounts = new Map<string, { name: string; count: number }>();
  const countryCounts = new Map<string, number>();
  const cityCounts = new Map<string, number>();
  const studioCounts = new Map<string, number>();
  const venueCounts = new Map<string, number>();

  for (const row of matched) {
    const scenesForCount = nodeType === 'city'
      ? getDirectScenesForCity({ scenes: row.scenes, scene: row.scene, cities: row.cities, place: row.place, places: row.places }, nodeValue)
      : nodeType === 'country'
        ? getDirectScenesForCountry({ scenes: row.scenes, scene: row.scene, countries: row.countries, place: row.place, places: row.places }, nodeValue)
        : parseStringArray(row.scenes);

    for (const scene of scenesForCount) {
      sceneCounts.set(scene, (sceneCounts.get(scene) || 0) + 1);
    }

    for (const country of getNormalizedCountries(row)) {
      countryCounts.set(country, (countryCounts.get(country) || 0) + 1);
    }

    const rowCities = getNormalizedCities(row);
    const includeCitiesForRow = nodeType === 'studio'
      ? getNormalizedStudios(row).some((studio) => studio.toLowerCase() === target)
      : nodeType === 'venue'
        ? getNormalizedVenues(row).some((venue) => venue.toLowerCase() === target)
        : true;
    if (includeCitiesForRow) {
      for (const city of rowCities) {
        cityCounts.set(city, (cityCounts.get(city) || 0) + 1);
      }
    }

    const studiosForCount = nodeType === 'city'
      ? getDirectStudiosForCity({ place: row.place, places: row.places, cities: row.cities, studios: row.studios }, nodeValue)
      : getNormalizedStudios(row);
    for (const studio of studiosForCount) {
      studioCounts.set(studio, (studioCounts.get(studio) || 0) + 1);
    }

    const venuesForCount = nodeType === 'city'
      ? getDirectVenuesForCity({ place: row.place, places: row.places, cities: row.cities, venues: row.venues }, nodeValue)
      : getNormalizedVenues(row);
    for (const venue of venuesForCount) {
      venueCounts.set(venue, (venueCounts.get(venue) || 0) + 1);
    }

    for (const artist of getTrackArtists(row)) {
      const key = artist.toLowerCase();
      const existing = artistCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        artistCounts.set(key, { name: artist, count: 1 });
      }
    }
  }

  const playlists = matched.slice(0, 30).map(({ tracks, ...rest }) => rest);
  const scenes = Array.from(sceneCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => name);
  const relatedArtists = Array.from(artistCounts.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)
    .map((item) => item.name);
  const relatedCountries = Array.from(countryCounts.entries())
    .filter(([name]) => !(nodeType === 'country' && name.toLowerCase() === target))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => name);
  const relatedCities = nodeType === 'city'
    ? []
    : Array.from(cityCounts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 8)
        .map(([name]) => name);
  const relatedStudios = Array.from(studioCounts.entries())
    .filter(([name]) => !(nodeType === 'studio' && name.toLowerCase() === target))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => name);
  const relatedVenues = Array.from(venueCounts.entries())
    .filter(([name]) => !(nodeType === 'venue' && name.toLowerCase() === target))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => name);

  return { playlists, scenes, relatedArtists, relatedCountries, relatedCities, relatedStudios, relatedVenues };
}

function getNormalizedScenes(row: Pick<PlaylistRow, 'scenes' | 'scene'>): string[] {
  const scenes = parseStringArray(row.scenes);
  if (scenes.length > 0) return scenes;
  if (row.scene && row.scene.length > 0) return [row.scene];
  return [];
}

function getDirectScenesForCity(
  row: Pick<PlaylistRow, 'scenes' | 'scene' | 'cities' | 'place' | 'places'>,
  city: string
): string[] {
  const target = city.trim().toLowerCase();
  if (!target) return [];

  const scenes = getNormalizedScenes({ scenes: row.scenes, scene: row.scene });
  if (scenes.length === 0) return [];

  const cities = getNormalizedCities({ cities: row.cities, place: row.place, places: row.places });
  if (cities.length === 1 && cities[0].toLowerCase() === target) {
    return scenes;
  }

  return scenes.filter((scene) => scene.toLowerCase().includes(target));
}

function getDirectScenesForCountry(
  row: Pick<PlaylistRow, 'scenes' | 'scene' | 'countries' | 'place' | 'places'>,
  country: string
): string[] {
  const target = country.trim().toLowerCase();
  if (!target) return [];

  const scenes = getNormalizedScenes({ scenes: row.scenes, scene: row.scene });
  if (scenes.length === 0) return [];

  const countries = getNormalizedCountries({ countries: row.countries, place: row.place, places: row.places });
  if (countries.length === 1 && countries[0].toLowerCase() === target) {
    return scenes;
  }

  return scenes.filter((scene) => scene.toLowerCase().includes(target));
}

export function getPlaylistByPrompt(prompt: string): PlaylistRow | undefined {
  const stmt = db.prepare('SELECT * FROM playlists WHERE prompt = ?');
  return stmt.get(prompt) as PlaylistRow | undefined;
}

export function normalizePromptForCache(prompt: string): string {
  return prompt
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[.!?]+$/g, '')
    .trim();
}

export function getPlaylistByCacheKey(cacheKey: string): PlaylistRow | undefined {
  const normalizedKey = normalizePromptForCache(cacheKey);
  if (!normalizedKey) return undefined;

  const stmt = db.prepare('SELECT * FROM playlists ORDER BY created_at DESC');
  const rows = stmt.all() as PlaylistRow[];

  return rows.find((row) => normalizePromptForCache(row.prompt) === normalizedKey);
}

export function getAllPlaylists(): Omit<PlaylistRow, 'tracks'>[] {
  const stmt = db.prepare(`
    select id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at 
    FROM playlists 
    WHERE prompt NOT LIKE '[system]%'
    ORDER BY created_at DESC 
    LIMIT 20
  `);
  return stmt.all() as Omit<PlaylistRow, 'tracks'>[];
}

export function getPlaylistById(id: number): PlaylistRow | undefined {
  const stmt = db.prepare('SELECT * FROM playlists WHERE id = ?');
  return stmt.get(id) as PlaylistRow | undefined;
}

export function updatePlaylistTrackSpotifyUrls(
  playlistId: number,
  updates: Array<{ artist: string; song: string; spotifyUrl?: string; spotifyUri?: string }>
): number {
  if (!Number.isFinite(playlistId) || playlistId <= 0 || updates.length === 0) return 0;

  const row = getPlaylistById(playlistId);
  if (!row?.tracks) return 0;

  let parsedTracks: unknown;
  try {
    parsedTracks = JSON.parse(row.tracks);
  } catch {
    return 0;
  }
  if (!Array.isArray(parsedTracks)) return 0;

  const linksByRecordingKey = new Map<string, { spotifyUrl: string; spotifyUri: string }>();
  for (const item of updates) {
    const artist = canonicalizeDisplayName(item.artist || '');
    const title = String(item.song || '').trim();
    const spotifyUrl = normalizeSpotifyUrl(item.spotifyUrl || '');
    const spotifyUri = normalizeSpotifyUri(item.spotifyUri || '') || spotifyUriFromUrl(spotifyUrl);
    const key = buildRecordingCanonicalKey(artist, title);
    if (!key || (!spotifyUrl && !spotifyUri)) continue;
    linksByRecordingKey.set(key, { spotifyUrl, spotifyUri });
  }

  if (linksByRecordingKey.size === 0) return 0;

  let updatedCount = 0;
  const nextTracks = parsedTracks.map((track) => {
    if (!track || typeof track !== 'object') return track;
    const artist = typeof (track as { artist?: unknown }).artist === 'string'
      ? canonicalizeDisplayName((track as { artist: string }).artist)
      : '';
    const song = typeof (track as { song?: unknown }).song === 'string'
      ? (track as { song: string }).song.trim()
      : '';
    const key = buildRecordingCanonicalKey(artist, song);
    if (!key) return track;
    const links = linksByRecordingKey.get(key);
    if (!links) return track;

    const spotifyUrl = links.spotifyUrl || '';
    const spotifyUri = links.spotifyUri || '';

    const current = typeof (track as { spotify_url?: unknown }).spotify_url === 'string'
      ? (track as { spotify_url: string }).spotify_url.trim()
      : '';
    const currentUri = typeof (track as { spotify_uri?: unknown }).spotify_uri === 'string'
      ? normalizeSpotifyUri((track as { spotify_uri: string }).spotify_uri)
      : '';
    if (current === spotifyUrl && currentUri === spotifyUri) return track;
    updatedCount += 1;
    return {
      ...track,
      ...(spotifyUrl ? { spotify_url: spotifyUrl } : {}),
      ...(spotifyUri ? { spotify_uri: spotifyUri } : {}),
    };
  });

  if (updatedCount > 0) {
    db.prepare('UPDATE playlists SET tracks = ? WHERE id = ?').run(JSON.stringify(nextTracks), playlistId);
  }

  return updatedCount;
}

export function getRelatedPlaylists(playlistId: number): Omit<PlaylistRow, 'tracks'>[] {
  const current = getPlaylistById(playlistId);
  if (!current || !current.tags) return [];

  let currentTags: string[] = [];
  try {
    const parsed = JSON.parse(current.tags);
    if (Array.isArray(parsed)) {
      currentTags = parsed;
    }
  } catch {
    return [];
  }

  if (currentTags.length === 0) return [];

  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    WHERE id != ?
      AND prompt NOT LIKE '[system]%'
  `);

  const rows = stmt.all(playlistId) as Omit<PlaylistRow, 'tracks'>[];

  return rows
    .map((row) => {
      if (!row.tags) return { row, sharedCount: 0 };
      try {
        const parsed = JSON.parse(row.tags);
        if (!Array.isArray(parsed)) return { row, sharedCount: 0 };
        const sharedCount = parsed.filter((tag: string) => currentTags.includes(tag)).length;
        return { row, sharedCount };
      } catch {
        return { row, sharedCount: 0 };
      }
    })
    .filter((item) => item.sharedCount > 0)
    .sort((a, b) => {
      if (b.sharedCount !== a.sharedCount) {
        return b.sharedCount - a.sharedCount;
      }
      return Date.parse(b.row.created_at) - Date.parse(a.row.created_at);
    })
    .slice(0, 6)
    .map((item) => item.row);
}

export function getPlaylistsByTag(tag: string): Omit<PlaylistRow, 'tracks'>[] {
  const target = normalizeTagForComparison(formatTagForDisplay(tag));
  if (!target) return [];

  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    WHERE prompt NOT LIKE '[system]%'
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as Omit<PlaylistRow, 'tracks'>[];

  return rows
    .filter((row) => {
      if (!row.tags) return false;
      try {
        const parsed = JSON.parse(row.tags);
        if (!Array.isArray(parsed)) return false;
        return parsed.some((item: unknown) => {
          if (typeof item !== 'string') return false;
          const normalized = normalizeTagForComparison(formatTagForDisplay(item));
          return normalized === target;
        });
      } catch {
        return false;
      }
    })
    .slice(0, 30);
}

export function getPlaylistsByPlace(place: string): Omit<PlaylistRow, 'tracks'>[] {
  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    WHERE prompt NOT LIKE '[system]%'
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as Omit<PlaylistRow, 'tracks'>[];
  const target = place.toLowerCase();

  return rows
    .filter((row) => getNormalizedPlaces(row).some((item) => item.toLowerCase() === target))
    .slice(0, 30);
}

export function getPlaylistsByScene(scene: string): Omit<PlaylistRow, 'tracks'>[] {
  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    WHERE prompt NOT LIKE '[system]%'
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as Omit<PlaylistRow, 'tracks'>[];
  const target = scene.toLowerCase();

  return rows
    .filter((row) => getNormalizedScenes(row).some((item) => item.toLowerCase() === target))
    .slice(0, 30);
}

export function getCountryAtlas(country: string): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  relatedArtists: string[];
  relatedCountries: string[];
  relatedCities: string[];
  relatedStudios: string[];
  relatedVenues: string[];
} {
  return getLocationAtlas(country, (row) => getNormalizedCountries(row), 'country');
}

export function getCityAtlas(city: string): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  relatedArtists: string[];
  relatedCountries: string[];
  relatedCities: string[];
  relatedStudios: string[];
  relatedVenues: string[];
} {
  return getLocationAtlas(city, (row) => getNormalizedCities(row), 'city');
}

export function getStudioAtlas(studio: string): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  relatedArtists: string[];
  relatedStudios: string[];
  relatedVenues: string[];
  city: string | null;
  country: string | null;
  relatedEquipment: string[];
} {
  const debugStudioEquipment = process.env.DEBUG_STUDIO_EQUIPMENT === '1';
  let evidencePlaylists: Omit<PlaylistRow, 'tracks'>[] = [];
  const evidenceSceneCounts = new Map<string, { name: string; count: number }>();

  const stmt = db.prepare(`
    SELECT prompt, countries, cities, studios, place, places
    FROM playlists
    ORDER BY created_at DESC
  `);
  const rows = stmt.all() as Array<Pick<PlaylistRow, 'prompt' | 'countries' | 'cities' | 'studios' | 'place' | 'places'>>;

  const target = buildStudioCanonicalKey(studio);
  const cityCounts = new Map<string, { name: string; count: number }>();
  const countryCounts = new Map<string, { name: string; count: number }>();
  const cityCountryCounts = new Map<string, Map<string, { name: string; count: number }>>();
  const relatedArtistCounts = new Map<string, { name: string; count: number }>();
  const equipmentCounts = new Map<string, { name: string; count: number }>();
  let matchedStudioPlaylistsCount = 0;
  let matchedStudioRecordingsCount = 0;
  let matchedRecordingEvidenceCount = 0;

  for (const row of rows) {
    const rowStudios = getNormalizedStudios({ studios: row.studios, place: row.place, places: row.places, cities: row.cities });
    if (!rowStudios.some((item) => item.toLowerCase() === target)) continue;
    matchedStudioPlaylistsCount += 1;
    const rowCities = getNormalizedCities({ cities: row.cities, place: row.place, places: row.places });
    const rowCountries = getNormalizedCountries({ countries: row.countries, place: row.place, places: row.places });

    for (const city of rowCities) {
      const cityKey = city.toLowerCase();
      const existing = cityCounts.get(cityKey);
      if (existing) {
        existing.count += 1;
      } else {
        cityCounts.set(cityKey, { name: city, count: 1 });
      }

      if (rowCountries.length > 0) {
        if (!cityCountryCounts.has(cityKey)) {
          cityCountryCounts.set(cityKey, new Map());
        }
        const perCity = cityCountryCounts.get(cityKey)!;
        for (const country of rowCountries) {
          const countryKey = country.toLowerCase();
          const existingCountry = perCity.get(countryKey);
          if (existingCountry) {
            existingCountry.count += 1;
          } else {
            perCity.set(countryKey, { name: country, count: 1 });
          }
        }
      }
    }

    for (const country of rowCountries) {
      const countryKey = country.toLowerCase();
      const existing = countryCounts.get(countryKey);
      if (existing) {
        existing.count += 1;
      } else {
        countryCounts.set(countryKey, { name: country, count: 1 });
      }
    }
  }

  const strongestCity = Array.from(cityCounts.values())
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })[0]?.name || null;

  let selectedCountry: string | null = null;
  if (strongestCity) {
    const perCityCountries = cityCountryCounts.get(strongestCity.toLowerCase());
    if (perCityCountries && perCityCountries.size > 0) {
      selectedCountry = Array.from(perCityCountries.values())
        .sort((a, b) => {
          if (b.count !== a.count) return b.count - a.count;
          return a.name.localeCompare(b.name);
        })[0]?.name || null;
    }
  }

  if (!selectedCountry) {
    selectedCountry = Array.from(countryCounts.values())
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })[0]?.name || null;
  }

  try {
    const playlistIdRows = db.prepare(`
      SELECT DISTINCT source_playlist_id
      FROM recording_studio_evidence
      WHERE COALESCE(studio_name_canonical, lower(trim(studio_name))) = ?
    `).all(target) as Array<{ source_playlist_id: number | null }>;

    const sourcePlaylistIds = playlistIdRows
      .map((row) => (typeof row.source_playlist_id === 'number' ? row.source_playlist_id : NaN))
      .filter((value) => Number.isFinite(value));

    if (sourcePlaylistIds.length > 0) {
      const sourcePlaceholders = sourcePlaylistIds.map(() => '?').join(', ');
      const playlistStmt = db.prepare(`
        SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, equipment, created_at
        FROM playlists
        WHERE id IN (${sourcePlaceholders})
        ORDER BY created_at DESC
        LIMIT 30
      `);
      evidencePlaylists = playlistStmt.all(...sourcePlaylistIds) as Omit<PlaylistRow, 'tracks'>[];

      for (const playlistRow of evidencePlaylists) {
        for (const sceneName of getNormalizedScenes({ scenes: playlistRow.scenes, scene: playlistRow.scene })) {
          const key = sceneName.toLowerCase();
          const existing = evidenceSceneCounts.get(key);
          if (existing) {
            existing.count += 1;
          } else {
            evidenceSceneCounts.set(key, { name: sceneName, count: 1 });
          }
        }
      }
    }

    const recordingIdRows = db.prepare(`
      SELECT DISTINCT recording_id
      FROM recording_studio_evidence
      WHERE COALESCE(studio_name_canonical, lower(trim(studio_name))) = ?
    `).all(target) as Array<{ recording_id: number | null }>;

    const recordingIds = recordingIdRows
      .map((row) => (typeof row.recording_id === 'number' ? row.recording_id : NaN))
      .filter((value) => Number.isFinite(value));

    matchedStudioRecordingsCount = recordingIds.length;

    if (recordingIds.length > 0) {
      const placeholders = recordingIds.map(() => '?').join(', ');

      const artistRows = db.prepare(`
        SELECT artist
        FROM recordings
        WHERE id IN (${placeholders})
      `).all(...recordingIds) as Array<{ artist: unknown }>;

      for (const row of artistRows) {
        const artistName = typeof row.artist === 'string' ? row.artist.trim() : '';
        if (!artistName) continue;
        const key = artistName.toLowerCase();
        const existing = relatedArtistCounts.get(key);
        if (existing) {
          existing.count += 1;
        } else {
          relatedArtistCounts.set(key, { name: artistName, count: 1 });
        }
      }

      const evidenceRows = db.prepare(`
        SELECT equipment_name
        FROM recording_equipment_evidence
        WHERE recording_id IN (${placeholders})
      `).all(...recordingIds) as Array<{ equipment_name: unknown }>;

      matchedRecordingEvidenceCount = evidenceRows.length;

      for (const row of evidenceRows) {
        const equipmentName = typeof row.equipment_name === 'string' ? canonicalizeEquipmentName(row.equipment_name) : '';
        if (!equipmentName) continue;
        if (isGenericEquipmentName(equipmentName)) continue;
        const key = equipmentName.toLowerCase();
        const existing = equipmentCounts.get(key);
        if (existing) {
          existing.count += 1;
        } else {
          equipmentCounts.set(key, { name: equipmentName, count: 1 });
        }
      }
    }
  } catch {
    // Skip malformed studio/recording evidence rows quietly
  }

  const relatedArtists = Array.from(relatedArtistCounts.values())
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);

  const relatedEquipment = Array.from(equipmentCounts.values())
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const scenes = Array.from(evidenceSceneCounts.values())
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);

  if (debugStudioEquipment) {
    console.log(`[studio-equipment-debug] studio="${studio}" matched_playlists=${matchedStudioPlaylistsCount} matched_studio_recordings=${matchedStudioRecordingsCount} matched_recording_evidence=${matchedRecordingEvidenceCount} final_related_equipment_count=${relatedEquipment.length}`);
    console.log(`[studio-equipment-debug] related_equipment_values=${relatedEquipment.join(', ')}`);
  }

  return {
    playlists: evidencePlaylists,
    scenes,
    relatedArtists,
    relatedStudios: [],
    relatedVenues: [],
    city: strongestCity,
    country: selectedCountry,
    relatedEquipment,
  };
}

export function getVenueAtlas(venue: string): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  relatedArtists: string[];
  relatedCountries: string[];
  relatedCities: string[];
  relatedStudios: string[];
  relatedVenues: string[];
} {
  return getLocationAtlas(venue, (row) => getNormalizedVenues(row), 'venue');
}

export function getEquipmentAtlas(name: string): {
  name: string;
  category: string;
  playlists: Omit<PlaylistRow, 'tracks'>[];
  relatedArtists: string[];
  relatedScenes: string[];
  relatedStudios: string[];
  relatedArtistsEvidence?: { relationType: string; evidenceCount: number };
  relatedStudiosEvidence?: { relationType: string; evidenceCount: number };
} {
  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, equipment, created_at
    FROM playlists
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as PlaylistRow[];
  const target = canonicalizeEquipmentName(name).toLowerCase();
  if (!target) {
    return { name: name.trim(), category: '', playlists: [], relatedArtists: [], relatedScenes: [], relatedStudios: [] };
  }

  const playlists: Omit<PlaylistRow, 'tracks'>[] = [];
  const artistCounts = new Map<string, { name: string; count: number }>();
  const sceneCounts = new Map<string, { playlistOccurrence: number; sceneOccurrence: number }>();
  const studioCounts = new Map<string, { name: string; count: number }>();
  let displayName = name.trim();
  let category = '';
  let strongestArtistEvidence = 0;
  let strongestStudioEvidence = 0;

  for (const row of rows) {
    const equipmentEntries = parseCanonicalEquipmentEntries(row.equipment);
    const matchedEntryIndex = equipmentEntries.findIndex((item) => item.name.toLowerCase() === target);
    if (matchedEntryIndex < 0) continue;

    const isPrimaryByFirst = equipmentEntries[0]?.name.toLowerCase() === target;
    const isPrimaryByPrompt = normalizePromptForCache(row.prompt || '').includes(target);
    if (!isPrimaryByFirst && !isPrimaryByPrompt) continue;

    const matchedEntry = equipmentEntries[matchedEntryIndex];
    if (!displayName) displayName = matchedEntry.name;
    if (!category) category = matchedEntry.category;

    playlists.push((({ tracks, ...rest }) => rest)(row));

    if (playlists.length >= 30) break;
  }

  try {
    const artistEvidenceRows = db.prepare(`
      SELECT r.artist AS artist_name
      FROM recording_equipment_evidence ree
      INNER JOIN recordings r ON r.id = ree.recording_id
      WHERE lower(trim(ree.equipment_name)) = ?
    `).all(target) as Array<{ artist_name: string | null }>;

    for (const row of artistEvidenceRows) {
      const artistName = typeof row.artist_name === 'string' ? row.artist_name.trim() : '';
      if (!artistName) continue;
      const key = artistName.toLowerCase();
      const existing = artistCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        artistCounts.set(key, { name: artistName, count: 1 });
      }
      if ((artistCounts.get(key)?.count || 0) > strongestArtistEvidence) {
        strongestArtistEvidence = artistCounts.get(key)!.count;
      }
    }

    const studioEvidenceRows = db.prepare(`
      SELECT DISTINCT
        ree.recording_id AS recording_id,
        rse.studio_name AS studio_name,
        COALESCE(rse.studio_name_canonical, lower(trim(rse.studio_name))) AS studio_key
      FROM recording_equipment_evidence ree
      INNER JOIN recording_studio_evidence rse ON rse.recording_id = ree.recording_id
      WHERE lower(trim(ree.equipment_name)) = ?
    `).all(target) as Array<{ recording_id: number | null; studio_name: string | null; studio_key: string | null }>;

    for (const row of studioEvidenceRows) {
      const studioName = typeof row.studio_name === 'string' ? row.studio_name.trim() : '';
      if (!studioName) continue;
      const key = typeof row.studio_key === 'string' && row.studio_key.trim().length > 0
        ? row.studio_key.trim()
        : studioName.toLowerCase();
      const existing = studioCounts.get(key);
      if (existing) {
        existing.count += 1;
        existing.name = choosePreferredDisplayValue(existing.name, studioName);
        if (existing.count > strongestStudioEvidence) {
          strongestStudioEvidence = existing.count;
        }
      } else {
        studioCounts.set(key, { name: studioName, count: 1 });
        if (strongestStudioEvidence < 1) {
          strongestStudioEvidence = 1;
        }
      }
    }

    const sourcePlaylistRows = db.prepare(`
      SELECT DISTINCT source_playlist_id
      FROM recording_equipment_evidence
      WHERE lower(trim(equipment_name)) = ?
    `).all(target) as Array<{ source_playlist_id: number | null }>;

    const sourcePlaylistIds = sourcePlaylistRows
      .map((row) => (typeof row.source_playlist_id === 'number' ? row.source_playlist_id : NaN))
      .filter((value) => Number.isFinite(value));

    if (sourcePlaylistIds.length > 0) {
      const sourcePlaceholders = sourcePlaylistIds.map(() => '?').join(', ');
      const sceneRows = db.prepare(`
        SELECT scenes, scene
        FROM playlists
        WHERE id IN (${sourcePlaceholders})
      `).all(...sourcePlaylistIds) as Array<Pick<PlaylistRow, 'scenes' | 'scene'>>;

      for (const row of sceneRows) {
        const scenesInRow = getNormalizedScenes({ scenes: row.scenes, scene: row.scene });
        const seenScenesInPlaylist = new Set<string>();
        for (const scene of scenesInRow) {
          const key = scene.toLowerCase();
          const existing = sceneCounts.get(key);
          if (existing) {
            existing.sceneOccurrence += 1;
            if (!seenScenesInPlaylist.has(key)) {
              existing.playlistOccurrence += 1;
              seenScenesInPlaylist.add(key);
            }
          } else {
            sceneCounts.set(key, { playlistOccurrence: 1, sceneOccurrence: 1 });
            seenScenesInPlaylist.add(key);
          }
        }
      }
    }
  } catch {
    // Skip malformed studio evidence rows quietly
  }

  return {
    name: displayName,
    category,
    playlists,
    relatedArtists: Array.from(artistCounts.values())
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 8)
      .map((item) => item.name),
    relatedScenes: Array.from(sceneCounts.entries())
      .sort((a, b) => {
        const strongWeight = 3;
        const aPlaylistOccurrence = a[1].playlistOccurrence;
        const bPlaylistOccurrence = b[1].playlistOccurrence;
        const aSceneOccurrence = a[1].sceneOccurrence;
        const bSceneOccurrence = b[1].sceneOccurrence;
        const aScore = (strongWeight * aPlaylistOccurrence) + aSceneOccurrence;
        const bScore = (strongWeight * bPlaylistOccurrence) + bSceneOccurrence;
        if (bScore !== aScore) return bScore - aScore;
        if (bPlaylistOccurrence !== aPlaylistOccurrence) return bPlaylistOccurrence - aPlaylistOccurrence;
        if (bSceneOccurrence !== aSceneOccurrence) return bSceneOccurrence - aSceneOccurrence;
        return a[0].localeCompare(b[0]);
      })
      .slice(0, 8)
      .map(([item]) => item),
    relatedStudios: Array.from(studioCounts.values())
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 8)
      .map((item) => item.name),
    relatedArtistsEvidence: strongestArtistEvidence > 0
      ? { relationType: 'recording_equipment_evidence', evidenceCount: strongestArtistEvidence }
      : undefined,
    relatedStudiosEvidence: strongestStudioEvidence > 0
      ? { relationType: 'studio_association', evidenceCount: strongestStudioEvidence }
      : undefined,
  };
}

export function getTopTags(limit = 12): Array<{ tag: string; count: number }> {
  const stmt = db.prepare("SELECT tags FROM playlists WHERE prompt NOT LIKE '[system]%'");
  const rows = stmt.all() as Array<{ tags: string | null }>;

  const counts = new Map<string, { tag: string; count: number }>();

  for (const row of rows) {
    if (!row.tags) continue;
    try {
      const parsed = JSON.parse(row.tags);
      if (!Array.isArray(parsed)) continue;
      for (const tag of parsed) {
        if (typeof tag !== 'string' || tag.length === 0) continue;
        const cleaned = formatTagForDisplay(tag);
        const key = normalizeTagForComparison(cleaned);
        if (!cleaned || !key || cleaned.startsWith('system')) continue;
        const existing = counts.get(key);
        if (existing) {
          existing.count += 1;
        } else {
          counts.set(key, { tag: cleaned, count: 1 });
        }
      }
    } catch {
      continue;
    }
  }

  return Array.from(counts.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, limit);
}

export function getKnownTags(): string[] {
  const stmt = db.prepare("SELECT tags FROM playlists WHERE prompt NOT LIKE '[system]%'");
  const rows = stmt.all() as Array<{ tags: string | null }>;
  const known = new Map<string, string>();

  for (const row of rows) {
    if (!row.tags) continue;
    try {
      const parsed = JSON.parse(row.tags);
      if (!Array.isArray(parsed)) continue;
      for (const tag of parsed) {
        if (typeof tag !== 'string') continue;
        const cleaned = formatTagForDisplay(tag);
        const key = normalizeTagForComparison(cleaned);
        if (!cleaned || !key || cleaned.startsWith('system')) continue;
        if (!known.has(key)) known.set(key, cleaned);
      }
    } catch {
      continue;
    }
  }

  return Array.from(known.values());
}

function normalizeTagForComparison(tag: string): string {
  return tag
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function formatTagForDisplay(tag: string): string {
  return canonicalizeDisplayName(tag)
    .replace(/[_-]+/g, ' ')
    .replace(/[.,;:!?/\\|()[\]{}]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

export function getDuplicateTagCandidates(): Array<{ normalized: string; tags: string[] }> {
  const knownTags = getKnownTags();
  const groups = new Map<string, Set<string>>();

  for (const tag of knownTags) {
    const normalized = normalizeTagForComparison(tag);
    if (!normalized) continue;

    const existing = groups.get(normalized) ?? new Set<string>();
    existing.add(tag);
    groups.set(normalized, existing);
  }

  return Array.from(groups.entries())
    .filter(([, tags]) => tags.size > 1)
    .map(([normalized, tags]) => ({
      normalized,
      tags: Array.from(tags).sort((a, b) => a.localeCompare(b)),
    }))
    .sort((a, b) => a.normalized.localeCompare(b.normalized));
}

export function mergeTagExact(source: string, target: string): { updatedPlaylists: number } {
  const sourceTag = source.trim();
  const targetTag = target.trim();

  if (!sourceTag || !targetTag || sourceTag === targetTag) {
    return { updatedPlaylists: 0 };
  }

  const selectStmt = db.prepare("SELECT id, tags FROM playlists WHERE prompt NOT LIKE '[system]%'");
  const updateStmt = db.prepare('UPDATE playlists SET tags = ? WHERE id = ?');
  const rows = selectStmt.all() as Array<{ id: number; tags: string | null }>;

  let updatedPlaylists = 0;

  for (const row of rows) {
    if (!row.tags) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(row.tags);
    } catch {
      continue;
    }

    if (!Array.isArray(parsed)) continue;

    const rawTags = parsed.filter((item): item is string => typeof item === 'string');
    if (rawTags.length === 0) continue;

    let changed = false;
    const replaced = rawTags.map((tag) => {
      if (tag === sourceTag) {
        changed = true;
        return targetTag;
      }
      return tag;
    });

    if (!changed) continue;

    const deduped: string[] = [];
    const seen = new Set<string>();
    for (const tag of replaced) {
      if (!seen.has(tag)) {
        seen.add(tag);
        deduped.push(tag);
      }
    }

    updateStmt.run(JSON.stringify(deduped), row.id);
    updatedPlaylists += 1;
  }

  return { updatedPlaylists };
}

export function getTagStats(): Array<{ tag: string; count: number }> {
  const stmt = db.prepare("SELECT tags FROM playlists WHERE prompt NOT LIKE '[system]%'");
  const rows = stmt.all() as Array<{ tags: string | null }>;
  const counts = new Map<string, { tag: string; count: number }>();

  for (const row of rows) {
    if (!row.tags) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(row.tags);
    } catch {
      continue;
    }

    if (!Array.isArray(parsed)) continue;

    for (const item of parsed) {
      if (typeof item !== 'string') continue;
      const cleaned = formatTagForDisplay(item);
      const key = normalizeTagForComparison(cleaned);
      if (!cleaned || !key || cleaned.startsWith('system')) continue;
      const existing = counts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        counts.set(key, { tag: cleaned, count: 1 });
      }
    }
  }

  return Array.from(counts.values())
    .sort((a, b) => b.count - a.count);
}

export function getArtistAtlas(artist: string): {
  playlists: Omit<PlaylistRow, 'tracks'>[];
  scenes: string[];
  places: string[];
  memberOf: string[];
  relatedStudios: string[];
  relatedArtists: string[];
  relatedCredits: Array<{ name: string; roles: string[] }>;
  relatedEquipment: string[];
  relatedArtistsEvidence?: { relationType: string; evidenceCount: number };
} {
  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as PlaylistRow[];
  const targetArtist = artist.trim().toLowerCase();
  const targetArtistCanonical = buildArtistCanonicalKey(artist);
  const targetPersonCanonical = buildPersonCanonicalKey(artist);

  const matched = rows.filter((row) => {
    try {
      const tracks = JSON.parse(row.tracks);
      if (!Array.isArray(tracks)) return false;
      return tracks.some((track) => {
        if (!track || typeof track !== 'object') return false;
        if (typeof track.artist !== 'string') return false;
        return track.artist.trim().toLowerCase() === targetArtist;
      });
    } catch {
      return false;
    }
  });

  const placeCounts = new Map<string, number>();
  const studioCounts = new Map<string, { name: string; count: number }>();
  const sceneCounts = new Map<string, { name: string; playlistEvidence: number; edgeEvidence: number }>();
  const relatedArtistCounts = new Map<string, { name: string; playlistOccurrence: number; trackOccurrence: number; edgeEvidence: number; sharedCreditEvidence: number; sharedStudioEvidence: number; sharedMembershipEvidence: number }>();
  const relatedCreditCounts = new Map<string, { name: string; count: number; roles: Set<string> }>();
  const relatedEquipmentCounts = new Map<string, { name: string; count: number }>();
  const memberOfCounts = new Map<string, { name: string; count: number }>();
  let strongestRelatedArtistEvidence = 0;

  for (const row of matched) {
    let hasExplicitTargetArtistInTracks = false;
    const targetArtistRecordingKeysInRow = new Set<string>();
    try {
      const parsedTracks = JSON.parse(row.tracks);
      if (Array.isArray(parsedTracks)) {
        for (const track of parsedTracks) {
          if (!track || typeof track !== 'object' || typeof track.artist !== 'string') continue;
          const trackArtist = track.artist.trim();
          if (!trackArtist || trackArtist.toLowerCase() !== targetArtist) continue;

          hasExplicitTargetArtistInTracks = true;

          const title = typeof (track as { song?: unknown }).song === 'string'
            ? (track as { song: string }).song.trim()
            : typeof (track as { title?: unknown }).title === 'string'
              ? (track as { title: string }).title.trim()
              : '';
          if (!title) continue;

          const canonicalKey = buildRecordingCanonicalKey(trackArtist, title);
          if (!canonicalKey) continue;
          targetArtistRecordingKeysInRow.add(canonicalKey);
        }
      }
    } catch {
      hasExplicitTargetArtistInTracks = false;
    }

    if (hasExplicitTargetArtistInTracks) {
      for (const place of [
        ...getNormalizedCountries(row),
        ...getNormalizedCities(row),
        ...getNormalizedVenues(row)
      ]) {
        placeCounts.set(place, (placeCounts.get(place) || 0) + 1);
      }
    }

    try {
      const tracks = JSON.parse(row.tracks);
      if (!Array.isArray(tracks)) continue;

      let hasTargetArtist = false;
      const coArtistsInPlaylist = new Map<string, string>();

      for (const track of tracks) {
        if (!track || typeof track !== 'object' || typeof track.artist !== 'string') continue;
        const name = track.artist.trim();
        if (!name) continue;

        const key = name.toLowerCase();
        if (key === targetArtist) {
          hasTargetArtist = true;
          continue;
        }

        if (!coArtistsInPlaylist.has(key)) {
          coArtistsInPlaylist.set(key, name);
        }
      }

      if (!hasTargetArtist) continue;

      for (const [key, displayName] of coArtistsInPlaylist.entries()) {
        const existing = relatedArtistCounts.get(key);
        if (existing) {
          existing.playlistOccurrence += 1;
        } else {
          relatedArtistCounts.set(key, { name: displayName, playlistOccurrence: 1, trackOccurrence: 0, edgeEvidence: 0, sharedCreditEvidence: 0, sharedStudioEvidence: 0, sharedMembershipEvidence: 0 });
        }
      }

      for (const track of tracks) {
        if (!track || typeof track !== 'object' || typeof track.artist !== 'string') continue;
        const name = track.artist.trim();
        if (!name) continue;
        const key = name.toLowerCase();
        if (key === targetArtist) continue;

        const existing = relatedArtistCounts.get(key);
        if (existing) {
          existing.trackOccurrence += 1;
        }
      }
    } catch {
      continue;
    }

    if (row.credits) {
      try {
        const parsedCredits = JSON.parse(row.credits);
        if (Array.isArray(parsedCredits)) {
          const seenInPlaylist = new Set<string>();

          for (const item of parsedCredits) {
            if (!item || typeof item !== 'object') continue;

            const creditName = typeof item.name === 'string' ? item.name.trim() : '';
            const role = typeof item.role === 'string' ? item.role.trim() : '';
            if (!creditName || !role) continue;

            const key = creditName.toLowerCase();
            const existing = relatedCreditCounts.get(key);
            if (existing) {
              existing.roles.add(role);
              if (!seenInPlaylist.has(key)) {
                existing.count += 1;
                seenInPlaylist.add(key);
              }
            } else {
              relatedCreditCounts.set(key, {
                name: creditName,
                count: 1,
                roles: new Set<string>([role]),
              });
              seenInPlaylist.add(key);
            }
          }
        }
      } catch {
        // Skip malformed credits safely
      }
    }
  }

  try {
    const canonicalKeys = new Set<string>();

    for (const row of matched) {
      try {
        const parsedTracks = JSON.parse(row.tracks);
        if (!Array.isArray(parsedTracks)) continue;

        for (const track of parsedTracks) {
          if (!track || typeof track !== 'object') continue;

          const trackArtist = typeof (track as { artist?: unknown }).artist === 'string'
            ? (track as { artist: string }).artist.trim().toLowerCase()
            : '';
          if (trackArtist !== targetArtist) continue;

          const title = typeof (track as { song?: unknown }).song === 'string'
            ? (track as { song: string }).song.trim()
            : typeof (track as { title?: unknown }).title === 'string'
              ? (track as { title: string }).title.trim()
              : '';
          if (!title) continue;

          const canonicalKey = buildRecordingCanonicalKey(artist, title);
          if (!canonicalKey) continue;
          canonicalKeys.add(canonicalKey);
        }
      } catch {
        // Skip malformed track rows quietly
      }
    }

    if (canonicalKeys.size > 0) {
      const selectRecordingStmt = db.prepare('SELECT id FROM recordings WHERE canonical_key = ?');
      const selectEvidenceStmt = db.prepare('SELECT equipment_name FROM recording_equipment_evidence WHERE recording_id = ?');
      const selectStudioEvidenceStmt = db.prepare(`
        SELECT studio_name, COALESCE(studio_name_canonical, lower(trim(studio_name))) AS studio_key
        FROM recording_studio_evidence
        WHERE recording_id = ?
      `);

      for (const canonicalKey of canonicalKeys) {
        const recordingRow = selectRecordingStmt.get(canonicalKey) as { id: number } | undefined;
        if (!recordingRow || typeof recordingRow.id !== 'number') continue;

        const studioEvidenceRows = selectStudioEvidenceStmt.all(recordingRow.id) as Array<{ studio_name: string | null; studio_key: string | null }>;
        const seenStudiosForRecording = new Set<string>();
        for (const evidence of studioEvidenceRows) {
          const studioName = typeof evidence.studio_name === 'string' ? evidence.studio_name.trim() : '';
          if (!studioName) continue;
          const key = typeof evidence.studio_key === 'string' && evidence.studio_key.trim().length > 0
            ? evidence.studio_key.trim()
            : studioName.toLowerCase();
          if (seenStudiosForRecording.has(key)) continue;
          seenStudiosForRecording.add(key);
          const existing = studioCounts.get(key);
          if (existing) {
            existing.count += 1;
            existing.name = choosePreferredDisplayValue(existing.name, studioName);
          } else {
            studioCounts.set(key, { name: studioName, count: 1 });
          }
        }

        const evidenceRows = selectEvidenceStmt.all(recordingRow.id) as Array<{ equipment_name: string | null }>;
        for (const evidence of evidenceRows) {
          const rawName = typeof evidence.equipment_name === 'string' ? evidence.equipment_name.trim() : '';
          if (!rawName) continue;

          const canonicalName = canonicalizeEquipmentName(rawName);
          if (!canonicalName) continue;
          if (isGenericEquipmentName(canonicalName)) continue;

          const key = canonicalName.toLowerCase();
          const existing = relatedEquipmentCounts.get(key);
          if (existing) {
            existing.count += 1;
          } else {
            relatedEquipmentCounts.set(key, { name: canonicalName, count: 1 });
          }
        }
      }
    }
  } catch {
    // Skip malformed recording/equipment rows quietly
  }

  try {
    const artistToEquipmentToSceneStmt = db.prepare(`
      SELECT
        es.to_name AS scene_name,
        ae.evidence_count AS artist_equipment_evidence,
        es.evidence_count AS equipment_scene_evidence
      FROM atlas_edges ae
      INNER JOIN atlas_edges es
        ON lower(trim(es.from_name)) = lower(trim(ae.to_name))
      WHERE ae.from_type = 'artist'
        AND ae.to_type = 'equipment'
        AND ae.relation_type = 'used_by_artist'
        AND lower(trim(ae.from_name)) = ?
        AND es.from_type = 'equipment'
        AND es.to_type = 'scene'
        AND es.relation_type = 'associated_with_scene'
    `);

    const edgeRows = artistToEquipmentToSceneStmt.all(targetArtist) as Array<{
      scene_name: string;
      artist_equipment_evidence: number | null;
      equipment_scene_evidence: number | null;
    }>;

    for (const row of edgeRows) {
      if (!row || typeof row !== 'object') continue;
      const sceneName = typeof row.scene_name === 'string' ? row.scene_name.trim() : '';
      if (!sceneName) continue;

      const aeEvidenceRaw = typeof row.artist_equipment_evidence === 'number' ? row.artist_equipment_evidence : 1;
      const esEvidenceRaw = typeof row.equipment_scene_evidence === 'number' ? row.equipment_scene_evidence : 1;
      const aeEvidence = Math.max(1, Math.min(20, Math.floor(aeEvidenceRaw)));
      const esEvidence = Math.max(1, Math.min(20, Math.floor(esEvidenceRaw)));
      const edgeContribution = aeEvidence + esEvidence;

      const key = sceneName.toLowerCase();
      const existing = sceneCounts.get(key);
      if (existing) {
        existing.edgeEvidence += edgeContribution;
      }
    }
  } catch {
    // Skip malformed edge rows quietly
  }

  try {
    const edgeStmt = db.prepare(`
      SELECT to_name, evidence_count
      FROM atlas_edges
      WHERE from_type = 'artist'
        AND to_type = 'artist'
        AND relation_type = 'co_artist'
        AND lower(trim(from_name)) = ?
    `);
    const edgeRows = edgeStmt.all(targetArtist) as Array<{ to_name: string; evidence_count: number | null }>;

    for (const row of edgeRows) {
      if (!row || typeof row !== 'object') continue;
      const relatedName = typeof row.to_name === 'string' ? row.to_name.trim() : '';
      if (!relatedName) continue;

      const key = relatedName.toLowerCase();
      if (key === targetArtist) continue;

      const evidenceRaw = typeof row.evidence_count === 'number' ? row.evidence_count : 1;
      const evidenceCount = Math.max(1, Math.min(20, Math.floor(evidenceRaw)));
      if (evidenceCount > strongestRelatedArtistEvidence) {
        strongestRelatedArtistEvidence = evidenceCount;
      }

      const existing = relatedArtistCounts.get(key);
      if (existing) {
        existing.edgeEvidence += evidenceCount;
      } else {
        relatedArtistCounts.set(key, {
          name: relatedName,
          playlistOccurrence: 0,
          trackOccurrence: 0,
          edgeEvidence: evidenceCount,
          sharedCreditEvidence: 0,
          sharedStudioEvidence: 0,
          sharedMembershipEvidence: 0,
        });
      }
    }
  } catch {
    // Skip malformed edge rows quietly
  }

  try {
    const targetCanonicalKeys = new Set<string>();
    const selectRecordingStmt = db.prepare('SELECT id FROM recordings WHERE canonical_key = ?');

    for (const row of matched) {
      try {
        const parsedTracks = JSON.parse(row.tracks);
        if (!Array.isArray(parsedTracks)) continue;

        for (const track of parsedTracks) {
          if (!track || typeof track !== 'object') continue;
          const trackArtist = typeof (track as { artist?: unknown }).artist === 'string'
            ? (track as { artist: string }).artist.trim().toLowerCase()
            : '';
          if (trackArtist !== targetArtist) continue;

          const title = typeof (track as { song?: unknown }).song === 'string'
            ? (track as { song: string }).song.trim()
            : typeof (track as { title?: unknown }).title === 'string'
              ? (track as { title: string }).title.trim()
              : '';
          if (!title) continue;

          const canonicalKey = buildRecordingCanonicalKey(artist, title);
          if (canonicalKey) targetCanonicalKeys.add(canonicalKey);
        }
      } catch {
        // Skip malformed track rows quietly
      }
    }

    const targetRecordingIds: number[] = [];
    for (const key of targetCanonicalKeys) {
      const row = selectRecordingStmt.get(key) as { id: number } | undefined;
      if (row && typeof row.id === 'number') targetRecordingIds.push(row.id);
    }

    if (targetRecordingIds.length > 0) {
      const recordingPlaceholders = targetRecordingIds.map(() => '?').join(', ');
      const targetCreditRows = db.prepare(`
        SELECT DISTINCT COALESCE(credit_name_canonical, lower(trim(credit_name))) AS credit_key
        FROM recording_credit_evidence
        WHERE recording_id IN (${recordingPlaceholders})
      `).all(...targetRecordingIds) as Array<{ credit_key: string | null }>;
      const targetStudioRows = db.prepare(`
        SELECT DISTINCT COALESCE(studio_name_canonical, lower(trim(studio_name))) AS studio_key
        FROM recording_studio_evidence
        WHERE recording_id IN (${recordingPlaceholders})
      `).all(...targetRecordingIds) as Array<{ studio_key: string | null }>;

      const targetCreditKeys = targetCreditRows
        .map((row) => (typeof row.credit_key === 'string' ? row.credit_key.trim() : ''))
        .filter((value) => value.length > 0);
      const targetStudioKeys = targetStudioRows
        .map((row) => (typeof row.studio_key === 'string' ? row.studio_key.trim() : ''))
        .filter((value) => value.length > 0);

      if (targetCreditKeys.length > 0) {
        const creditPlaceholders = targetCreditKeys.map(() => '?').join(', ');
        const sharedCreditRows = db.prepare(`
          SELECT r.artist AS artist_name, COUNT(DISTINCT COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name)))) AS shared_count
          FROM recordings r
          INNER JOIN recording_credit_evidence rce ON rce.recording_id = r.id
          WHERE lower(trim(r.artist)) != ?
            AND COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) IN (${creditPlaceholders})
          GROUP BY lower(trim(r.artist)), r.artist
        `).all(targetArtist, ...targetCreditKeys) as Array<{ artist_name: string | null; shared_count: number | null }>;

        for (const row of sharedCreditRows) {
          const artistName = typeof row.artist_name === 'string' ? row.artist_name.trim() : '';
          if (!artistName) continue;
          const key = artistName.toLowerCase();
          const shared = typeof row.shared_count === 'number' ? Math.max(0, Math.floor(row.shared_count)) : 0;
          const existing = relatedArtistCounts.get(key);
          if (existing) {
            existing.sharedCreditEvidence += shared;
          } else {
            relatedArtistCounts.set(key, {
              name: artistName,
              playlistOccurrence: 0,
              trackOccurrence: 0,
              edgeEvidence: 0,
              sharedCreditEvidence: shared,
              sharedStudioEvidence: 0,
              sharedMembershipEvidence: 0,
            });
          }
        }
      }

      if (targetStudioKeys.length > 0) {
        const studioPlaceholders = targetStudioKeys.map(() => '?').join(', ');
        const sharedStudioRows = db.prepare(`
          SELECT r.artist AS artist_name, COUNT(DISTINCT lower(trim(rse.studio_name))) AS shared_count
          FROM recordings r
          INNER JOIN recording_studio_evidence rse ON rse.recording_id = r.id
          WHERE lower(trim(r.artist)) != ?
            AND lower(trim(rse.studio_name)) IN (${studioPlaceholders})
          GROUP BY lower(trim(r.artist)), r.artist
        `).all(targetArtist, ...targetStudioKeys) as Array<{ artist_name: string | null; shared_count: number | null }>;

        for (const row of sharedStudioRows) {
          const artistName = typeof row.artist_name === 'string' ? row.artist_name.trim() : '';
          if (!artistName) continue;
          const key = artistName.toLowerCase();
          const shared = typeof row.shared_count === 'number' ? Math.max(0, Math.floor(row.shared_count)) : 0;
          const existing = relatedArtistCounts.get(key);
          if (existing) {
            existing.sharedStudioEvidence += shared;
          } else {
            relatedArtistCounts.set(key, {
              name: artistName,
              playlistOccurrence: 0,
              trackOccurrence: 0,
              edgeEvidence: 0,
              sharedCreditEvidence: 0,
              sharedStudioEvidence: shared,
              sharedMembershipEvidence: 0,
            });
          }
        }
      }

      const membershipBands = db.prepare(`
        SELECT DISTINCT band_name, COALESCE(band_name_canonical, lower(trim(band_name))) AS band_key
        FROM artist_membership_evidence
        WHERE COALESCE(person_name_canonical, lower(trim(person_name))) = ?
      `).all(targetPersonCanonical) as Array<{ band_name: string | null; band_key: string | null }>;
      for (const row of membershipBands) {
        const bandKey = typeof row.band_key === 'string' ? row.band_key.trim() : '';
        if (!bandKey) continue;
        const existing = relatedArtistCounts.get(bandKey);
        if (existing) {
          existing.sharedMembershipEvidence += 1;
        } else {
          relatedArtistCounts.set(bandKey, {
            name: typeof row.band_name === 'string' ? row.band_name.trim() : bandKey,
            playlistOccurrence: 0,
            trackOccurrence: 0,
            edgeEvidence: 0,
            sharedCreditEvidence: 0,
            sharedStudioEvidence: 0,
            sharedMembershipEvidence: 1,
          });
        }
      }

      const membershipMembers = db.prepare(`
        SELECT DISTINCT person_name
        FROM artist_membership_evidence
        WHERE COALESCE(band_name_canonical, lower(trim(band_name))) = ?
      `).all(targetArtistCanonical) as Array<{ person_name: string | null }>;
      for (const row of membershipMembers) {
        const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
        if (!personName) continue;
        const key = buildPersonCanonicalKey(personName);
        if (key === targetPersonCanonical) continue;
        const existing = relatedArtistCounts.get(key);
        if (existing) {
          existing.sharedMembershipEvidence += 1;
        } else {
          relatedArtistCounts.set(key, {
            name: personName,
            playlistOccurrence: 0,
            trackOccurrence: 0,
            edgeEvidence: 0,
            sharedCreditEvidence: 0,
            sharedStudioEvidence: 0,
            sharedMembershipEvidence: 1,
          });
        }
      }
    }
  } catch {
    // Skip malformed shared credit/studio evidence rows quietly
  }

  try {
    const membershipRows = db.prepare(`
      SELECT band_name
      FROM artist_membership_evidence
      WHERE COALESCE(person_name_canonical, lower(trim(person_name))) = ?
    `).all(targetPersonCanonical) as Array<{ band_name: string | null }>;

    for (const row of membershipRows) {
      const bandName = typeof row.band_name === 'string' ? row.band_name.trim() : '';
      if (!bandName) continue;
      const key = bandName.toLowerCase();
      const existing = memberOfCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        memberOfCounts.set(key, { name: bandName, count: 1 });
      }
    }
  } catch {
    // Skip malformed membership rows quietly
  }

  const playlists = matched.slice(0, 30).map(({ tracks, ...rest }) => rest);
  const places = [
    ...Array.from(placeCounts.entries()).map(([name, count]) => ({ name, count })),
    ...Array.from(studioCounts.values()),
  ]
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const scenes = Array.from(sceneCounts.values())
    .sort((a, b) => {
      const aScore = a.playlistEvidence + a.edgeEvidence;
      const bScore = b.playlistEvidence + b.edgeEvidence;
      if (bScore !== aScore) return bScore - aScore;
      if (b.playlistEvidence !== a.playlistEvidence) return b.playlistEvidence - a.playlistEvidence;
      if (b.edgeEvidence !== a.edgeEvidence) return b.edgeEvidence - a.edgeEvidence;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const relatedStudios = Array.from(studioCounts.values())
    .filter((item) => item.count >= 2)
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const memberOf = Array.from(memberOfCounts.values())
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const relatedArtists = Array.from(relatedArtistCounts.values())
    .filter(
      (item) => item.sharedMembershipEvidence >= 1
        || item.sharedCreditEvidence >= 2
        || item.sharedStudioEvidence >= 3
    )
    .sort((a, b) => {
      const aShared = a.sharedCreditEvidence + a.sharedStudioEvidence;
      const bShared = b.sharedCreditEvidence + b.sharedStudioEvidence;
      const aScore = (6 * a.sharedMembershipEvidence) + (2 * a.edgeEvidence) + aShared;
      const bScore = (6 * b.sharedMembershipEvidence) + (2 * b.edgeEvidence) + bShared;
      if (bScore !== aScore) return bScore - aScore;
      if (b.sharedMembershipEvidence !== a.sharedMembershipEvidence) return b.sharedMembershipEvidence - a.sharedMembershipEvidence;
      if (b.edgeEvidence !== a.edgeEvidence) return b.edgeEvidence - a.edgeEvidence;
      if (bShared !== aShared) return bShared - aShared;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);
  const relatedCredits = Array.from(relatedCreditCounts.values())
    .sort((a, b) => {
      const strongWeight = 3;
      const aDistinctRoles = a.roles.size;
      const bDistinctRoles = b.roles.size;
      const aScore = (strongWeight * a.count) + aDistinctRoles;
      const bScore = (strongWeight * b.count) + bDistinctRoles;
      if (bScore !== aScore) return bScore - aScore;
      if (b.count !== a.count) return b.count - a.count;
      if (bDistinctRoles !== aDistinctRoles) return bDistinctRoles - aDistinctRoles;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => ({
      name: item.name,
      roles: Array.from(item.roles),
    }));
  const relatedEquipment = Array.from(relatedEquipmentCounts.values())
    .filter((item) => item.count >= 2)
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return a.name.localeCompare(b.name);
    })
    .slice(0, 8)
    .map((item) => item.name);

  return {
    playlists,
    places,
    scenes,
    memberOf,
    relatedStudios,
    relatedArtists,
    relatedCredits,
    relatedEquipment,
    relatedArtistsEvidence: strongestRelatedArtistEvidence > 0
      ? { relationType: 'co_artist', evidenceCount: strongestRelatedArtistEvidence }
      : undefined,
  };
}

export function getCreditAtlas(name: string): {
  name: string;
  roles: string[];
  primaryRoles: string[];
  playlists: Omit<PlaylistRow, 'tracks'>[];
  relatedArtists: string[];
  memberOf: string[];
  associatedStudios: string[];
} {
  const target = buildCreditCanonicalKey(name);
  if (!target) {
    return { name: name.trim(), roles: [], primaryRoles: [], playlists: [], relatedArtists: [], memberOf: [], associatedStudios: [] };
  }

  const stmt = db.prepare(`
    SELECT id, prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, created_at
    FROM playlists
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as PlaylistRow[];
  const roles = new Set<string>();
  const roleCounts = new Map<string, { name: string; count: number }>();
  const playlists: Omit<PlaylistRow, 'tracks'>[] = [];
  const relatedArtistCounts = new Map<string, { name: string; count: number }>();
  const associatedStudioCounts = new Map<string, { name: string; count: number }>();
  const memberOfCounts = new Map<string, { name: string; count: number }>();
  let displayName = name.trim();
  let hasDisplayNameFromData = false;

  for (const row of rows) {
    if (!row.credits) continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(row.credits);
    } catch {
      continue;
    }

    if (!Array.isArray(parsed)) continue;

    let matchedInRow = false;

    for (const item of parsed) {
      if (!item || typeof item !== 'object') continue;

      const creditName = typeof item.name === 'string' ? item.name.trim() : '';
      const role = typeof item.role === 'string' ? item.role.trim() : '';

      if (!creditName || !role) continue;
      if (!ALLOWED_CREDIT_ROLES.has(role)) continue;
      if (buildCreditCanonicalKey(creditName) !== target) continue;

      if (!hasDisplayNameFromData) {
        displayName = creditName;
        hasDisplayNameFromData = true;
      }
      matchedInRow = true;
    }

    if (matchedInRow && playlists.length >= 30) break;
  }

  try {
    const playlistIdRows = db.prepare(`
      SELECT DISTINCT source_playlist_id
      FROM recording_credit_evidence
      WHERE COALESCE(credit_name_canonical, lower(trim(credit_name))) = ?
    `).all(target) as Array<{ source_playlist_id: number | null }>;

    const sourcePlaylistIds = playlistIdRows
      .map((row) => (typeof row.source_playlist_id === 'number' ? row.source_playlist_id : NaN))
      .filter((value) => Number.isFinite(value));

    if (sourcePlaylistIds.length > 0) {
      const sourcePlaceholders = sourcePlaylistIds.map(() => '?').join(', ');
      const playlistRows = db.prepare(`
        SELECT id, prompt, title, description, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, equipment, created_at
        FROM playlists
        WHERE id IN (${sourcePlaceholders})
        ORDER BY created_at DESC
        LIMIT 30
      `).all(...sourcePlaylistIds) as Omit<PlaylistRow, 'tracks'>[];

      playlists.push(...playlistRows);
    }

    const creditEvidenceRows = db.prepare(`
      SELECT rce.credit_name, rce.credit_role, r.artist
      FROM recording_credit_evidence rce
      INNER JOIN recordings r ON r.id = rce.recording_id
      WHERE COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) = ?
    `).all(target) as Array<{ credit_name: string | null; credit_role: string | null; artist: string | null }>;

    for (const row of creditEvidenceRows) {
      const creditName = typeof row.credit_name === 'string' ? row.credit_name.trim() : '';
      const role = typeof row.credit_role === 'string' ? row.credit_role.trim() : '';
      const artistName = typeof row.artist === 'string' ? row.artist.trim() : '';

      if (creditName && !hasDisplayNameFromData) {
        displayName = creditName;
        hasDisplayNameFromData = true;
      }

      if (role && ALLOWED_CREDIT_ROLES.has(role)) {
        roles.add(role);
        const roleKey = role.toLowerCase();
        const existingRole = roleCounts.get(roleKey);
        if (existingRole) {
          existingRole.count += 1;
        } else {
          roleCounts.set(roleKey, { name: role, count: 1 });
        }
      }

      if (artistName) {
        const key = buildPersonCanonicalKey(artistName);
        if (key !== target) {
          const existing = relatedArtistCounts.get(key);
          if (existing) {
            existing.count += 1;
          } else {
            relatedArtistCounts.set(key, { name: artistName, count: 1 });
          }
        }
      }
    }

    const studioEvidenceRows = db.prepare(`
      SELECT rse.studio_name, COALESCE(rse.studio_name_canonical, lower(trim(rse.studio_name))) AS studio_key
      FROM recording_credit_evidence rce
      INNER JOIN recording_studio_evidence rse ON rse.recording_id = rce.recording_id
      WHERE COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) = ?
    `).all(target) as Array<{ studio_name: string | null; studio_key: string | null }>;

    for (const row of studioEvidenceRows) {
      const studioName = typeof row.studio_name === 'string' ? row.studio_name.trim() : '';
      if (!studioName) continue;
      const key = typeof row.studio_key === 'string' && row.studio_key.trim().length > 0
        ? row.studio_key.trim()
        : studioName.toLowerCase();
      const existing = associatedStudioCounts.get(key);
      if (existing) {
        existing.count += 1;
        existing.name = choosePreferredDisplayValue(existing.name, studioName);
      } else {
        associatedStudioCounts.set(key, { name: studioName, count: 1 });
      }
    }

    const membershipRows = db.prepare(`
      SELECT band_name
      FROM artist_membership_evidence
      WHERE COALESCE(person_name_canonical, lower(trim(person_name))) = ?
    `).all(target) as Array<{ band_name: string | null }>;

    for (const row of membershipRows) {
      const bandName = typeof row.band_name === 'string' ? row.band_name.trim() : '';
      if (!bandName) continue;
      const key = buildArtistCanonicalKey(bandName);
      const existing = memberOfCounts.get(key);
      if (existing) {
        existing.count += 1;
      } else {
        memberOfCounts.set(key, { name: bandName, count: 1 });
      }
    }
  } catch {
    // Skip malformed credit evidence rows safely
  }

  return {
    name: displayName,
    roles: Array.from(roles),
    primaryRoles: Array.from(roleCounts.values())
      .filter((item) => item.count >= 2 && item.name.toLowerCase() !== 'session_musician')
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 5)
      .map((item) => item.name),
    playlists,
    relatedArtists: Array.from(relatedArtistCounts.values())
      .filter((item) => item.count >= 2)
      .sort((a, b) => b.count - a.count)
      .slice(0, 8)
      .map((item) => item.name),
    memberOf: Array.from(memberOfCounts.values())
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 8)
      .map((item) => item.name),
    associatedStudios: Array.from(associatedStudioCounts.values())
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 8)
      .map((item) => item.name),
  };
}

export function getBandMembers(bandName: string): string[] {
  const target = buildArtistCanonicalKey(bandName);
  if (!target) return [];

  try {
    const truthRows = db.prepare(`
      SELECT p.canonical_name AS person_name, p.canonical_key AS person_key
      FROM truth_entities g
      INNER JOIN truth_memberships tm ON tm.group_entity_id = g.id
      INNER JOIN truth_entities p ON p.id = tm.person_entity_id
      WHERE g.entity_type = 'group'
        AND g.canonical_key = ?
      ORDER BY p.canonical_name ASC
    `).all(target) as Array<{ person_name: string | null; person_key: string | null }>;

    const rows = db.prepare(`
      SELECT person_name, COALESCE(person_name_canonical, lower(trim(person_name))) AS person_key
      FROM artist_membership_evidence
      WHERE COALESCE(band_name_canonical, lower(trim(band_name))) = ?
      ORDER BY person_name ASC
    `).all(target) as Array<{ person_name: string | null; person_key: string | null }>;

    const unique = new Map<string, string>();
    for (const row of truthRows) {
      const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
      const personKey = typeof row.person_key === 'string' ? row.person_key.trim() : '';
      if (!personName || !personKey) continue;
      if (unique.has(personKey)) continue;
      unique.set(personKey, personName);
    }

    for (const row of rows) {
      const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
      const personKey = typeof row.person_key === 'string' ? row.person_key.trim() : '';
      if (!personName || !personKey) continue;
      if (unique.has(personKey)) continue;
      unique.set(personKey, personName);
    }

    return Array.from(unique.values());
  } catch {
    return [];
  }
}

export type TruthEntityType = 'person' | 'group' | 'artist';

export interface TruthEntityRow {
  id: number;
  entity_type: TruthEntityType;
  canonical_name: string;
  canonical_key: string;
}

function buildTruthCanonicalKey(entityType: TruthEntityType, value: string): string {
  if (entityType === 'person') return buildPersonCanonicalKey(value);
  return buildArtistCanonicalKey(value);
}

export function upsertTruthEntity(entityType: TruthEntityType, name: string): TruthEntityRow | null {
  const canonicalName = canonicalizeDisplayName(name);
  const canonicalKey = buildTruthCanonicalKey(entityType, canonicalName);
  if (!canonicalName || !canonicalKey) return null;

  const upsertStmt = db.prepare(`
    INSERT INTO truth_entities (entity_type, canonical_name, canonical_key)
    VALUES (?, ?, ?)
    ON CONFLICT(entity_type, canonical_key) DO UPDATE SET
      canonical_name = excluded.canonical_name,
      updated_at = CURRENT_TIMESTAMP
  `);
  upsertStmt.run(entityType, canonicalName, canonicalKey);

  const row = db.prepare(`
    SELECT id, entity_type, canonical_name, canonical_key
    FROM truth_entities
    WHERE entity_type = ?
      AND canonical_key = ?
    LIMIT 1
  `).get(entityType, canonicalKey) as TruthEntityRow | undefined;

  return row || null;
}

export function linkTruthExternalId(entityId: number, source: string, externalId: string): void {
  if (!Number.isFinite(entityId) || entityId <= 0) return;
  const normalizedSource = source.trim().toLowerCase();
  const normalizedExternalId = externalId.trim();
  if (!normalizedSource || !normalizedExternalId) return;

  const stmt = db.prepare(`
    INSERT INTO truth_external_ids (entity_id, source, external_id)
    VALUES (?, ?, ?)
    ON CONFLICT(source, external_id) DO UPDATE SET
      entity_id = excluded.entity_id,
      updated_at = CURRENT_TIMESTAMP
  `);
  stmt.run(entityId, normalizedSource, normalizedExternalId);
}

export function upsertTruthMembership(
  personEntityId: number,
  groupEntityId: number,
  source: string,
  sourceRef: string,
  memberRole: string | null,
  validFrom: string | null,
  validTo: string | null,
  confidence = 100
): void {
  if (!Number.isFinite(personEntityId) || personEntityId <= 0) return;
  if (!Number.isFinite(groupEntityId) || groupEntityId <= 0) return;
  const normalizedSource = source.trim().toLowerCase();
  if (!normalizedSource) return;

  const stmt = db.prepare(`
    INSERT INTO truth_memberships (
      person_entity_id,
      group_entity_id,
      source,
      source_ref,
      member_role,
      confidence,
      valid_from,
      valid_to
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(person_entity_id, group_entity_id, source) DO UPDATE SET
      source_ref = excluded.source_ref,
      member_role = excluded.member_role,
      confidence = excluded.confidence,
      valid_from = excluded.valid_from,
      valid_to = excluded.valid_to,
      updated_at = CURRENT_TIMESTAMP
  `);

  stmt.run(
    personEntityId,
    groupEntityId,
    normalizedSource,
    sourceRef && sourceRef.trim().length > 0 ? sourceRef.trim() : null,
    memberRole && memberRole.trim().length > 0 ? memberRole.trim() : null,
    Math.max(0, Math.min(100, Math.floor(confidence))),
    validFrom && validFrom.trim().length > 0 ? validFrom.trim() : null,
    validTo && validTo.trim().length > 0 ? validTo.trim() : null,
  );
}

export function createTruthImportRun(source: string, subjectType: string | null, subjectValue: string | null): number {
  const stmt = db.prepare(`
    INSERT INTO truth_import_runs (source, subject_type, subject_value, status)
    VALUES (?, ?, ?, 'started')
  `);
  const result = stmt.run(
    source.trim().toLowerCase(),
    subjectType && subjectType.trim().length > 0 ? subjectType.trim() : null,
    subjectValue && subjectValue.trim().length > 0 ? subjectValue.trim() : null,
  );
  return Number(result.lastInsertRowid || 0);
}

export function upsertTruthCreditClaim(params: {
  artist: string;
  title: string;
  creditName: string;
  creditRole: string;
  source: string;
  sourceRef?: string | null;
  confidence?: number;
  creditEntityId?: number | null;
}): boolean {
  const artist = canonicalizeDisplayName(params.artist || '');
  const title = String(params.title || '').trim();
  const creditName = canonicalizeDisplayName(params.creditName || '');
  const creditRole = String(params.creditRole || '').trim().toLowerCase();
  const source = String(params.source || '').trim().toLowerCase();
  const recordingKey = buildRecordingCanonicalKey(artist, title);

  if (!artist || !title || !creditName || !creditRole || !source || !recordingKey) {
    return false;
  }

  db.prepare(`
    INSERT INTO truth_credit_claims (
      recording_canonical_key,
      artist_name,
      recording_title,
      credit_entity_id,
      credit_name,
      credit_role,
      source,
      source_ref,
      confidence
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(recording_canonical_key, credit_name, credit_role, source) DO UPDATE SET
      credit_entity_id = excluded.credit_entity_id,
      source_ref = excluded.source_ref,
      confidence = excluded.confidence,
      updated_at = CURRENT_TIMESTAMP
  `).run(
    recordingKey,
    artist,
    title,
    Number.isFinite(Number(params.creditEntityId)) ? Number(params.creditEntityId) : null,
    creditName,
    creditRole,
    source,
    params.sourceRef && String(params.sourceRef).trim().length > 0 ? String(params.sourceRef).trim() : null,
    Math.max(0, Math.min(100, Math.floor(Number(params.confidence ?? 100))))
  );

  return true;
}

export function getTracksByTruthCreditClaim(
  creditName: string,
  creditRole: string,
  limit = 50
): Array<{ artist: string; title: string; source: string; confidence: number }> {
  const normalizedName = canonicalizeDisplayName(creditName || '');
  const normalizedRole = String(creditRole || '').trim().toLowerCase();
  if (!normalizedName || !normalizedRole) return [];

  const safeLimit = Math.max(1, Math.min(500, Math.floor(limit)));

  try {
    const rows = db.prepare(`
      SELECT artist_name, recording_title, source, confidence
      FROM truth_credit_claims
      WHERE lower(trim(credit_name)) = lower(trim(?))
        AND lower(trim(credit_role)) = ?
      ORDER BY confidence DESC, lower(trim(artist_name)) ASC, lower(trim(recording_title)) ASC
      LIMIT ?
    `).all(normalizedName, normalizedRole, safeLimit) as Array<{
      artist_name: string | null;
      recording_title: string | null;
      source: string | null;
      confidence: number | null;
    }>;

    return rows
      .map((row) => ({
        artist: typeof row.artist_name === 'string' ? row.artist_name.trim() : '',
        title: typeof row.recording_title === 'string' ? row.recording_title.trim() : '',
        source: typeof row.source === 'string' ? row.source.trim().toLowerCase() : '',
        confidence: typeof row.confidence === 'number' ? Math.max(0, Math.floor(row.confidence)) : 0,
      }))
      .filter((row) => row.artist.length > 0 && row.title.length > 0 && row.source.length > 0);
  } catch {
    return [];
  }
}

export function getTruthCreditEvidenceCount(creditName: string, creditRole: string): number {
  const normalizedName = canonicalizeDisplayName(creditName || '');
  const normalizedRole = String(creditRole || '').trim().toLowerCase();
  if (!normalizedName || !normalizedRole) return 0;

  try {
    const row = db.prepare(`
      SELECT COUNT(DISTINCT recording_canonical_key) AS count
      FROM truth_credit_claims
      WHERE lower(trim(credit_name)) = lower(trim(?))
        AND lower(trim(credit_role)) = ?
    `).get(normalizedName, normalizedRole) as { count: number } | undefined;

    return typeof row?.count === 'number' ? Math.max(0, Math.floor(row.count)) : 0;
  } catch {
    return 0;
  }
}

export function completeTruthImportRun(importRunId: number, status: 'success' | 'error', stats: Record<string, unknown>): void {
  if (!Number.isFinite(importRunId) || importRunId <= 0) return;
  db.prepare(`
    UPDATE truth_import_runs
    SET status = ?, stats_json = ?, finished_at = CURRENT_TIMESTAMP
    WHERE id = ?
  `).run(status, JSON.stringify(stats || {}), importRunId);
}

export function ensureSystemSourcePlaylist(prompt: string, title: string, description: string): number {
  const existing = getPlaylistByPrompt(prompt);
  if (existing) return existing.id;

  const created = savePlaylist(
    prompt,
    title,
    description,
    '[]',
    JSON.stringify(['system_seed', 'truth'])
  );
  return created.id;
}

export function hasRecordingCreditEvidence(
  artist: string,
  title: string,
  creditName: string,
  creditRole: string
): boolean {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return false;

  const normalizedCreditName = buildCreditCanonicalKey(creditName);
  const normalizedCreditRole = creditRole.trim().toLowerCase();
  if (!normalizedCreditName || !normalizedCreditRole) return false;

  try {
    const row = db.prepare(`
      SELECT 1 AS matched
      FROM recordings r
      INNER JOIN recording_credit_evidence rce ON rce.recording_id = r.id
      WHERE r.canonical_key = ?
        AND COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) = ?
        AND lower(trim(rce.credit_role)) = ?
      LIMIT 1
    `).get(canonicalKey, normalizedCreditName, normalizedCreditRole) as { matched: number } | undefined;

    return Boolean(row && row.matched === 1);
  } catch {
    return false;
  }
}

export function getTracksByRecordingCreditEvidence(
  creditName: string,
  creditRole: string,
  limit = 24
): Array<{ artist: string; title: string; evidence_count: number }> {
  const normalizedCreditName = buildCreditCanonicalKey(creditName);
  const normalizedCreditRole = creditRole.trim().toLowerCase();
  if (!normalizedCreditName || !normalizedCreditRole) return [];

  const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.floor(limit)) : 24;

  try {
    const rows = db.prepare(`
      SELECT r.artist, r.title, COUNT(*) AS evidence_count
      FROM recordings r
      INNER JOIN recording_credit_evidence rce ON rce.recording_id = r.id
      WHERE COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) = ?
        AND lower(trim(rce.credit_role)) = ?
      GROUP BY r.id, r.artist, r.title
      ORDER BY evidence_count DESC, lower(trim(r.artist)) ASC, lower(trim(r.title)) ASC
      LIMIT ?
    `).all(normalizedCreditName, normalizedCreditRole, safeLimit) as Array<{
      artist: string | null;
      title: string | null;
      evidence_count: number | null;
    }>;

    return rows
      .map((row) => ({
        artist: typeof row.artist === 'string' ? row.artist.trim() : '',
        title: typeof row.title === 'string' ? row.title.trim() : '',
        evidence_count: typeof row.evidence_count === 'number' && Number.isFinite(row.evidence_count)
          ? Math.max(1, Math.floor(row.evidence_count))
          : 1,
      }))
      .filter((row) => row.artist.length > 0 && row.title.length > 0);
  } catch {
    return [];
  }
}

export function getRecordingCreditEvidenceCount(
  creditName: string,
  creditRole: string
): number {
  const normalizedCreditName = buildCreditCanonicalKey(creditName);
  const normalizedCreditRole = creditRole.trim().toLowerCase();
  if (!normalizedCreditName || !normalizedCreditRole) return 0;

  try {
    const row = db.prepare(`
      SELECT COUNT(DISTINCT r.id) AS count
      FROM recordings r
      INNER JOIN recording_credit_evidence rce ON rce.recording_id = r.id
      WHERE COALESCE(rce.credit_name_canonical, lower(trim(rce.credit_name))) = ?
        AND lower(trim(rce.credit_role)) = ?
    `).get(normalizedCreditName, normalizedCreditRole) as { count: number } | undefined;

    const count = typeof row?.count === 'number' ? row.count : 0;
    return Math.max(0, Math.floor(count));
  } catch {
    return 0;
  }
}

export function getRecordingIsrc(artist: string, title: string): string | null {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return null;

  try {
    const row = db.prepare('SELECT isrc FROM recordings WHERE canonical_key = ? LIMIT 1').get(canonicalKey) as { isrc: string | null } | undefined;
    const value = typeof row?.isrc === 'string' ? normalizeIsrc(row.isrc) : '';
    return value.length >= 12 ? value : null;
  } catch {
    return null;
  }
}

export function setRecordingIsrc(artist: string, title: string, isrc: string): void {
  const normalizedIsrc = normalizeIsrc(isrc || '');
  if (!normalizedIsrc || normalizedIsrc.length < 12) return;
  const recordingId = ensureRecordingId(artist, title);
  if (!recordingId) return;

  db.prepare('UPDATE recordings SET isrc = ? WHERE id = ?').run(normalizedIsrc, recordingId);
}

function normalizeSpotifyUri(value: string): string {
  const match = String(value || '').trim().match(/^spotify:track:([A-Za-z0-9]+)$/);
  if (!match) return '';
  return `spotify:track:${match[1]}`;
}

function normalizeSpotifyUrl(value: string): string {
  return String(value || '').trim();
}

function spotifyTrackIdFromUrl(value: string): string {
  const match = String(value || '').trim().match(/spotify\.com\/track\/([A-Za-z0-9]+)/i);
  return match?.[1] || '';
}

function spotifyUriFromUrl(value: string): string {
  const trackId = spotifyTrackIdFromUrl(value);
  if (!trackId) return '';
  return `spotify:track:${trackId}`;
}

export function getRecordingSpotifyUri(artist: string, title: string): string | null {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return null;
  try {
    const row = db.prepare('SELECT spotify_uri, spotify_url FROM recordings WHERE canonical_key = ? LIMIT 1').get(canonicalKey) as {
      spotify_uri: string | null;
      spotify_url: string | null;
    } | undefined;
    const value = typeof row?.spotify_uri === 'string' ? normalizeSpotifyUri(row.spotify_uri) : '';
    if (value) return value;
    const fromUrl = typeof row?.spotify_url === 'string' ? spotifyUriFromUrl(row.spotify_url) : '';
    if (fromUrl) return fromUrl;
    return value || null;
  } catch {
    return null;
  }
}

export function setRecordingSpotifyUri(artist: string, title: string, spotifyUri: string): void {
  const value = normalizeSpotifyUri(spotifyUri || '');
  if (!value) return;
  const recordingId = ensureRecordingId(artist, title);
  if (!recordingId) return;
  db.prepare('UPDATE recordings SET spotify_uri = ? WHERE id = ?').run(value, recordingId);
}

export function getRecordingSpotifyUrl(artist: string, title: string): string | null {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return null;
  try {
    const row = db.prepare('SELECT spotify_url FROM recordings WHERE canonical_key = ? LIMIT 1').get(canonicalKey) as { spotify_url: string | null } | undefined;
    const value = typeof row?.spotify_url === 'string' ? row.spotify_url.trim() : '';
    return value || null;
  } catch {
    return null;
  }
}

export function setRecordingSpotifyUrl(artist: string, title: string, spotifyUrl: string): void {
  const value = normalizeSpotifyUrl(spotifyUrl || '');
  if (!value) return;
  const recordingId = ensureRecordingId(artist, title);
  if (!recordingId) return;
  const derivedUri = spotifyUriFromUrl(value);
  if (derivedUri) {
    db.prepare('UPDATE recordings SET spotify_url = ?, spotify_uri = ? WHERE id = ?').run(value, derivedUri, recordingId);
  } else {
    db.prepare('UPDATE recordings SET spotify_url = ? WHERE id = ?').run(value, recordingId);
  }
}

export function getRecordingDurationMs(artist: string, title: string): number | null {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return null;
  try {
    const row = db.prepare('SELECT duration_ms FROM recordings WHERE canonical_key = ? LIMIT 1').get(canonicalKey) as { duration_ms: number | null } | undefined;
    const value = typeof row?.duration_ms === 'number' && Number.isFinite(row.duration_ms)
      ? Math.max(0, Math.floor(row.duration_ms))
      : 0;
    return value > 0 ? value : null;
  } catch {
    return null;
  }
}

export function setRecordingDurationMs(artist: string, title: string, durationMs: number): void {
  const parsed = Number(durationMs);
  if (!Number.isFinite(parsed)) return;
  const value = Math.max(0, Math.floor(parsed));
  if (value <= 0) return;
  const recordingId = ensureRecordingId(artist, title);
  if (!recordingId) return;
  db.prepare('UPDATE recordings SET duration_ms = ? WHERE id = ?').run(value, recordingId);
}

export function hasRecordingStudioEvidence(
  artist: string,
  title: string,
  studioName: string,
  trustedOnly = false
): boolean {
  const canonicalKey = buildRecordingCanonicalKey(artist, title);
  if (!canonicalKey) return false;

  const normalizedStudio = buildStudioCanonicalKey(studioName);
  if (!normalizedStudio) return false;

  try {
    const trustedJoinClause = trustedOnly
      ? 'INNER JOIN playlists p ON p.id = rse.source_playlist_id'
      : '';
    const trustedWhereClause = trustedOnly
      ? 'AND (p.prompt LIKE ? OR p.prompt LIKE ?)'
      : '';
    const params: Array<string> = [canonicalKey, normalizedStudio];
    if (trustedOnly) {
      params.push('[system] studio evidence backfill from discogs ::%');
      params.push('[system] studio evidence backfill from musicbrainz ::%');
    }

    const row = db.prepare(`
      SELECT 1 AS matched
      FROM recordings r
      INNER JOIN recording_studio_evidence rse ON rse.recording_id = r.id
      ${trustedJoinClause}
      WHERE r.canonical_key = ?
        AND COALESCE(rse.studio_name_canonical, lower(trim(rse.studio_name))) = ?
        ${trustedWhereClause}
      LIMIT 1
    `).get(...params) as { matched: number } | undefined;

    return Boolean(row && row.matched === 1);
  } catch {
    return false;
  }
}

export function getTracksByRecordingStudioEvidence(
  studioName: string,
  limit = 120,
  trustedOnly = false
): Array<{ artist: string; title: string }> {
  const normalizedStudio = buildStudioCanonicalKey(studioName);
  if (!normalizedStudio) return [];

  const safeLimit = Math.max(1, Math.min(500, Math.floor(limit)));
  try {
    const trustedJoinClause = trustedOnly
      ? 'INNER JOIN playlists p ON p.id = rse.source_playlist_id'
      : '';
    const trustedWhereClause = trustedOnly
      ? 'AND (p.prompt LIKE ? OR p.prompt LIKE ?)'
      : '';
    const params: Array<string | number> = [normalizedStudio];
    if (trustedOnly) {
      params.push('[system] studio evidence backfill from discogs ::%');
      params.push('[system] studio evidence backfill from musicbrainz ::%');
    }
    params.push(safeLimit);

    const rows = db.prepare(`
      SELECT r.artist AS artist, r.title AS title, COUNT(*) AS evidence_count, MAX(rse.created_at) AS last_seen
      FROM recording_studio_evidence rse
      INNER JOIN recordings r ON r.id = rse.recording_id
      ${trustedJoinClause}
      WHERE COALESCE(rse.studio_name_canonical, lower(trim(rse.studio_name))) = ?
      ${trustedWhereClause}
      GROUP BY r.id, r.artist, r.title
      ORDER BY evidence_count DESC, last_seen DESC
      LIMIT ?
    `).all(...params) as Array<{ artist?: string; title?: string }>;

    return rows
      .map((row) => ({
        artist: typeof row.artist === 'string' ? row.artist.trim() : '',
        title: typeof row.title === 'string' ? row.title.trim() : '',
      }))
      .filter((row) => row.artist.length > 0 && row.title.length > 0);
  } catch {
    return [];
  }
}

export type AtlasNodeType = 'artist' | 'tag' | 'scene' | 'country' | 'city' | 'studio' | 'venue' | 'equipment' | 'playlist';

export interface AtlasNodeSuggestion {
  type: AtlasNodeType;
  value: string;
}

export interface AtlasPathNode {
  type: AtlasNodeType;
  value: string;
}

export interface AtlasPathResult {
  nodes: AtlasPathNode[];
  relations: string[];
  paths?: Array<{
    nodes: AtlasPathNode[];
    relations: string[];
  }>;
}

const VALID_NODE_TYPES: AtlasNodeType[] = ['artist', 'tag', 'scene', 'country', 'city', 'studio', 'venue', 'equipment', 'playlist'];

export function searchAtlasNodeSuggestions(query: string, limit = 20): AtlasNodeSuggestion[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return [];

  const stmt = db.prepare(`
    SELECT id, tracks, tags, scene, scenes, countries, cities, studios, venues, equipment
    FROM playlists
    ORDER BY created_at DESC
  `);

  const rows = stmt.all() as Array<
    Pick<PlaylistRow, 'id' | 'tracks' | 'tags' | 'scene' | 'scenes' | 'countries' | 'cities' | 'studios' | 'venues' | 'equipment'>
  >;

  const results: AtlasNodeSuggestion[] = [];
  const seen = new Set<string>();

  const pushIfMatch = (type: AtlasNodeType, value: string): void => {
    const trimmed = value.trim();
    if (!trimmed) return;
    if (!trimmed.toLowerCase().includes(needle)) return;

    const key = `${type}:${trimmed.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    results.push({ type, value: trimmed });
  };

  for (const row of rows) {
    for (const artist of getTrackArtists({ tracks: row.tracks })) {
      pushIfMatch('artist', artist);
    }

    for (const tag of parseStringArray(row.tags)) {
      pushIfMatch('tag', tag);
    }

    for (const scene of getNormalizedScenes({ scenes: row.scenes, scene: row.scene })) {
      pushIfMatch('scene', scene);
    }

    for (const country of parseStringArray(row.countries)) {
      pushIfMatch('country', country);
    }

    for (const city of parseStringArray(row.cities)) {
      pushIfMatch('city', city);
    }

    for (const studio of parseStringArray(row.studios)) {
      pushIfMatch('studio', studio);
    }

    for (const venue of parseStringArray(row.venues)) {
      pushIfMatch('venue', venue);
    }

    for (const equipment of getNormalizedEquipmentNames({ equipment: row.equipment })) {
      pushIfMatch('equipment', equipment);
    }

    pushIfMatch('playlist', String(row.id));

    if (results.length >= limit) {
      return results.slice(0, limit);
    }
  }

  try {
    const membershipRows = db.prepare(`
      SELECT band_name, person_name
      FROM artist_membership_evidence
      ORDER BY created_at DESC
    `).all() as Array<{ band_name: string | null; person_name: string | null }>;

    for (const row of membershipRows) {
      const bandName = typeof row.band_name === 'string' ? row.band_name.trim() : '';
      const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
      if (bandName) pushIfMatch('artist', bandName);
      if (personName) pushIfMatch('artist', personName);

      if (results.length >= limit) {
        return results.slice(0, limit);
      }
    }
  } catch {
    // Membership evidence is optional.
  }

  return results.slice(0, limit);
}

function toNodeKey(type: AtlasNodeType, value: string): string {
  return `${type}:${value}`;
}

function parseNodeKey(key: string): AtlasPathNode {
  const idx = key.indexOf(':');
  const type = key.slice(0, idx) as AtlasNodeType;
  const value = key.slice(idx + 1);
  return { type, value };
}

export function isValidAtlasNodeType(value: string): value is AtlasNodeType {
  return VALID_NODE_TYPES.includes(value as AtlasNodeType);
}

export function getGraphNeighbors(
  nodeType: string,
  nodeName: string,
  limit = 10
): Array<{
  nodeType: AtlasNodeType;
  nodeName: string;
  relationType: string;
  score: number;
}> {
  const fromType = nodeType.trim();
  const fromName = nodeName.trim().toLowerCase();
  const maxResults = Math.max(1, Math.min(50, Math.floor(limit)));

  if (!fromType || !fromName || !isValidAtlasNodeType(fromType)) {
    return [];
  }

  try {
    const edgeStmt = db.prepare(`
      SELECT to_type, to_name, relation_type, evidence_count, strength
      FROM atlas_edges
      WHERE from_type = ?
        AND lower(trim(from_name)) = ?
    `);
    const edges = edgeStmt.all(fromType, fromName) as Array<{
      to_type: unknown;
      to_name: unknown;
      relation_type: unknown;
      evidence_count: unknown;
      strength: unknown;
    }>;

    const statsStmt = db.prepare(`
      SELECT edge_count, weighted_strength
      FROM atlas_node_stats
      WHERE node_type = ?
        AND lower(trim(node_name)) = ?
      LIMIT 1
    `);

    const CENTRALITY_FACTOR = 10;
    const neighbors: Array<{ nodeType: AtlasNodeType; nodeName: string; relationType: string; score: number }> = [];

    for (const edge of edges) {
      if (!edge || typeof edge !== 'object') continue;

      const toType = typeof edge.to_type === 'string' ? edge.to_type.trim() : '';
      const toName = typeof edge.to_name === 'string' ? edge.to_name.trim() : '';
      const relationType = typeof edge.relation_type === 'string' ? edge.relation_type.trim() : '';
      if (!toType || !toName || !relationType || !isValidAtlasNodeType(toType)) continue;

      const evidenceRaw = typeof edge.evidence_count === 'number' ? edge.evidence_count : 0;
      const strengthRaw = typeof edge.strength === 'number' ? edge.strength : 0;
      const evidenceCount = Math.max(0, Math.min(1000, Math.floor(evidenceRaw)));
      const strength = Math.max(0, Math.min(10, Math.floor(strengthRaw)));

      let nodeCentrality = 0;
      const statsRow = statsStmt.get(toType, toName.toLowerCase()) as { edge_count?: unknown; weighted_strength?: unknown } | undefined;
      if (statsRow) {
        const edgeCount = typeof statsRow.edge_count === 'number' ? statsRow.edge_count : 0;
        const weightedStrength = typeof statsRow.weighted_strength === 'number' ? statsRow.weighted_strength : 0;
        nodeCentrality = Math.max(0, Math.floor(edgeCount)) + Math.max(0, Math.floor(weightedStrength));
      }

      const score = evidenceCount + strength + (nodeCentrality / CENTRALITY_FACTOR);
      neighbors.push({ nodeType: toType, nodeName: toName, relationType, score });
    }

    return neighbors
      .sort((a, b) => b.score - a.score)
      .slice(0, maxResults);
  } catch {
    return [];
  }
}

function getAtlasEdgeEvidence(
  fromType: string,
  fromName: string,
  toType: string
): { relationType: string; evidenceCount: number } | null {
  const normalizedFromType = fromType.trim();
  const normalizedToType = toType.trim();
  const normalizedFromName = fromName.trim().toLowerCase();

  if (!normalizedFromType || !normalizedToType || !normalizedFromName) {
    return null;
  }

  try {
    const stmt = db.prepare(`
      SELECT relation_type, evidence_count
      FROM atlas_edges
      WHERE from_type = ?
        AND to_type = ?
        AND lower(trim(from_name)) = ?
    `);

    const rows = stmt.all(normalizedFromType, normalizedToType, normalizedFromName) as Array<{
      relation_type: unknown;
      evidence_count: unknown;
    }>;

    let best: { relationType: string; evidenceCount: number } | null = null;

    for (const row of rows) {
      const relationType = typeof row.relation_type === 'string' ? row.relation_type.trim() : '';
      if (!relationType) continue;

      const evidenceRaw = typeof row.evidence_count === 'number' ? row.evidence_count : 1;
      const evidenceCount = Math.max(1, Math.min(20, Math.floor(evidenceRaw)));

      if (!best) {
        best = { relationType, evidenceCount };
        continue;
      }

      if (evidenceCount > best.evidenceCount) {
        best = { relationType, evidenceCount };
        continue;
      }

      if (evidenceCount === best.evidenceCount && relationType.localeCompare(best.relationType) < 0) {
        best = { relationType, evidenceCount };
      }
    }

    return best;
  } catch {
    return null;
  }
}

export function getConnectionPath(
  fromType: AtlasNodeType,
  fromValue: string,
  toType: AtlasNodeType,
  toValue: string,
  maxDepth = 3
): AtlasPathResult {
  const stmt = db.prepare(`
    SELECT id, tracks, tags, scene, scenes, countries, cities, studios, venues, equipment, place, places
    FROM playlists
  `);
  const rows = stmt.all() as Array<
    Pick<PlaylistRow, 'id' | 'tracks' | 'tags' | 'scene' | 'scenes' | 'countries' | 'cities' | 'studios' | 'venues' | 'equipment' | 'place' | 'places'>
  >;

  const nodeStatsMap = new Map<string, number>();
  try {
    const statsStmt = db.prepare(`
      SELECT node_type, node_name, edge_count, weighted_strength
      FROM atlas_node_stats
    `);
    const statsRows = statsStmt.all() as Array<{
      node_type: unknown;
      node_name: unknown;
      edge_count: unknown;
      weighted_strength: unknown;
    }>;

    for (const row of statsRows) {
      if (!row || typeof row !== 'object') continue;
      if (typeof row.node_type !== 'string' || typeof row.node_name !== 'string') continue;

      const type = row.node_type.trim();
      const name = row.node_name.trim();
      if (!type || !name) continue;

      const edgeCount = typeof row.edge_count === 'number' ? row.edge_count : 0;
      const weightedStrength = typeof row.weighted_strength === 'number' ? row.weighted_strength : 0;
      const score = Math.max(0, Math.floor(edgeCount)) + Math.max(0, Math.floor(weightedStrength));
      const key = `${type}:${name.toLowerCase()}`;
      nodeStatsMap.set(key, score);
    }
  } catch {
    // Node stats are optional ranking signals.
  }

  const getNodeCentrality = (nodeKey: string): number => nodeStatsMap.get(nodeKey) || 0;

  const adjacency = new Map<string, Map<string, string>>();
  const canonical = new Map<string, string>();
  const edgeFrequencies = new Map<string, number>();
  type RelationType = 'influence' | 'credit' | 'artist_cooccurrence' | 'artist_scene' | 'artist_studio' | 'artist_venue' | 'artist_city' | 'artist_equipment' | 'country' | 'tag' | 'playlist_hub' | 'generic';
  interface EdgeMetadata {
    relationType: RelationType;
    strength: 1 | 2 | 3;
    evidenceCount: number;
  }
  const edgeMetadata = new Map<string, EdgeMetadata>();

  const getEdgeFrequencyKey = (aKey: string, bKey: string): string => {
    return aKey < bKey ? `${aKey}|${bKey}` : `${bKey}|${aKey}`;
  };

  const getRelationPriority = (relation: string, nextKey: string): number => {
    const value = relation.toLowerCase();
    if (value.includes('influence')) return 70;
    if (value.includes('credit')) return 60;
    if (value.includes('artist')) return 50;
    if (value.includes('scene')) return 40;
    if (value.includes('studio') || value.includes('venue') || value.includes('equipment')) return 30;
    if (value.includes('city') || value.includes('country')) return 20;
    if (nextKey.startsWith('playlist:')) return 10;
    return 0;
  };

  const getRelationType = (relation: string, aType: AtlasNodeType, bType: AtlasNodeType): RelationType => {
    const value = relation.toLowerCase();
    if (value.includes('influence')) return 'influence';
    if (value.includes('credit')) return 'credit';
    if ((aType === 'artist' && bType === 'artist') || value.includes('artist co-occurrence')) return 'artist_cooccurrence';
    if (value.includes('scene')) return 'artist_scene';
    if (value.includes('studio')) return 'artist_studio';
    if (value.includes('venue')) return 'artist_venue';
    if (value.includes('equipment')) return 'artist_equipment';
    if (value.includes('city')) return 'artist_city';
    if (value.includes('country')) return 'country';
    if (value.includes('tag')) return 'tag';
    if (aType === 'playlist' || bType === 'playlist') return 'playlist_hub';
    return 'generic';
  };

  const getStrengthForRelationType = (relationType: RelationType): 1 | 2 | 3 => {
    if (relationType === 'influence' || relationType === 'credit' || relationType === 'artist_cooccurrence') return 3;
    if (
      relationType === 'artist_scene'
      || relationType === 'artist_studio'
      || relationType === 'artist_venue'
      || relationType === 'artist_city'
      || relationType === 'artist_equipment'
    ) {
      return 2;
    }
    return 1;
  };

  const register = (type: AtlasNodeType, value: string): string => {
    const trimmed = value.trim();
    if (!trimmed) return '';

    const id = `${type}:${trimmed.toLowerCase()}`;
    if (!canonical.has(id)) {
      canonical.set(id, trimmed);
    }

    return canonical.get(id) || trimmed;
  };

  const addEdge = (
    aType: AtlasNodeType,
    aValue: string,
    bType: AtlasNodeType,
    bValue: string,
    relation: string,
    evidenceIncrement = 1
  ): void => {
    const a = register(aType, aValue);
    const b = register(bType, bValue);
    if (!a || !b) return;

    const aKey = toNodeKey(aType, a);
    const bKey = toNodeKey(bType, b);

    if (!adjacency.has(aKey)) adjacency.set(aKey, new Map());
    if (!adjacency.has(bKey)) adjacency.set(bKey, new Map());

    adjacency.get(aKey)?.set(bKey, relation);
    adjacency.get(bKey)?.set(aKey, relation);

    const edgeKey = getEdgeFrequencyKey(aKey, bKey);
    edgeFrequencies.set(edgeKey, (edgeFrequencies.get(edgeKey) || 0) + evidenceIncrement);

    const relationType = getRelationType(relation, aType, bType);
    const strength = getStrengthForRelationType(relationType);
    const existingMetadata = edgeMetadata.get(edgeKey);
    if (!existingMetadata) {
      edgeMetadata.set(edgeKey, {
        relationType,
        strength,
        evidenceCount: evidenceIncrement,
      });
    } else {
      existingMetadata.evidenceCount += evidenceIncrement;
      if (strength > existingMetadata.strength) {
        existingMetadata.strength = strength;
        existingMetadata.relationType = relationType;
      }
    }
  };

  for (const row of rows) {
    const playlistValue = String(row.id);

    for (const artist of getTrackArtists({ tracks: row.tracks })) {
      addEdge('playlist', playlistValue, 'artist', artist, 'has artist');
    }

    for (const tag of parseStringArray(row.tags)) {
      addEdge('playlist', playlistValue, 'tag', tag, 'has tag');
    }

    for (const scene of getNormalizedScenes({ scenes: row.scenes, scene: row.scene })) {
      addEdge('playlist', playlistValue, 'scene', scene, 'in scene');
    }

    for (const country of getNormalizedCountries({ countries: row.countries, place: row.place, places: row.places })) {
      addEdge('playlist', playlistValue, 'country', country, 'from country');
    }

    for (const city of getNormalizedCities({ cities: row.cities, place: row.place, places: row.places })) {
      addEdge('playlist', playlistValue, 'city', city, 'from city');
    }

    for (const studio of getNormalizedStudios({ studios: row.studios, place: row.place, places: row.places, cities: row.cities })) {
      addEdge('playlist', playlistValue, 'studio', studio, 'from studio');
    }

    for (const venue of getNormalizedVenues({ venues: row.venues, place: row.place, places: row.places })) {
      addEdge('playlist', playlistValue, 'venue', venue, 'from venue');
    }

    for (const equipment of getNormalizedEquipmentNames({ equipment: row.equipment })) {
      addEdge('playlist', playlistValue, 'equipment', equipment, 'uses equipment');
    }
  }

  try {
    const membershipRows = db.prepare(`
      SELECT band_name, person_name
      FROM artist_membership_evidence
    `).all() as Array<{ band_name: string | null; person_name: string | null }>;

    for (const row of membershipRows) {
      const bandName = typeof row.band_name === 'string' ? row.band_name.trim() : '';
      const personName = typeof row.person_name === 'string' ? row.person_name.trim() : '';
      if (!bandName || !personName) continue;
      addEdge('artist', personName, 'artist', bandName, 'member of', 3);
    }
  } catch {
    // Membership evidence is optional.
  }

  const atlasEdgeStmt = db.prepare(`
    SELECT from_type, from_name, to_type, to_name, relation_type, evidence_count
    FROM atlas_edges
  `);
  const atlasEdgeRows = atlasEdgeStmt.all() as Array<{
    from_type: string;
    from_name: string;
    to_type: string;
    to_name: string;
    relation_type: string;
    evidence_count: number | null;
  }>;

  const mapAtlasRelationToGraphRelation = (
    relationType: string,
    fromType: AtlasNodeType,
    toType: AtlasNodeType
  ): string => {
    const rel = relationType.toLowerCase();
    if (rel === 'co_artist') return 'has artist';
    if (rel === 'used_by_artist') return fromType === 'equipment' ? 'has artist' : 'uses equipment';
    if (rel === 'associated_with_scene') return 'in scene';
    if (rel === 'located_in') {
      if (toType === 'city' || fromType === 'city') return 'from city';
      if (toType === 'country' || fromType === 'country') return 'from country';
    }
    return relationType;
  };

  for (const row of atlasEdgeRows) {
    if (!row || typeof row !== 'object') continue;

    const fromType = typeof row.from_type === 'string' ? row.from_type.trim() : '';
    const toType = typeof row.to_type === 'string' ? row.to_type.trim() : '';
    const fromName = typeof row.from_name === 'string' ? row.from_name.trim() : '';
    const toName = typeof row.to_name === 'string' ? row.to_name.trim() : '';
    const relationType = typeof row.relation_type === 'string' ? row.relation_type.trim() : '';

    if (!fromType || !toType || !fromName || !toName || !relationType) continue;
    if (!isValidAtlasNodeType(fromType) || !isValidAtlasNodeType(toType)) continue;

    const evidenceRaw = typeof row.evidence_count === 'number' ? row.evidence_count : 1;
    const evidenceCount = Math.max(1, Math.min(20, Math.floor(evidenceRaw)));
    const mappedRelation = mapAtlasRelationToGraphRelation(relationType, fromType, toType);

    addEdge(fromType, fromName, toType, toName, mappedRelation, evidenceCount);
  }

  const fromCanonical = canonical.get(`${fromType}:${fromValue.trim().toLowerCase()}`);
  const toCanonical = canonical.get(`${toType}:${toValue.trim().toLowerCase()}`);

  if (!fromCanonical || !toCanonical) {
    return { nodes: [], relations: [] };
  }

  const start = toNodeKey(fromType, fromCanonical);
  const goal = toNodeKey(toType, toCanonical);

  if (!adjacency.has(start) || !adjacency.has(goal)) {
    return { nodes: [], relations: [] };
  }

  const queue: Array<{ key: string; path: string[]; relations: string[] }> = [{ key: start, path: [start], relations: [] }];
  const visited = new Set<string>([start]);
  const foundPaths: Array<{ nodes: AtlasPathNode[]; relations: string[] }> = [];
  const foundPathKeys = new Set<string>();

  while (queue.length > 0 && foundPaths.length < 3) {
    const current = queue.shift();
    if (!current) break;

    if (current.key === goal) {
      const pathKey = current.path.join('>');
      if (!foundPathKeys.has(pathKey)) {
        foundPathKeys.add(pathKey);
        foundPaths.push({
          nodes: current.path.map(parseNodeKey),
          relations: current.relations,
        });
      }
      continue;
    }

    const depth = current.path.length - 1;
    if (depth >= maxDepth) continue;

    const neighbors = adjacency.get(current.key);
    if (!neighbors) continue;

    const sortedNeighbors = Array.from(neighbors.entries())
      .map((entry, index) => ({ entry, index }))
      .sort((a, b) => {
        const [aKey, aRelation] = a.entry;
        const [bKey, bRelation] = b.entry;

        const aPriority = getRelationPriority(aRelation, aKey);
        const bPriority = getRelationPriority(bRelation, bKey);
        if (bPriority !== aPriority) {
          return bPriority - aPriority;
        }

        const aMeta = edgeMetadata.get(getEdgeFrequencyKey(current.key, aKey));
        const bMeta = edgeMetadata.get(getEdgeFrequencyKey(current.key, bKey));
        const aStrength = aMeta?.strength ?? 1;
        const bStrength = bMeta?.strength ?? 1;
        if (bStrength !== aStrength) {
          return bStrength - aStrength;
        }

        const aEvidence = aMeta?.evidenceCount ?? (edgeFrequencies.get(getEdgeFrequencyKey(current.key, aKey)) || 0);
        const bEvidence = bMeta?.evidenceCount ?? (edgeFrequencies.get(getEdgeFrequencyKey(current.key, bKey)) || 0);
        if (bEvidence !== aEvidence) {
          return bEvidence - aEvidence;
        }

        const aCentrality = getNodeCentrality(aKey);
        const bCentrality = getNodeCentrality(bKey);
        if (bCentrality !== aCentrality) {
          return bCentrality - aCentrality;
        }

        return a.index - b.index;
      })
      .map((item) => item.entry);

    for (const [nextKey, relation] of sortedNeighbors) {
      const isGoal = nextKey === goal;
      if (!isGoal && visited.has(nextKey)) continue;
      if (!isGoal) {
        visited.add(nextKey);
      }
      queue.push({
        key: nextKey,
        path: [...current.path, nextKey],
        relations: [...current.relations, relation],
      });
    }
  }

  if (foundPaths.length > 0) {
    return {
      nodes: foundPaths[0].nodes,
      relations: foundPaths[0].relations,
      paths: foundPaths,
    };
  }

  return { nodes: [], relations: [] };
}

export function savePlaylist(
  prompt: string,
  title: string,
  description: string,
  tracks: string,
  tags: string | null = null,
  place: string | null = null,
  scene: string | null = null,
  places: string | null = null,
  scenes: string | null = null,
  countries: string | null = null,
  cities: string | null = null,
  studios: string | null = null,
  venues: string | null = null,
  influences: string | null = null,
  credits: string | null = null,
  equipment: string | null = null
): PlaylistRow {
  const stmt = db.prepare(`
    INSERT INTO playlists (prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, equipment)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(prompt) DO UPDATE SET
      title = excluded.title,
      description = excluded.description,
      tracks = excluded.tracks,
      tags = excluded.tags,
      place = excluded.place,
      scene = excluded.scene,
      places = excluded.places,
      scenes = excluded.scenes,
      countries = excluded.countries,
      cities = excluded.cities,
      studios = excluded.studios,
      venues = excluded.venues,
      influences = excluded.influences,
      credits = excluded.credits,
      equipment = excluded.equipment,
      created_at = CURRENT_TIMESTAMP
  `);
  stmt.run(prompt, title, description, tracks, tags, place, scene, places, scenes, countries, cities, studios, venues, influences, credits, equipment);

  const savedRow = db.prepare('SELECT * FROM playlists WHERE prompt = ?').get(prompt) as PlaylistRow | undefined;
  if (!savedRow) {
    throw new Error('Failed to persist playlist row');
  }
  const sourcePlaylistId = savedRow.id;

  db.prepare('DELETE FROM recording_equipment_evidence WHERE source_playlist_id = ?').run(sourcePlaylistId);
  db.prepare('DELETE FROM recording_studio_evidence WHERE source_playlist_id = ?').run(sourcePlaylistId);
  db.prepare('DELETE FROM recording_credit_evidence WHERE source_playlist_id = ?').run(sourcePlaylistId);
  db.prepare('DELETE FROM artist_membership_evidence WHERE source_playlist_id = ?').run(sourcePlaylistId);

  try {
    let parsedTracks: unknown;
    try {
      parsedTracks = JSON.parse(tracks);
    } catch {
      parsedTracks = [];
    }

    let parsedEquipment: unknown = [];
    if (equipment) {
      try {
        parsedEquipment = JSON.parse(equipment);
      } catch {
        parsedEquipment = [];
      }
    }

    let parsedCredits: unknown = [];
    if (credits) {
      try {
        parsedCredits = JSON.parse(credits);
      } catch {
        parsedCredits = [];
      }
    }

    const equipmentItems = Array.isArray(parsedEquipment)
      ? parsedEquipment
          .filter(
            (item): item is { name: string; category: string } =>
              !!item
              && typeof item === 'object'
              && typeof item.name === 'string'
              && item.name.trim().length > 0
              && typeof item.category === 'string'
              && item.category.trim().length > 0
          )
          .map((item) => ({ name: canonicalizeEquipmentName(item.name), category: item.category.trim() }))
          .filter((item) => item.name.length > 0 && !isGenericEquipmentName(item.name))
      : [];
    const creditItems = Array.isArray(parsedCredits)
      ? parsedCredits
          .filter(
            (item): item is { name: string; role: string } =>
              !!item
              && typeof item === 'object'
              && typeof item.name === 'string'
              && item.name.trim().length > 0
              && typeof item.role === 'string'
              && item.role.trim().length > 0
          )
          .map((item) => ({
            name: canonicalizeDisplayName(item.name),
            nameCanonical: buildCreditCanonicalKey(item.name),
            role: item.role.trim(),
          }))
          .filter((item) => item.name.length > 0 && item.nameCanonical.length > 0)
          .filter((item) => ALLOWED_CREDIT_ROLES.has(item.role))
      : [];
    const normalizedStudiosForEvidence = Array.from(
      new Set(
        getNormalizedStudios({ studios, place, places, cities })
          .filter((studioName) => studioName.trim().length > 0)
          .filter((studioName) => isValidStudioEvidenceName(studioName))
      )
    );
    const normalizedPrompt = normalizePromptForCache(prompt || '');
    const normalizedPromptForStudioMatch = normalizeStudioMatchText(prompt || '');
    const hasStudioCue = /\bstudio\b|\bstudios\b|\brecorded at\b|\brecorded in\b|\btracked at\b|\btracked in\b|\bcut at\b|\bcut in\b|\bmade at\b|\bmade in\b|\bdone at\b|\bdone in\b/.test(normalizedPrompt);
    const promptMentionedStudiosForEvidence = normalizedStudiosForEvidence.filter((studioName) => {
      const normalizedStudioName = normalizeStudioMatchText(studioName);
      if (!normalizedStudioName) return false;
      return normalizedPromptForStudioMatch.includes(normalizedStudioName);
    });
    const persistedStudiosForEvidence =
      promptMentionedStudiosForEvidence.length > 0 && hasStudioCue
        ? promptMentionedStudiosForEvidence
        : [];

    const insertedAtlasEdgeKeys = new Set<string>();
    const insertAtlasEdgeEvidenceOnce = (
      fromType: string,
      fromName: string,
      toType: string,
      toName: string,
      relationType: string,
      strength: number
    ): void => {
      const a = `${fromType}:${fromName}`;
      const b = `${toType}:${toName}`;
      const edgeKey = a <= b ? `${relationType}::${a}::${b}` : `${relationType}::${b}::${a}`;
      if (insertedAtlasEdgeKeys.has(edgeKey)) return;
      insertedAtlasEdgeKeys.add(edgeKey);
      insertAtlasEdgeEvidence(fromType, fromName, toType, toName, relationType, strength);
    };

    if (Array.isArray(parsedTracks) && parsedTracks.length > 0) {
        const selectRecordingByKeyStmt = db.prepare('SELECT id FROM recordings WHERE canonical_key = ?');
        const insertRecordingStmt = db.prepare('INSERT INTO recordings (artist, title, canonical_key) VALUES (?, ?, ?)');
        const updateRecordingSpotifyUrlStmt = db.prepare('UPDATE recordings SET spotify_url = ? WHERE id = ?');
        const updateRecordingIsrcStmt = db.prepare('UPDATE recordings SET isrc = ? WHERE id = ?');
        const insertEvidenceStmt = db.prepare(
          'INSERT INTO recording_equipment_evidence (recording_id, equipment_name, equipment_category, source_playlist_id) VALUES (?, ?, ?, ?)'
        );
        const equipmentEvidenceExistsStmt = db.prepare(`
          SELECT 1
          FROM recording_equipment_evidence
          WHERE recording_id = ?
            AND source_playlist_id = ?
            AND lower(trim(equipment_name)) = ?
            AND lower(trim(equipment_category)) = ?
          LIMIT 1
        `);
        const insertStudioEvidenceStmt = db.prepare(
          'INSERT INTO recording_studio_evidence (recording_id, studio_name, studio_name_canonical, source_playlist_id) VALUES (?, ?, ?, ?)'
        );
        const studioEvidenceExistsStmt = db.prepare(`
          SELECT 1
          FROM recording_studio_evidence
          WHERE recording_id = ?
            AND source_playlist_id = ?
            AND COALESCE(studio_name_canonical, lower(trim(studio_name))) = ?
          LIMIT 1
        `);
        const insertCreditEvidenceStmt = db.prepare(
          'INSERT INTO recording_credit_evidence (recording_id, credit_name, credit_name_canonical, credit_role, source_playlist_id) VALUES (?, ?, ?, ?, ?)'
        );
        const creditEvidenceExistsStmt = db.prepare(`
          SELECT 1
          FROM recording_credit_evidence
          WHERE recording_id = ?
            AND source_playlist_id = ?
            AND COALESCE(credit_name_canonical, lower(trim(credit_name))) = ?
            AND lower(trim(credit_role)) = ?
          LIMIT 1
        `);

        const insertedEvidenceKeys = new Set<string>();
        const insertedStudioEvidenceKeys = new Set<string>();
        const insertedCreditEvidenceKeys = new Set<string>();

        for (const track of parsedTracks) {
          if (!track || typeof track !== 'object') continue;

          const artistValue = typeof (track as { artist?: unknown }).artist === 'string'
            ? (track as { artist: string }).artist.trim()
            : '';
          const titleValue = typeof (track as { song?: unknown }).song === 'string'
            ? (track as { song: string }).song.trim()
            : typeof (track as { title?: unknown }).title === 'string'
              ? (track as { title: string }).title.trim()
              : '';

          if (!artistValue || !titleValue) continue;

          const canonicalKey = buildRecordingCanonicalKey(artistValue, titleValue);
          if (!canonicalKey) continue;

          let recordingId: number;
          const existingRecording = selectRecordingByKeyStmt.get(canonicalKey) as { id: number } | undefined;
          if (existingRecording) {
            recordingId = existingRecording.id;
          } else {
            const insertResult = insertRecordingStmt.run(artistValue, titleValue, canonicalKey);
            recordingId = insertResult.lastInsertRowid as number;
          }

          const spotifyUrlValue = typeof (track as { spotify_url?: unknown }).spotify_url === 'string'
            ? (track as { spotify_url: string }).spotify_url.trim()
            : '';
          if (spotifyUrlValue) {
            updateRecordingSpotifyUrlStmt.run(spotifyUrlValue, recordingId);
          }

          const isrcValueRaw = typeof (track as { isrc?: unknown }).isrc === 'string'
            ? (track as { isrc: string }).isrc
            : '';
          const isrcValue = normalizeIsrc(isrcValueRaw);
          if (isrcValue.length >= 12) {
            updateRecordingIsrcStmt.run(isrcValue, recordingId);
          }

          for (const equipmentItem of equipmentItems) {
            const dedupeKey = `${recordingId}::${equipmentItem.name.toLowerCase()}::${equipmentItem.category.toLowerCase()}`;
            if (insertedEvidenceKeys.has(dedupeKey)) continue;
            insertedEvidenceKeys.add(dedupeKey);

            const equipmentExists = equipmentEvidenceExistsStmt.get(
              recordingId,
              sourcePlaylistId,
              equipmentItem.name.toLowerCase(),
              equipmentItem.category.toLowerCase()
            ) as { 1: number } | undefined;
            if (equipmentExists) continue;

            insertEvidenceStmt.run(
              recordingId,
              equipmentItem.name,
              equipmentItem.category,
              sourcePlaylistId
            );
          }

          for (const rawStudioName of persistedStudiosForEvidence) {
            const studioName = canonicalizeDisplayName(rawStudioName);
            if (!studioName) continue;
            const studioNameCanonical = buildStudioCanonicalKey(studioName);
            if (!studioNameCanonical) continue;
            const studioDedupeKey = `${recordingId}::${studioNameCanonical}`;
            if (insertedStudioEvidenceKeys.has(studioDedupeKey)) continue;
            insertedStudioEvidenceKeys.add(studioDedupeKey);

            const studioExists = studioEvidenceExistsStmt.get(
              recordingId,
              sourcePlaylistId,
              studioNameCanonical
            ) as { 1: number } | undefined;
            if (studioExists) continue;

            insertStudioEvidenceStmt.run(
              recordingId,
              studioName,
              studioNameCanonical,
              sourcePlaylistId
            );
          }

          for (const creditItem of creditItems) {
            const creditDedupeKey = `${recordingId}::${creditItem.nameCanonical}::${creditItem.role.toLowerCase()}`;
            if (insertedCreditEvidenceKeys.has(creditDedupeKey)) continue;
            insertedCreditEvidenceKeys.add(creditDedupeKey);

            const creditExists = creditEvidenceExistsStmt.get(
              recordingId,
              sourcePlaylistId,
              creditItem.nameCanonical,
              creditItem.role.toLowerCase()
            ) as { 1: number } | undefined;
            if (creditExists) continue;

            insertCreditEvidenceStmt.run(
              recordingId,
              creditItem.name,
              creditItem.nameCanonical,
              creditItem.role,
              sourcePlaylistId
            );
          }
        }
    }

    if (Array.isArray(parsedTracks) && parsedTracks.length > 0) {
      const uniqueArtists = new Set<string>();
      for (const track of parsedTracks) {
        if (!track || typeof track !== 'object') continue;
        const artistValue = typeof (track as { artist?: unknown }).artist === 'string'
          ? (track as { artist: string }).artist.trim()
          : '';
        if (!artistValue) continue;
        uniqueArtists.add(artistValue);
      }

      const artists = Array.from(uniqueArtists);
      for (let i = 0; i < artists.length; i += 1) {
        for (let j = i + 1; j < artists.length; j += 1) {
          insertAtlasEdgeEvidence(
            'artist',
            artists[i],
            'artist',
            artists[j],
            'co_artist',
            3
          );
        }
      }

      if (equipmentItems.length > 0 && uniqueArtists.size > 0) {
        const uniqueEquipmentNames = new Set<string>();
        for (const equipmentItem of equipmentItems) {
          if (!equipmentItem.name) continue;
          uniqueEquipmentNames.add(equipmentItem.name);
        }

        for (const equipmentName of uniqueEquipmentNames) {
          for (const artistName of uniqueArtists) {
            insertAtlasEdgeEvidenceOnce(
              'equipment',
              equipmentName,
              'artist',
              artistName,
              'used_by_artist',
              3
            );
          }
        }
      }
    }

    if (equipmentItems.length > 0) {
      const uniqueEquipmentNames = new Set<string>();
      for (const equipmentItem of equipmentItems) {
        if (!equipmentItem.name) continue;
        uniqueEquipmentNames.add(equipmentItem.name);
      }

      const parsedScenes = parseStringArray(scenes);
      const uniqueScenes = new Set<string>();
      for (const item of parsedScenes) {
        const trimmed = item.trim();
        if (trimmed) uniqueScenes.add(trimmed);
      }
      if (uniqueScenes.size === 0 && scene && scene.trim().length > 0) {
        uniqueScenes.add(scene.trim());
      }

      for (const equipmentName of uniqueEquipmentNames) {
        for (const sceneName of uniqueScenes) {
          insertAtlasEdgeEvidenceOnce(
            'equipment',
            equipmentName,
            'scene',
            sceneName,
            'associated_with_scene',
            3
          );
        }
      }
    }

    const normalizedStudios = getNormalizedStudios({ studios, place, places, cities });
    const normalizedCities = getNormalizedCities({ cities, place, places });
    if (normalizedCities.length === 1 && normalizedStudios.length > 0) {
      const cityName = normalizedCities[0];
      const uniqueStudios = new Set(normalizedStudios.filter((studioName) => studioName.trim().length > 0));
      for (const studioName of uniqueStudios) {
        insertAtlasEdgeEvidenceOnce(
          'studio',
          studioName,
          'city',
          cityName,
          'located_in',
          3
        );
      }
    }
  } catch {
    // Evidence insertion is best-effort; playlist save should still succeed.
  }
  
  return {
    id: sourcePlaylistId,
    prompt,
    title,
    description,
    tracks,
    tags,
    place,
    scene,
    places,
    scenes,
    countries,
    cities,
    studios,
    venues,
    influences,
    credits,
    equipment,
    created_at: new Date().toISOString()
  };
}

export function saveArtistMembershipEvidence(
  sourcePlaylistId: number,
  memberships: Array<{ band: string; person: string; role?: string }>
): void {
  if (!Number.isFinite(sourcePlaylistId) || sourcePlaylistId <= 0) return;
  if (!Array.isArray(memberships) || memberships.length === 0) return;

  const insertStmt = db.prepare(
    'INSERT INTO artist_membership_evidence (band_name, band_name_canonical, person_name, person_name_canonical, member_role, source_playlist_id) VALUES (?, ?, ?, ?, ?, ?)'
  );
  const existsStmt = db.prepare(`
    SELECT 1
    FROM artist_membership_evidence
    WHERE source_playlist_id = ?
      AND COALESCE(band_name_canonical, lower(trim(band_name))) = ?
      AND COALESCE(person_name_canonical, lower(trim(person_name))) = ?
    LIMIT 1
  `);

  const dedupe = new Set<string>();

  for (const item of memberships) {
    if (!item || typeof item !== 'object') continue;
    const band = typeof item.band === 'string' ? canonicalizeDisplayName(item.band) : '';
    const person = typeof item.person === 'string' ? canonicalizeDisplayName(item.person) : '';
    const role = typeof item.role === 'string' ? item.role.trim() : '';
    if (!band || !person) continue;

    const bandCanonical = buildArtistCanonicalKey(band);
    const personCanonical = buildPersonCanonicalKey(person);
    if (!bandCanonical || !personCanonical) continue;

    const key = `${bandCanonical}::${personCanonical}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);

    const exists = existsStmt.get(sourcePlaylistId, bandCanonical, personCanonical) as { 1: number } | undefined;
    if (exists) continue;

    insertStmt.run(band, bandCanonical, person, personCanonical, role || null, sourcePlaylistId);
    insertAtlasEdgeEvidence('artist', person, 'artist', band, 'member_of', 3);
  }
}
