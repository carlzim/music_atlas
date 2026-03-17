import Database from 'better-sqlite3';
import { pathToFileURL } from 'url';
import {
  getPlaylistByCacheKey,
  getPlaylistByPrompt,
  savePlaylist,
} from '../services/db.js';
import { buildCreditCanonicalKey, canonicalizeDisplayName } from '../services/normalize.js';

export const SOURCE_PROMPT = '[system] credit evidence backfill v1';

export const CREDIT_SEED: Array<{
  artist: string;
  title: string;
  creditName: string;
  creditRole: string;
}> = [
  { artist: 'Led Zeppelin', title: 'Fool in the Rain', creditName: 'Jimmy Page', creditRole: 'producer' },
  { artist: 'Led Zeppelin', title: 'Carouselambra', creditName: 'Jimmy Page', creditRole: 'producer' },
  { artist: 'ABBA', title: 'The Winner Takes It All', creditName: 'Benny Andersson', creditRole: 'producer' },
  { artist: 'ABBA', title: 'Lay All Your Love on Me', creditName: 'Benny Andersson', creditRole: 'producer' },
  { artist: 'ABBA', title: 'Gimme! Gimme! Gimme! (A Man After Midnight)', creditName: 'Benny Andersson', creditRole: 'producer' },
  { artist: 'ABBA', title: 'The Winner Takes It All', creditName: 'Bjorn Ulvaeus', creditRole: 'producer' },
  { artist: 'ABBA', title: 'Lay All Your Love on Me', creditName: 'Bjorn Ulvaeus', creditRole: 'producer' },
  { artist: 'ABBA', title: 'Gimme! Gimme! Gimme! (A Man After Midnight)', creditName: 'Bjorn Ulvaeus', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Heroes', creditName: 'David Bowie', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Heroes', creditName: 'Tony Visconti', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Life on Mars?', creditName: 'Ken Scott', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Life on Mars?', creditName: 'David Bowie', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Ziggy Stardust', creditName: 'Ken Scott', creditRole: 'producer' },
  { artist: 'David Bowie', title: 'Ziggy Stardust', creditName: 'David Bowie', creditRole: 'producer' },
  { artist: 'The Beatles', title: 'A Day in the Life', creditName: 'George Martin', creditRole: 'producer' },
  { artist: 'The Beatles', title: 'Hey Jude', creditName: 'George Martin', creditRole: 'producer' },
  { artist: 'The Beatles', title: 'Strawberry Fields Forever', creditName: 'George Martin', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'California Girls', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'God Only Knows', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'Wouldn\'t It Be Nice', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'Good Vibrations', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'Caroline, No', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'I Get Around', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'The Beach Boys', title: 'Don\'t Worry Baby', creditName: 'Brian Wilson', creditRole: 'producer' },
  { artist: 'Eagles', title: 'Take It Easy', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'Eagles', title: 'Witchy Woman', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'Eagles', title: 'Tequila Sunrise', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'Eagles', title: 'Desperado', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'The Who', title: 'Baba O\'Riley', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'The Who', title: 'Won\'t Get Fooled Again', creditName: 'Glyn Johns', creditRole: 'producer' },
  { artist: 'My Bloody Valentine', title: 'Only Shallow', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'My Bloody Valentine', title: 'When You Sleep', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Nine Inch Nails', title: 'Closer', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Nine Inch Nails', title: 'The Great Below', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'The Smashing Pumpkins', title: 'Bullet with Butterfly Wings', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Placebo', title: 'Pure Morning', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'The Killers', title: 'When You Were Young', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Foals', title: 'Spanish Sahara', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Interpol', title: 'Evil', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Yeah Yeah Yeahs', title: 'Maps', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Arctic Monkeys', title: 'Do I Wanna Know?', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'U2', title: 'Vertigo', creditName: 'Alan Moulder', creditRole: 'engineer' },
  { artist: 'Antônio Carlos Jobim', title: 'Wave', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Triste', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Look to the Sky', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'The Red Blouse', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Batidinha', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Captain Bacardi', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'The Girl from Ipanema', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Corcovado (Quiet Nights of Quiet Stars)', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Antônio Carlos Jobim', title: 'Desafinado', creditName: 'Claus Ogerman', creditRole: 'arranger' },
  { artist: 'Oscar Peterson', title: 'Sunny', creditName: 'Claus Ogerman', creditRole: 'arranger' },
];

function normalizeRecordingToken(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, ' ');
}

function buildRecordingCanonicalKey(artist: string, title: string): string {
  return `${normalizeRecordingToken(artist)}::${normalizeRecordingToken(title)}`;
}

function ensureSourcePlaylistId(): number {
  const existing = getPlaylistByPrompt(SOURCE_PROMPT) || getPlaylistByCacheKey(SOURCE_PROMPT);
  if (existing) return existing.id;

  const created = savePlaylist(
    SOURCE_PROMPT,
    'System credit evidence seed',
    'Synthetic playlist row used as source for explicit recording credit evidence seeding.',
    '[]',
    JSON.stringify(['system_seed', 'credit'])
  );
  return created.id;
}

export function runCreditBackfill(): void {
  const sourcePlaylistId = ensureSourcePlaylistId();
  const db = new Database('playlists.db');

  const selectRecording = db.prepare(`
    SELECT id
    FROM recordings
    WHERE lower(trim(artist)) = lower(trim(?))
      AND lower(trim(title)) = lower(trim(?))
    LIMIT 1
  `);
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

  const creditExists = db.prepare(`
    SELECT 1
    FROM recording_credit_evidence
    WHERE recording_id = ?
      AND source_playlist_id = ?
      AND COALESCE(credit_name_canonical, lower(trim(credit_name))) = ?
      AND lower(trim(credit_role)) = ?
    LIMIT 1
  `);

  const insertCredit = db.prepare(`
    INSERT INTO recording_credit_evidence (recording_id, credit_name, credit_name_canonical, credit_role, source_playlist_id)
    VALUES (?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  let skippedExisting = 0;
  let insertedRecordings = 0;

  for (const seed of CREDIT_SEED) {
    const recordingCanonicalKey = buildRecordingCanonicalKey(seed.artist, seed.title);
    if (!recordingCanonicalKey) continue;

    let recording = selectRecordingByCanonical.get(recordingCanonicalKey) as { id: number } | undefined;
    if (!recording) {
      recording = selectRecording.get(seed.artist, seed.title) as { id: number } | undefined;
    }
    if (!recording) {
      const insertResult = insertRecording.run(seed.artist.trim(), seed.title.trim(), recordingCanonicalKey);
      recording = { id: insertResult.lastInsertRowid as number };
      insertedRecordings += 1;
    }

    const creditName = canonicalizeDisplayName(seed.creditName);
    const creditCanonical = buildCreditCanonicalKey(creditName);
    const role = seed.creditRole.trim().toLowerCase();
    if (!creditName || !creditCanonical || !role) continue;

    const exists = creditExists.get(recording.id, sourcePlaylistId, creditCanonical, role) as { 1: number } | undefined;
    if (exists) {
      skippedExisting += 1;
      continue;
    }

    insertCredit.run(recording.id, creditName, creditCanonical, role, sourcePlaylistId);
    inserted += 1;
  }

  console.log('[backfill:credit] done');
  console.log(`[backfill:credit] source_playlist_id=${sourcePlaylistId}`);
  console.log(`[backfill:credit] seed_rows=${CREDIT_SEED.length}`);
  console.log(`[backfill:credit] inserted_recordings=${insertedRecordings}`);
  console.log(`[backfill:credit] inserted=${inserted}`);
  console.log(`[backfill:credit] skipped_existing=${skippedExisting}`);
}

const entryPath = process.argv[1] ? pathToFileURL(process.argv[1]).href : '';
if (entryPath && import.meta.url === entryPath) {
  runCreditBackfill();
}
