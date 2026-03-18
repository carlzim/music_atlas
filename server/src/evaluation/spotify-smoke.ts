import 'dotenv/config';
import { searchTrackWithDiagnostics } from '../services/spotify.js';

interface SpotifySmokeCase {
  id: string;
  artist: string;
  song: string;
  prompt: string;
  expectedHint?: string;
  minScore?: number;
  required?: boolean;
  forbidTerms?: string[];
}

const CASES: SpotifySmokeCase[] = [
  {
    id: 'venue_cbgb_should_prefer_cbgb_context',
    artist: 'Ramones',
    song: 'Blitzkrieg Bop',
    prompt: 'Best live recordings from CBGB',
    minScore: 8,
    required: true,
  },
  {
    id: 'studio_context_should_avoid_live_archive_bias',
    artist: 'Neil Young',
    song: 'Harvest',
    prompt: "The best songs from Neil Young's so-called ditch trilogy",
    expectedHint: 'spotify.com/track/',
    minScore: 0,
    required: true,
  },
  {
    id: 'venue_hollywood_bowl_should_prefer_live_context',
    artist: 'The Beatles',
    song: "A Hard Day's Night",
    prompt: 'The best recordings from The Hollywood Bowl',
    expectedHint: 'spotify.com/track/',
    minScore: 10,
    required: false,
  },
  {
    id: 'studio_prompt_should_not_require_live',
    artist: 'David Bowie',
    song: 'Heroes',
    prompt: 'Best songs recorded at Hansa Studios',
    expectedHint: 'spotify.com/track/',
    minScore: 4,
    required: false,
  },
  {
    id: 'bowie_berlin_prompt_should_avoid_soundtrack_or_remix_bias',
    artist: 'David Bowie',
    song: 'A New Career in a New Town',
    prompt: 'The best of David Bowies Berlin era',
    expectedHint: 'spotify.com/track/',
    minScore: 1,
    required: false,
    forbidTerms: ['soundtrack', 'remix', 'live', 'from the soundtrack', 'motion picture'],
  },
  {
    id: 'non_film_prompt_should_avoid_soundtrack_album_bias',
    artist: 'David Bowie',
    song: 'Speed of Life',
    prompt: 'Best songs from the Bowie Berlin trilogy',
    expectedHint: 'spotify.com/track/',
    minScore: 1,
    required: false,
    forbidTerms: ['soundtrack', 'motion picture', 'film'],
  },
  {
    id: 'britpop_prompt_should_match_standard_version',
    artist: 'Oasis',
    song: 'Wonderwall',
    prompt: "Best of 90's britpop",
    expectedHint: 'spotify.com/track/',
    minScore: 4,
    required: false,
  },
];

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');
  console.log('[eval:spotify] Starting Spotify smoke evaluation');
  let failedRequired = 0;

  for (const testCase of CASES) {
    const result = await searchTrackWithDiagnostics(testCase.artist, testCase.song, testCase.prompt);
    const url = result.spotify_url || '';

    if (!url) {
      console.log(`[eval:spotify] WARN ${testCase.id} -> no match`);
      if (strict && testCase.required) failedRequired += 1;
      continue;
    }

    const normalizedUrl = url.toLowerCase();
    if (testCase.expectedHint && !normalizedUrl.includes(testCase.expectedHint.toLowerCase())) {
      console.log(`[eval:spotify] WARN ${testCase.id} -> matched ${url}`);
      if (strict && testCase.required) failedRequired += 1;
      continue;
    }

    if (typeof testCase.minScore === 'number' && typeof result.score !== 'number') {
      console.log(`[eval:spotify] WARN ${testCase.id} -> matched ${url} but score missing`);
      if (strict && testCase.required) failedRequired += 1;
      continue;
    }

    if (typeof testCase.minScore === 'number' && typeof result.score === 'number' && result.score < testCase.minScore) {
      console.log(`[eval:spotify] WARN ${testCase.id} -> matched ${url} score=${result.score} < minScore=${testCase.minScore}`);
      if (strict && testCase.required) failedRequired += 1;
      continue;
    }

    if (Array.isArray(testCase.forbidTerms) && testCase.forbidTerms.length > 0) {
      const haystack = `${result.matchedTitle || ''} ${result.matchedAlbumTitle || ''}`.toLowerCase();
      const hit = testCase.forbidTerms.find((term) => haystack.includes(term.toLowerCase()));
      if (hit) {
        console.log(`[eval:spotify] WARN ${testCase.id} -> matched ${url} contains forbidden term "${hit}" in title/album`);
        if (strict && testCase.required) failedRequired += 1;
        continue;
      }
    }

    console.log(`[eval:spotify] PASS ${testCase.id} -> matched ${url} score=${result.score}`);
  }

  if (strict && failedRequired > 0) {
    console.error(`[eval:spotify] Strict mode failed required cases: ${failedRequired}`);
    process.exitCode = 1;
  }

  console.log('[eval:spotify] Done');
}

run().catch((error) => {
  console.error('[eval:spotify] Failed:', error);
  process.exitCode = 1;
});
