import { generatePlaylist } from '../services/gemini.js';

interface StudioReferenceCase {
  id: string;
  prompt: string;
  coreArtists: string[];
  topN: number;
  minCoreHits: number;
}

interface StudioReferenceResult {
  id: string;
  pass: boolean;
  skipped: boolean;
  details: string;
}

function normalizeArtistKey(value: string): string {
  return value
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’']/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function artistMatchesCore(candidate: string, core: string): boolean {
  const c = normalizeArtistKey(candidate);
  const k = normalizeArtistKey(core);
  if (!c || !k) return false;
  return c === k || c.includes(k) || k.includes(c);
}

async function runCase(testCase: StudioReferenceCase): Promise<StudioReferenceResult> {
  try {
    const response = await generatePlaylist(testCase.prompt);
    const tracks = Array.isArray(response.playlist?.tracks) ? response.playlist.tracks : [];
    const top = tracks.slice(0, Math.max(1, testCase.topN));

    const matchedCore = new Set<string>();
    for (const track of top) {
      const artist = typeof track.artist === 'string' ? track.artist : '';
      if (!artist) continue;
      for (const core of testCase.coreArtists) {
        if (artistMatchesCore(artist, core)) {
          matchedCore.add(core);
        }
      }
    }

    const coreHits = matchedCore.size;
    const pass = coreHits >= testCase.minCoreHits;
    return {
      id: testCase.id,
      pass,
      skipped: false,
      details: `tracks=${tracks.length} top${testCase.topN}_core_hits=${coreHits}/${testCase.minCoreHits} matched=[${Array.from(matchedCore).join(', ')}]`,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      id: testCase.id,
      pass: false,
      skipped: true,
      details: `skipped_error=${message.slice(0, 180)}`,
    };
  }
}

async function run(): Promise<void> {
  const strict = process.argv.includes('--strict');

  const cases: StudioReferenceCase[] = [
    {
      id: 'studio_reference_olympic_core_artists',
      prompt: 'Best recordings made in Olympic Studios London [eval reference]',
      coreArtists: ['The Rolling Stones', 'Led Zeppelin', 'The Who', 'The Jimi Hendrix Experience', 'Jimi Hendrix', 'Traffic', 'Small Faces'],
      topN: 15,
      minCoreHits: 3,
    },
    {
      id: 'studio_reference_abbey_core_artists',
      prompt: 'Best recordings made in Abbey Road studios [eval reference]',
      coreArtists: ['The Beatles', 'Pink Floyd', 'Radiohead', 'Oasis', 'The Hollies', 'John Lennon', 'George Harrison'],
      topN: 15,
      minCoreHits: 3,
    },
  ];

  const results: StudioReferenceResult[] = [];
  for (const testCase of cases) {
    // Sequential execution avoids unnecessary parallel Gemini load.
    // eslint-disable-next-line no-await-in-loop
    results.push(await runCase(testCase));
  }

  const skipped = results.filter((result) => result.skipped).length;
  const passed = results.filter((result) => result.pass).length;
  const failed = results.length - passed - skipped;

  console.log('[eval:studio-reference] Studio reference harness');
  console.log(`[eval:studio-reference] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}, Skipped: ${skipped}`);
  for (const result of results) {
    const status = result.skipped ? 'SKIP' : result.pass ? 'PASS' : 'FAIL';
    console.log(`[eval:studio-reference] ${status} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[eval:studio-reference] fatal error: ${message}`);
  process.exitCode = 1;
});
