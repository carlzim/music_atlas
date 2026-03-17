import Database from 'better-sqlite3';
import {
  getArtistAtlas,
  getCreditAtlas,
  getEquipmentAtlas,
  getStudioAtlas,
} from '../services/db.js';

type CaseField = 'relatedArtists' | 'relatedEquipment' | 'memberOf' | 'associatedStudios' | 'relatedStudios' | 'scenes';
type NodeType = 'artist' | 'credit' | 'studio' | 'equipment' | 'prompt';

interface GoldenCase {
  id: string;
  nodeType: NodeType;
  nodeValue: string;
  field: CaseField;
  mode?: 'strict' | 'conditional';
  expectAny?: string[];
  expectNone?: string[];
}

interface CaseResult {
  id: string;
  status: 'PASS' | 'FAIL' | 'SKIP';
  details: string;
}

const GOLDEN_CASES: GoldenCase[] = [
  {
    id: 'no_false_memberof_brian_eno_bowie',
    nodeType: 'credit',
    nodeValue: 'Brian Eno',
    field: 'memberOf',
    expectNone: ['David Bowie', 'Talking Heads', 'U2'],
  },
  {
    id: 'no_false_memberof_brian_eno_heads',
    nodeType: 'artist',
    nodeValue: 'Brian Eno',
    field: 'memberOf',
    expectNone: ['David Bowie', 'Talking Heads', 'U2'],
  },
  {
    id: 'studio_no_playlist_leak_gold_star_lennon',
    nodeType: 'studio',
    nodeValue: 'Gold Star Studios',
    field: 'relatedArtists',
    expectNone: ['John Lennon', 'George Harrison'],
  },
  {
    id: 'equipment_no_generic_microphone',
    nodeType: 'equipment',
    nodeValue: 'microphone',
    field: 'relatedArtists',
    expectNone: ['*any*'],
  },
  {
    id: 'equipment_no_generic_guitar',
    nodeType: 'equipment',
    nodeValue: 'guitar',
    field: 'relatedArtists',
    expectNone: ['*any*'],
  },
  {
    id: 'equipment_no_generic_keyboard',
    nodeType: 'equipment',
    nodeValue: 'keyboard',
    field: 'relatedArtists',
    expectNone: ['*any*'],
  },
  {
    id: 'equipment_no_generic_drums',
    nodeType: 'equipment',
    nodeValue: 'drums',
    field: 'relatedArtists',
    expectNone: ['*any*'],
  },
  {
    id: 'studio_relatedstudios_disabled',
    nodeType: 'studio',
    nodeValue: 'Gold Star Studios',
    field: 'relatedStudios',
    expectNone: ['*any*'],
  },
  {
    id: 'studio_relatedstudios_disabled_hansa',
    nodeType: 'studio',
    nodeValue: 'Hansa Studios',
    field: 'relatedStudios',
    expectNone: ['*any*'],
  },
  {
    id: 'credit_no_false_memberof_david_bowie',
    nodeType: 'credit',
    nodeValue: 'David Bowie',
    field: 'memberOf',
    expectNone: ['U2', 'Talking Heads', 'Brian Eno'],
  },
  {
    id: 'artist_memberof_john_lennon_beatles',
    nodeType: 'artist',
    nodeValue: 'John Lennon',
    field: 'memberOf',
    mode: 'conditional',
    expectAny: ['The Beatles'],
  },
  {
    id: 'artist_memberof_david_gilmour_pink_floyd',
    nodeType: 'artist',
    nodeValue: 'David Gilmour',
    field: 'memberOf',
    mode: 'conditional',
    expectAny: ['Pink Floyd'],
  },
  {
    id: 'artist_memberof_no_false_noel_gallagher_beatles',
    nodeType: 'artist',
    nodeValue: 'Noel Gallagher',
    field: 'memberOf',
    expectNone: ['The Beatles'],
  },
  {
    id: 'artist_memberof_no_false_liam_gallagher_beatles',
    nodeType: 'artist',
    nodeValue: 'Liam Gallagher',
    field: 'memberOf',
    expectNone: ['The Beatles'],
  },
  {
    id: 'artist_memberof_no_false_david_gilmour_beatles',
    nodeType: 'artist',
    nodeValue: 'David Gilmour',
    field: 'memberOf',
    expectNone: ['The Beatles'],
  },
  {
    id: 'artist_memberof_noel_gallagher_oasis',
    nodeType: 'artist',
    nodeValue: 'Noel Gallagher',
    field: 'memberOf',
    mode: 'conditional',
    expectAny: ['Oasis'],
  },
  {
    id: 'artist_memberof_liam_gallagher_oasis',
    nodeType: 'artist',
    nodeValue: 'Liam Gallagher',
    field: 'memberOf',
    mode: 'conditional',
    expectAny: ['Oasis'],
  },
  {
    id: 'artist_relatedartists_oasis_should_include_noel',
    nodeType: 'artist',
    nodeValue: 'Oasis',
    field: 'relatedArtists',
    mode: 'conditional',
    expectAny: ['Noel Gallagher'],
  },
  {
    id: 'artist_relatedartists_oasis_should_include_liam',
    nodeType: 'artist',
    nodeValue: 'Oasis',
    field: 'relatedArtists',
    mode: 'conditional',
    expectAny: ['Liam Gallagher'],
  },
  {
    id: 'artist_relatedartists_pink_floyd_no_beatles_leak',
    nodeType: 'artist',
    nodeValue: 'Pink Floyd',
    field: 'relatedArtists',
    expectNone: ['John Lennon', 'Paul McCartney', 'George Harrison', 'Ringo Starr', 'The Beatles'],
  },
  {
    id: 'warhol_prompt_no_bebop_scene',
    nodeType: 'prompt',
    nodeValue: 'Songs from Albums with cover art created by Andy Warhol (strict check)',
    field: 'scenes',
    mode: 'conditional',
    expectNone: ['Bebop jazz'],
  },
];

function normalize(value: string): string {
  return value.trim().toLowerCase();
}

function getFieldValues(nodeType: NodeType, nodeValue: string, field: CaseField): string[] {
  if (nodeType === 'prompt') {
    if (field !== 'scenes') return [];

    try {
      const db = new Database('playlists.db');
      const row = db.prepare(`
        SELECT scenes
        FROM playlists
        WHERE lower(prompt) = lower(?)
        ORDER BY created_at DESC
        LIMIT 1
      `).get(nodeValue) as { scenes?: string | null } | undefined;

      if (!row || !row.scenes) return [];
      const parsed = JSON.parse(row.scenes);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter((item): item is string => typeof item === 'string' && item.trim().length > 0);
    } catch {
      return [];
    }
  }

  if (nodeType === 'artist') {
    const data = getArtistAtlas(nodeValue);
    if (field === 'relatedArtists') return data.relatedArtists;
    if (field === 'relatedEquipment') return data.relatedEquipment;
    if (field === 'relatedStudios') return data.relatedStudios;
    if (field === 'memberOf') return data.memberOf || [];
    return [];
  }

  if (nodeType === 'credit') {
    const data = getCreditAtlas(nodeValue);
    if (field === 'relatedArtists') return data.relatedArtists;
    if (field === 'memberOf') return data.memberOf;
    if (field === 'associatedStudios') return data.associatedStudios;
    return [];
  }

  if (nodeType === 'studio') {
    const data = getStudioAtlas(nodeValue);
    if (field === 'relatedArtists') return data.relatedArtists;
    if (field === 'relatedEquipment') return data.relatedEquipment;
    if (field === 'relatedStudios') return data.relatedStudios;
    return [];
  }

  const data = getEquipmentAtlas(nodeValue);
  if (field === 'relatedArtists') return data.relatedArtists;
  if (field === 'relatedStudios') return data.relatedStudios;
  return [];
}

function evaluateCase(testCase: GoldenCase): CaseResult {
  const values = getFieldValues(testCase.nodeType, testCase.nodeValue, testCase.field);
  const normalizedValues = values.map(normalize);

  const hasPositiveExpectation = Array.isArray(testCase.expectAny) && testCase.expectAny.length > 0;
  if ((testCase.mode || 'strict') === 'conditional' && normalizedValues.length === 0 && hasPositiveExpectation) {
    return {
      id: testCase.id,
      status: 'SKIP',
      details: `No evidence yet for ${testCase.nodeType}:${testCase.nodeValue} ${testCase.field}`,
    };
  }

  const forbidden = (testCase.expectNone || []).map(normalize);
  if (forbidden.includes('*any*')) {
    if (normalizedValues.length > 0) {
      return {
        id: testCase.id,
        status: 'FAIL',
        details: `Expected empty ${testCase.field}, got ${values.join(', ')}`,
      };
    }
  } else {
    const foundForbidden = forbidden.filter((item) => normalizedValues.includes(item));
    if (foundForbidden.length > 0) {
      return {
        id: testCase.id,
        status: 'FAIL',
        details: `Found forbidden values in ${testCase.field}: ${foundForbidden.join(', ')}`,
      };
    }
  }

  const expectedAny = (testCase.expectAny || []).map(normalize);
  if (expectedAny.length > 0) {
    const foundAny = expectedAny.some((item) => normalizedValues.includes(item));
    if (!foundAny) {
      return {
        id: testCase.id,
        status: 'FAIL',
        details: `Expected one of [${(testCase.expectAny || []).join(', ')}] in ${testCase.field}, got [${values.join(', ')}]`,
      };
    }
  }

  return {
    id: testCase.id,
    status: 'PASS',
    details: `${testCase.field}: [${values.join(', ')}]`,
  };
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const results = GOLDEN_CASES.map(evaluateCase);
  const passed = results.filter((result) => result.status === 'PASS');
  const failed = results.filter((result) => result.status === 'FAIL');
  const skipped = results.filter((result) => result.status === 'SKIP');

  console.log('[eval] Atlas quality harness');
  console.log(`[eval] Cases: ${results.length}, Passed: ${passed.length}, Failed: ${failed.length}, Skipped: ${skipped.length}`);

  for (const result of results) {
    console.log(`[eval] ${result.status} ${result.id} -> ${result.details}`);
  }

  if (strict && failed.length > 0) {
    process.exitCode = 1;
  }
}

run();
