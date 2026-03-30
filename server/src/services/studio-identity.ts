import { buildStudioCanonicalKey } from './normalize.js';

export interface StudioIdentity {
  key: string;
  primaryName: string;
  aliases: string[];
  successorNames: string[];
  discogsLabelId?: number;
  musicBrainzPlaceId?: string;
  preferredArtists?: string[];
  activeStartYear?: number;
  activeEndYear?: number;
  curatedRecordedTracks?: Array<{ artist: string; title: string }>;
}

const STUDIO_IDENTITIES: StudioIdentity[] = [
  {
    key: 'emi_studios_stockholm',
    primaryName: 'EMI Studios, Stockholm',
    aliases: [
      'EMI Studio',
      'EMI Studios Stockholm',
      'EMI Studio 1 & 2, Stockholm',
      'EMI-Studio',
      'EMI studios i Skarmarbrink',
      'EMI studios i Skarmarbrink, Stockholm',
      'EMI studios in Skarmarbrink, Stockholm',
      'EMI studios in Stockholm',
    ],
    successorNames: [
      'Cosmos Studios',
      'X-Level Studios',
      'Baggpipe Studios',
      'Kaza Studios',
      'IMRSV Studios, Stockholm',
    ],
    discogsLabelId: 270404,
  },
  {
    key: 'air_studios_london',
    primaryName: 'AIR Studios, London',
    aliases: [
      'AIR Studios London',
      'Air Studios London',
      'AIR Lyndhurst Hall',
      'AIR Studios',
    ],
    successorNames: [],
    discogsLabelId: 29092,
    musicBrainzPlaceId: 'b7a2fdd6-0d78-463a-ba46-8514945d5f4d',
    preferredArtists: [
      'Kate Bush',
      'Elton John',
      'The Police',
      'Dire Straits',
      'Duran Duran',
      'Mike Oldfield',
      'Paul McCartney',
      'George Michael',
      'Phil Collins',
      'Peter Gabriel',
    ],
  },
  {
    key: 'abbey_road_studios_london',
    primaryName: 'Abbey Road Studios, London',
    aliases: [
      'Abbey Road Studios',
      'Abbey Road Studios London',
      'Abbey Road, London',
      'EMI Studios, London',
      'EMI Studios London',
      'EMI Recording Studios, London',
    ],
    successorNames: [],
    musicBrainzPlaceId: 'bd55aeb7-19d1-4607-a500-14b8479d3fed',
    activeStartYear: 1960,
    preferredArtists: [
      'The Beatles',
      'Pink Floyd',
      'Radiohead',
      'Oasis',
      'Amy Winehouse',
      'Duran Duran',
      'Kate Bush',
      'George Harrison',
      'Paul McCartney',
      'John Lennon',
    ],
  },
  {
    key: 'gold_star_studios_los_angeles',
    primaryName: 'Gold Star Studios',
    aliases: [
      'Gold Star Studios, Los Angeles',
      'Gold Star Recording Studios',
      'Gold Star Studios Los Angeles',
      'Gold Star',
    ],
    successorNames: [],
    discogsLabelId: 263247,
    musicBrainzPlaceId: 'd1338bbb-12bb-44e9-810b-6e473ceda061',
    preferredArtists: [
      'The Ronettes',
      'The Crystals',
      'The Righteous Brothers',
      'Darlene Love',
      'The Beach Boys',
      'Ike & Tina Turner',
      'Sonny & Cher',
      'Herb Alpert & The Tijuana Brass',
      'Buffalo Springfield',
      'Cher',
      'Ritchie Valens',
    ],
  },
  {
    key: 'polar_studios_stockholm',
    primaryName: 'Polar Studios, Stockholm',
    aliases: [
      'Polar Studios',
      'Polar Studio',
      'Polarstudion',
      'Polarstudion, Stockholm',
      'Polar Studios Stockholm',
    ],
    successorNames: [],
    musicBrainzPlaceId: '2288e333-936b-4516-bdea-274934476caa',
    preferredArtists: [
      'ABBA',
      'Led Zeppelin',
      'Genesis',
      'Roxy Music',
      'The Ramones',
      'Frida',
      'Agnetha Faltskog',
    ],
    activeStartYear: 1978,
    curatedRecordedTracks: [
      { artist: 'ABBA', title: 'Voulez-Vous' },
      { artist: 'ABBA', title: 'The Winner Takes It All' },
      { artist: 'ABBA', title: 'Super Trouper' },
      { artist: 'ABBA', title: 'One Of Us' },
      { artist: 'ABBA', title: 'The Day Before You Came' },
      { artist: 'Led Zeppelin', title: 'In the Evening' },
      { artist: 'Led Zeppelin', title: 'Fool in the Rain' },
      { artist: 'Led Zeppelin', title: 'All My Love' },
      { artist: 'Led Zeppelin', title: 'Carouselambra' },
      { artist: 'Genesis', title: 'Turn It On Again' },
      { artist: 'Genesis', title: 'Duchess' },
      { artist: 'Genesis', title: 'Misunderstanding' },
    ],
  },
];

interface CompiledStudioIdentity {
  identity: StudioIdentity;
  canonicalNames: string[];
  canonicalSuccessorNames: string[];
}

function compileStudioIdentity(identity: StudioIdentity): CompiledStudioIdentity {
  const canonicalSet = new Set<string>();
  for (const value of [identity.primaryName, ...identity.aliases]) {
    const canonical = buildStudioCanonicalKey(value);
    if (canonical) canonicalSet.add(canonical);
  }

  const successorSet = new Set<string>();
  for (const value of identity.successorNames) {
    const canonical = buildStudioCanonicalKey(value);
    if (canonical) successorSet.add(canonical);
  }

  return {
    identity,
    canonicalNames: Array.from(canonicalSet),
    canonicalSuccessorNames: Array.from(successorSet),
  };
}

const COMPILED_IDENTITIES = STUDIO_IDENTITIES.map(compileStudioIdentity);

function findBestIdentityMatch(value: string): CompiledStudioIdentity | null {
  const canonicalInput = buildStudioCanonicalKey(value);
  if (!canonicalInput) return null;

  let best: { identity: CompiledStudioIdentity; score: number } | null = null;

  for (const compiled of COMPILED_IDENTITIES) {
    for (const candidate of compiled.canonicalNames) {
      let score = 0;
      if (canonicalInput === candidate) {
        score = 1000 + candidate.length;
      } else if (canonicalInput.includes(candidate)) {
        score = 700 + candidate.length;
      } else if (candidate.includes(canonicalInput) && canonicalInput.length >= 4) {
        score = 500 + canonicalInput.length;
      }
      if (score <= 0) continue;
      if (!best || score > best.score) {
        best = { identity: compiled, score };
      }
    }
  }

  return best?.identity || null;
}

export interface ResolvedStudioIdentity {
  key: string;
  primaryName: string;
  acceptedStudioNames: string[];
  excludedSuccessorNames: string[];
  discogsLabelId?: number;
  musicBrainzPlaceId?: string;
  preferredArtists: string[];
  activeStartYear?: number;
  activeEndYear?: number;
  curatedRecordedTracks: Array<{ artist: string; title: string }>;
}

function toResolvedStudioIdentity(compiled: CompiledStudioIdentity): ResolvedStudioIdentity {
  return {
    key: compiled.identity.key,
    primaryName: compiled.identity.primaryName,
    acceptedStudioNames: [compiled.identity.primaryName, ...compiled.identity.aliases],
    excludedSuccessorNames: [...compiled.identity.successorNames],
    discogsLabelId: compiled.identity.discogsLabelId,
    musicBrainzPlaceId: compiled.identity.musicBrainzPlaceId,
    preferredArtists: [...(compiled.identity.preferredArtists || [])],
    activeStartYear: compiled.identity.activeStartYear,
    activeEndYear: compiled.identity.activeEndYear,
    curatedRecordedTracks: [...(compiled.identity.curatedRecordedTracks || [])],
  };
}

export function resolveStudioIdentity(value: string): ResolvedStudioIdentity | null {
  const match = findBestIdentityMatch(value);
  if (!match) return null;
  return toResolvedStudioIdentity(match);
}

export function resolveStudioIdentityFromPrompt(prompt: string): ResolvedStudioIdentity | null {
  return resolveStudioIdentity(prompt);
}
