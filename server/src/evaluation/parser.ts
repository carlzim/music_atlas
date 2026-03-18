import {
  detectCreditPromptForEval,
  extractPlaceEntityFromPromptForEval,
  parsePlaylistResponseForEval,
} from '../services/gemini.js';

interface ParserCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

function runPlaceExtractionCase(): ParserCaseResult {
  const id = 'place_extract_capitol_studios_using_clause';
  const prompt = 'Songs recorded at Capitol Studios using their classic echo chamber';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Capitol Studios';
  return {
    id,
    pass,
    details: `expected="Capitol Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionMadeInCase(): ParserCaseResult {
  const id = 'place_extract_olympic_studios_made_in_phrase';
  const prompt = 'The best recordings made in Olympic Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Olympic Studios';
  return {
    id,
    pass,
    details: `expected="Olympic Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionMadeAtCase(): ParserCaseResult {
  const id = 'place_extract_olympic_studios_made_at_phrase';
  const prompt = 'Best songs made at Olympic Studios in London';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Olympic Studios';
  return {
    id,
    pass,
    details: `expected="Olympic Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionDoneInCase(): ParserCaseResult {
  const id = 'place_extract_trident_done_in_phrase';
  const prompt = 'The best recordings done in Trident Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Trident Studios';
  return {
    id,
    pass,
    details: `expected="Trident Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionSongsAtCase(): ParserCaseResult {
  const id = 'place_extract_songs_at_olympic_phrase';
  const prompt = 'Best songs at Olympic Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Olympic Studios';
  return {
    id,
    pass,
    details: `expected="Olympic Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionTracksInCase(): ParserCaseResult {
  const id = 'place_extract_tracks_in_hansa_phrase';
  const prompt = 'Best tracks in Hansa Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Hansa Studios';
  return {
    id,
    pass,
    details: `expected="Hansa Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionFromTheCase(): ParserCaseResult {
  const id = 'place_extract_tracks_from_the_phrase';
  const prompt = 'Best tracks from the Olympic Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Olympic Studios';
  return {
    id,
    pass,
    details: `expected="Olympic Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionRecordedAtTheCase(): ParserCaseResult {
  const id = 'place_extract_recorded_at_the_phrase';
  const prompt = 'Best songs recorded at the Hansa Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Hansa Studios';
  return {
    id,
    pass,
    details: `expected="Hansa Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionRecordingHistoryOfCase(): ParserCaseResult {
  const id = 'place_extract_recording_history_of_studio';
  const prompt = 'The recording history of Sun Studio';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Sun Studio';
  return {
    id,
    pass,
    details: `expected="Sun Studio" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionStoryOfCase(): ParserCaseResult {
  const id = 'place_extract_story_of_studio';
  const prompt = 'The story of Sun Studio';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Sun Studio';
  return {
    id,
    pass,
    details: `expected="Sun Studio" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionStoryFromCase(): ParserCaseResult {
  const id = 'place_extract_story_from_studio';
  const prompt = 'The story from FAME Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'FAME Studios';
  return {
    id,
    pass,
    details: `expected="FAME Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionRecordingHistoryAtCase(): ParserCaseResult {
  const id = 'place_extract_recording_history_at_studio';
  const prompt = 'Recording history at the Muscle Shoals Sound Studio';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Muscle Shoals Sound Studio';
  return {
    id,
    pass,
    details: `expected="Muscle Shoals Sound Studio" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionTimelineOfCase(): ParserCaseResult {
  const id = 'place_extract_timeline_of_studio';
  const prompt = 'A timeline of the Hansa Studios';
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Hansa Studios';
  return {
    id,
    pass,
    details: `expected="Hansa Studios" actual="${extracted || ''}"`,
  };
}

function runPlaceExtractionPossessiveStudioCase(): ParserCaseResult {
  const id = 'place_extract_possessive_studio_phrase';
  const prompt = "Sun Studio's best recordings";
  const extracted = extractPlaceEntityFromPromptForEval(prompt);
  const pass = extracted === 'Sun Studio';
  return {
    id,
    pass,
    details: `expected="Sun Studio" actual="${extracted || ''}"`,
  };
}

function runCoverArtCreditCase(): ParserCaseResult {
  const id = 'credit_detect_cover_art_created_by';
  const prompt = 'Songs from Albums with cover art created by Andy Warhol';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(detected && detected.role === 'cover_designer' && detected.name === 'Andy Warhol');
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runSleeveDesignCreditCase(): ParserCaseResult {
  const id = 'credit_detect_sleeve_design_by';
  const prompt = 'songs from albums with sleeve design by Peter Saville';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(detected && detected.role === 'cover_designer' && detected.name === 'Peter Saville');
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runArtDirectionCreditCase(): ParserCaseResult {
  const id = 'credit_detect_art_direction_by';
  const prompt = 'Albums with art direction by Peter Saville';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(detected && detected.role === 'art_director' && detected.name === 'Peter Saville');
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runPhotographyCreditCase(): ParserCaseResult {
  const id = 'credit_detect_photography_by';
  const prompt = 'Tracks with photography by Anton Corbijn';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(detected && detected.role === 'photographer' && detected.name === 'Anton Corbijn');
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runMembersOfCreditCase(): ParserCaseResult {
  const id = 'credit_detect_produced_by_members_of';
  const prompt = 'Tracks produced by members of The Byrds';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'The Byrds'
    && detected.membersOfBand === true
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runAsAProducerCreditCase(): ParserCaseResult {
  const id = 'credit_detect_with_name_as_a_producer';
  const prompt = "The best Beach Boys songs from the 60's with Brian Wilson as a producer";
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Brian Wilson'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProductionsOfProducerCreditCase(): ParserCaseResult {
  const id = 'credit_detect_productions_of_swedish_producer';
  const prompt = 'The best productions of Swedish producer Anders Burman.';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProductionsByProducerCreditCase(): ParserCaseResult {
  const id = 'credit_detect_productions_by_swedish_producer';
  const prompt = 'The best productions by Swedish producer Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runWorkOfProducerCreditCase(): ParserCaseResult {
  const id = 'credit_detect_work_of_swedish_producer';
  const prompt = 'The work of Swedish producer Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerNameProductionsCase(): ParserCaseResult {
  const id = 'credit_detect_producer_name_productions';
  const prompt = 'Anders Burman productions from the 60s';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducedByAdjectiveProducerCase(): ParserCaseResult {
  const id = 'credit_detect_produced_by_adjective_producer';
  const prompt = 'Tracks produced by legendary producer Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runSongsByProducerCase(): ParserCaseResult {
  const id = 'credit_detect_songs_by_producer';
  const prompt = 'Best songs by producer Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerColonCase(): ParserCaseResult {
  const id = 'credit_detect_producer_colon_name';
  const prompt = 'Producer: Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runHyphenProducedCase(): ParserCaseResult {
  const id = 'credit_detect_hyphen_produced_phrase';
  const prompt = 'Anders Burman-produced songs from Sweden';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerCommaCase(): ParserCaseResult {
  const id = 'credit_detect_producer_comma_name';
  const prompt = 'Best songs by producer, Anders Burman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerPossessiveCase(): ParserCaseResult {
  const id = 'credit_detect_producer_possessive_productions';
  const prompt = "Anders Burman's productions";
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Anders Burman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runRejectGenericProductionsPromptCase(): ParserCaseResult {
  const id = 'credit_reject_generic_productions_prompt';
  const prompt = 'The best productions from Sweden';
  const detected = detectCreditPromptForEval(prompt);
  const pass = detected === null;
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runRejectEraProductionsPromptCase(): ParserCaseResult {
  const id = 'credit_reject_era_productions_prompt';
  const prompt = "The best productions of the 60's";
  const detected = detectCreditPromptForEval(prompt);
  const pass = detected === null;
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runEngineeredByEngineerCase(): ParserCaseResult {
  const id = 'credit_detect_engineered_by_engineer';
  const prompt = 'Recordings engineered by legendary engineer Alan Moulder';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'engineer'
    && detected.name === 'Alan Moulder'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runTracksByEngineerCase(): ParserCaseResult {
  const id = 'credit_detect_tracks_by_engineer';
  const prompt = 'Best tracks by engineer Alan Moulder';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'engineer'
    && detected.name === 'Alan Moulder'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runEngineerColonCase(): ParserCaseResult {
  const id = 'credit_detect_engineer_colon_name';
  const prompt = 'Engineer: Alan Moulder';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'engineer'
    && detected.name === 'Alan Moulder'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runArrangedByArrangerCase(): ParserCaseResult {
  const id = 'credit_detect_arranged_by_arranger';
  const prompt = 'Songs arranged by Swedish arranger Claus Ogerman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'arranger'
    && detected.name === 'Claus Ogerman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runTracksByArrangerCase(): ParserCaseResult {
  const id = 'credit_detect_tracks_by_arranger';
  const prompt = 'Best tracks by arranger Claus Ogerman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'arranger'
    && detected.name === 'Claus Ogerman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runArrangerColonCase(): ParserCaseResult {
  const id = 'credit_detect_arranger_colon_name';
  const prompt = 'Arranger: Claus Ogerman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'arranger'
    && detected.name === 'Claus Ogerman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerSwedishProducedByCase(): ParserCaseResult {
  const id = 'credit_detect_swedish_producerade_av';
  const prompt = 'Låtar producerade av Quincy Jones';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'Quincy Jones'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runProducerSwedishWorkAsCase(): ParserCaseResult {
  const id = 'credit_detect_swedish_work_as_producer';
  const prompt = 'Arbeten av David Crosby som producent';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'producer'
    && detected.name === 'David Crosby'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runEngineerSwedishMixedByCase(): ParserCaseResult {
  const id = 'credit_detect_swedish_mixade_av';
  const prompt = 'Spår mixade av Alan Parsons';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'engineer'
    && detected.name === 'Alan Parsons'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runEngineerEnglishMixedByCase(): ParserCaseResult {
  const id = 'credit_detect_english_mixed_by';
  const prompt = 'Tracks mixed by Alan Parsons';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'engineer'
    && detected.name === 'Alan Parsons'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runArrangerSwedishArrangedByCase(): ParserCaseResult {
  const id = 'credit_detect_swedish_arrangerade_av';
  const prompt = 'Inspelningar arrangerade av Claus Ogerman';
  const detected = detectCreditPromptForEval(prompt);
  const pass = Boolean(
    detected
    && detected.role === 'arranger'
    && detected.name === 'Claus Ogerman'
  );
  return {
    id,
    pass,
    details: `detected=${JSON.stringify(detected)}`,
  };
}

function runJsonParseRepairCase(): ParserCaseResult {
  const id = 'playlist_parse_repair_smart_quotes_and_missing_brace';
  const nearJson = `{
  “title”: “Test Playlist”,
  “description”: “A short description”,
  “tracks”: [
    {
      “artist”: “David Bowie”,
      “song”: “Heroes”,
      “reason”: “Line with quoted text “classic era” and parser should recover.”
    ,
    {
      “artist”: “The Beatles”,
      “song”: “Help!”,
      “reason”: “Second track.”
    }
  ]
}`;

  try {
    const parsed = parsePlaylistResponseForEval(nearJson);
    const pass = Boolean(parsed.title && Array.isArray(parsed.tracks) && parsed.tracks.length === 2);
    return {
      id,
      pass,
      details: `title="${parsed.title}" tracks=${parsed.tracks.length}`,
    };
  } catch (error) {
    return {
      id,
      pass: false,
      details: `parse error=${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

function runArtistFieldFeaturingSplitCase(): ParserCaseResult {
  const id = 'playlist_parse_artist_field_featuring_split';
  const payload = JSON.stringify({
    title: 'Test',
    description: 'Test',
    tracks: [
      {
        artist: 'Lill Lindfors, duett med Billy Gezon',
        song: 'Sa vill jag bli',
        reason: 'Test reason',
      },
    ],
  });

  try {
    const parsed = parsePlaylistResponseForEval(payload);
    const track = parsed.tracks[0] as {
      artist?: string;
      featured_artists?: string[];
      artist_display?: string;
    };
    const featured = Array.isArray(track.featured_artists) ? track.featured_artists : [];
    const pass = track.artist === 'Lill Lindfors'
      && featured.includes('Billy Gezon')
      && track.artist_display === 'Lill Lindfors, duett med Billy Gezon';
    return {
      id,
      pass,
      details: `artist="${track.artist || ''}" featured=${JSON.stringify(featured)} display="${track.artist_display || ''}"`,
    };
  } catch (error) {
    return {
      id,
      pass: false,
      details: `parse error=${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

function runSongTitleFeaturingExtractCase(): ParserCaseResult {
  const id = 'playlist_parse_song_title_featuring_extract';
  const payload = JSON.stringify({
    title: 'Test',
    description: 'Test',
    tracks: [
      {
        artist: 'Pugh Rogefeldt',
        song: 'Bla jeans och stjarnljus (feat. Lill Lindfors)',
        reason: 'Test reason',
      },
    ],
  });

  try {
    const parsed = parsePlaylistResponseForEval(payload);
    const track = parsed.tracks[0] as {
      artist?: string;
      song?: string;
      featured_artists?: string[];
    };
    const featured = Array.isArray(track.featured_artists) ? track.featured_artists : [];
    const pass = track.artist === 'Pugh Rogefeldt'
      && track.song === 'Bla jeans och stjarnljus'
      && featured.includes('Lill Lindfors');
    return {
      id,
      pass,
      details: `artist="${track.artist || ''}" song="${track.song || ''}" featured=${JSON.stringify(featured)}`,
    };
  } catch (error) {
    return {
      id,
      pass: false,
      details: `parse error=${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

function runFullArtistNamePreservedCase(): ParserCaseResult {
  const id = 'playlist_parse_full_artist_name_preserved';
  const payload = JSON.stringify({
    title: 'Test',
    description: 'Test',
    tracks: [
      {
        artist: 'Ann-Louise Hansson',
        song: 'Alla min langtan',
        reason: 'Test reason',
      },
    ],
  });

  try {
    const parsed = parsePlaylistResponseForEval(payload);
    const track = parsed.tracks[0] as { artist?: string };
    const pass = track.artist === 'Ann-Louise Hansson';
    return {
      id,
      pass,
      details: `artist="${track.artist || ''}"`,
    };
  } catch (error) {
    return {
      id,
      pass: false,
      details: `parse error=${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

function runArtistFieldSwedishOchSplitCase(): ParserCaseResult {
  const id = 'playlist_parse_artist_field_swedish_och_split';
  const payload = JSON.stringify({
    title: 'Test',
    description: 'Test',
    tracks: [
      {
        artist: 'Lill Lindfors och Anders Linder',
        song: 'Tank vilket liv',
        reason: 'Test reason',
      },
    ],
  });

  try {
    const parsed = parsePlaylistResponseForEval(payload);
    const track = parsed.tracks[0] as {
      artist?: string;
      featured_artists?: string[];
      artist_display?: string;
    };
    const featured = Array.isArray(track.featured_artists) ? track.featured_artists : [];
    const pass = track.artist === 'Lill Lindfors'
      && featured.includes('Anders Linder')
      && track.artist_display === 'Lill Lindfors och Anders Linder';
    return {
      id,
      pass,
      details: `artist="${track.artist || ''}" featured=${JSON.stringify(featured)} display="${track.artist_display || ''}"`,
    };
  } catch (error) {
    return {
      id,
      pass: false,
      details: `parse error=${error instanceof Error ? error.message : String(error)}`,
    };
  }
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const results: ParserCaseResult[] = [
    runPlaceExtractionCase(),
    runPlaceExtractionMadeInCase(),
    runPlaceExtractionMadeAtCase(),
    runPlaceExtractionDoneInCase(),
    runPlaceExtractionSongsAtCase(),
    runPlaceExtractionTracksInCase(),
    runPlaceExtractionFromTheCase(),
    runPlaceExtractionRecordedAtTheCase(),
    runPlaceExtractionRecordingHistoryOfCase(),
    runPlaceExtractionStoryOfCase(),
    runPlaceExtractionStoryFromCase(),
    runPlaceExtractionRecordingHistoryAtCase(),
    runPlaceExtractionTimelineOfCase(),
    runPlaceExtractionPossessiveStudioCase(),
    runCoverArtCreditCase(),
    runSleeveDesignCreditCase(),
    runArtDirectionCreditCase(),
    runPhotographyCreditCase(),
    runMembersOfCreditCase(),
    runAsAProducerCreditCase(),
    runProductionsOfProducerCreditCase(),
    runProductionsByProducerCreditCase(),
    runWorkOfProducerCreditCase(),
    runProducerNameProductionsCase(),
    runProducedByAdjectiveProducerCase(),
    runSongsByProducerCase(),
    runProducerColonCase(),
    runHyphenProducedCase(),
    runProducerCommaCase(),
    runProducerPossessiveCase(),
    runRejectGenericProductionsPromptCase(),
    runRejectEraProductionsPromptCase(),
    runEngineeredByEngineerCase(),
    runTracksByEngineerCase(),
    runEngineerColonCase(),
    runArrangedByArrangerCase(),
    runTracksByArrangerCase(),
    runArrangerColonCase(),
    runProducerSwedishProducedByCase(),
    runProducerSwedishWorkAsCase(),
    runEngineerSwedishMixedByCase(),
    runEngineerEnglishMixedByCase(),
    runArrangerSwedishArrangedByCase(),
    runJsonParseRepairCase(),
    runArtistFieldFeaturingSplitCase(),
    runArtistFieldSwedishOchSplitCase(),
    runSongTitleFeaturingExtractCase(),
    runFullArtistNamePreservedCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;

  console.log('[eval:parser] Parser quality harness');
  console.log(`[eval:parser] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:parser] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run();
