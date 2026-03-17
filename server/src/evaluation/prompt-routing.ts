import { resolvePromptRoute, type PromptIntent, type RouteMode } from '../services/prompt-routing.js';

interface RoutingCase {
  id: string;
  prompt: string;
  expectedIntent: PromptIntent;
  expectedMode: RouteMode;
  expectedConfidence?: 'high' | 'medium' | 'low';
  expectedCreditRole?: string;
  expectedReasonCode?: string;
  reasonIncludes?: string;
}

interface RoutingCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

const CASES: RoutingCase[] = [
  {
    id: 'routing_credit_producer_truth_first',
    prompt: 'Songs produced by Brian Eno',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'producer',
    expectedReasonCode: 'credit_role_detected',
    reasonIncludes: 'credit role detected',
  },
  {
    id: 'routing_credit_engineer_truth_first',
    prompt: 'Tracks engineered by Alan Parsons',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'engineer',
    expectedReasonCode: 'credit_role_detected',
  },
  {
    id: 'routing_credit_arranger_truth_first',
    prompt: 'Songs arranged by Claus Ogerman',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'arranger',
    expectedReasonCode: 'credit_role_detected',
  },
  {
    id: 'routing_credit_cover_designer_truth_first',
    prompt: 'Songs from albums with sleeve design by Peter Saville',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'cover_designer',
    expectedReasonCode: 'credit_role_detected',
  },
  {
    id: 'routing_credit_art_director_truth_first',
    prompt: 'Albums with art direction by Peter Saville',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'art_director',
    expectedReasonCode: 'credit_role_detected',
  },
  {
    id: 'routing_credit_photographer_truth_first',
    prompt: 'Tracks with photography by Anton Corbijn',
    expectedIntent: 'credit',
    expectedMode: 'truth-first',
    expectedConfidence: 'high',
    expectedCreditRole: 'photographer',
    expectedReasonCode: 'credit_role_detected',
  },
  {
    id: 'routing_studio_hybrid',
    prompt: 'Songs recorded at Abbey Road Studios',
    expectedIntent: 'studio',
    expectedMode: 'hybrid',
    expectedConfidence: 'high',
    expectedReasonCode: 'studio_cue_detected',
  },
  {
    id: 'routing_venue_hybrid',
    prompt: 'Best live recordings from CBGB',
    expectedIntent: 'venue',
    expectedMode: 'hybrid',
    expectedConfidence: 'high',
    expectedReasonCode: 'venue_cue_detected',
  },
  {
    id: 'routing_equipment_hybrid',
    prompt: 'Songs with Mellotron and TR-808 textures',
    expectedIntent: 'equipment',
    expectedMode: 'hybrid',
    expectedConfidence: 'medium',
    expectedReasonCode: 'equipment_cue_detected',
  },
  {
    id: 'routing_discovery_gemini_first',
    prompt: 'Discover underrated 70s psych artists',
    expectedIntent: 'artist-discovery',
    expectedMode: 'gemini-first',
    expectedConfidence: 'medium',
    expectedReasonCode: 'artist_discovery_cue_detected',
  },
  {
    id: 'routing_mood_gemini_first',
    prompt: 'Melancholic rainy-night playlist for late drives',
    expectedIntent: 'abstract-mood',
    expectedMode: 'gemini-first',
    expectedConfidence: 'high',
    expectedReasonCode: 'abstract_mood_cue_detected',
  },
  {
    id: 'routing_unknown_gemini_first',
    prompt: 'asdf qwerty',
    expectedIntent: 'unknown',
    expectedMode: 'gemini-first',
    expectedConfidence: 'low',
    expectedReasonCode: 'no_factual_intent_detected',
    reasonIncludes: 'no factual intent detected',
  },
  {
    id: 'routing_empty_prompt',
    prompt: '   ',
    expectedIntent: 'unknown',
    expectedMode: 'gemini-first',
    expectedConfidence: 'low',
    expectedReasonCode: 'empty_prompt',
    reasonIncludes: 'empty prompt',
  },
];

function runCase(item: RoutingCase): RoutingCaseResult {
  const decision = resolvePromptRoute(item.prompt);
  const intentPass = decision.intent === item.expectedIntent;
  const modePass = decision.mode === item.expectedMode;
  const confidencePass = !item.expectedConfidence || decision.confidence === item.expectedConfidence;
  const reasonCodePass = !item.expectedReasonCode || decision.reasonCode === item.expectedReasonCode;
  const creditRolePass = !item.expectedCreditRole || decision.credit?.role === item.expectedCreditRole;
  const reasonPass = !item.reasonIncludes || decision.reason.includes(item.reasonIncludes);
  const pass = intentPass && modePass && confidencePass && reasonCodePass && creditRolePass && reasonPass;

  return {
    id: item.id,
    pass,
    details: `intent=${decision.intent} mode=${decision.mode} confidence=${decision.confidence} reason_code=${decision.reasonCode} credit_role=${decision.credit?.role || 'n/a'} reason="${decision.reason}"`,
  };
}

function runEnvOverrideCase(): RoutingCaseResult {
  const id = 'routing_env_override_credit_mode';
  const previous = process.env.ROUTE_CREDIT_MODE;
  process.env.ROUTE_CREDIT_MODE = 'gemini-first';
  try {
    const decision = resolvePromptRoute('Songs produced by Brian Eno');
    const pass = decision.intent === 'credit'
      && decision.mode === 'gemini-first'
      && decision.reasonCode === 'credit_role_detected'
      && decision.reason.includes('env override ROUTE_CREDIT_MODE=gemini-first');
    return {
      id,
      pass,
      details: `intent=${decision.intent} mode=${decision.mode} reason="${decision.reason}"`,
    };
  } finally {
    if (typeof previous === 'string') {
      process.env.ROUTE_CREDIT_MODE = previous;
    } else {
      delete process.env.ROUTE_CREDIT_MODE;
    }
  }
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const results = [...CASES.map(runCase), runEnvOverrideCase()];
  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;

  console.log('[eval:routing] Prompt routing harness');
  console.log(`[eval:routing] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:routing] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run();
