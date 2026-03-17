import { creditArtistFoldForEval, creditTitleMatchForEval } from '../services/gemini.js';

interface CreditMatchCaseResult {
  id: string;
  pass: boolean;
  details: string;
}

function runArtistDiacriticFoldCase(): CreditMatchCaseResult {
  const id = 'credit_match_artist_fold_diacritic';
  const left = creditArtistFoldForEval('Antônio Carlos Jobim');
  const right = creditArtistFoldForEval('Antonio Carlos Jobim');
  const pass = left === right;
  return {
    id,
    pass,
    details: `left="${left}" right="${right}"`,
  };
}

function runTitleParentheticalCase(): CreditMatchCaseResult {
  const id = 'credit_match_title_parenthetical_equivalence';
  const left = 'Corcovado (Quiet Nights of Quiet Stars)';
  const right = 'Corcovado';
  const pass = creditTitleMatchForEval(left, right);
  return {
    id,
    pass,
    details: `left="${left}" right="${right}"`,
  };
}

function runTitleQuoteNormalizationCase(): CreditMatchCaseResult {
  const id = 'credit_match_title_quote_normalization';
  const left = "What's So Funny 'Bout Peace, Love and Understanding";
  const right = 'What’s So Funny ’Bout Peace Love and Understanding';
  const pass = creditTitleMatchForEval(left, right);
  return {
    id,
    pass,
    details: `left="${left}" right="${right}"`,
  };
}

function runTitleNegativeCase(): CreditMatchCaseResult {
  const id = 'credit_match_title_negative_distinct_songs';
  const left = 'Wave';
  const right = 'Triste';
  const pass = !creditTitleMatchForEval(left, right);
  return {
    id,
    pass,
    details: `left="${left}" right="${right}"`,
  };
}

function run(): void {
  const strict = process.argv.includes('--strict');
  const results: CreditMatchCaseResult[] = [
    runArtistDiacriticFoldCase(),
    runTitleParentheticalCase(),
    runTitleQuoteNormalizationCase(),
    runTitleNegativeCase(),
  ];

  const passed = results.filter((item) => item.pass).length;
  const failed = results.length - passed;

  console.log('[eval:credit-match] Credit match harness');
  console.log(`[eval:credit-match] Cases: ${results.length}, Passed: ${passed}, Failed: ${failed}`);
  for (const result of results) {
    console.log(`[eval:credit-match] ${result.pass ? 'PASS' : 'FAIL'} ${result.id} -> ${result.details}`);
  }

  if (strict && failed > 0) {
    process.exitCode = 1;
  }
}

run();
