import {pathToFileURL} from 'node:url';

export const REQUEST_TIMEOUT_MS = 90_000;
const ERROR_MESSAGES = Object.freeze({
  CONFIGURATION: 'Configure a valid HTTPS origin and scheduler secret.',
  REQUEST_TIMEOUT: 'Background request exceeded its time budget.',
  REQUEST_ABORTED: 'Background request was aborted.',
  NETWORK_ERROR: 'Background endpoint could not be reached.',
  HTTP_ERROR: 'Background endpoint returned an unsuccessful HTTP status.',
  INVALID_JSON: 'Background endpoint returned invalid JSON.',
  INVALID_RESULT: 'Background scan returned an invalid or unsuccessful result.',
  TELEGRAM_FAILURE: 'Telegram has failed deliveries; inspect the owner dashboard.',
  UNEXPECTED_ERROR: 'Background runner failed unexpectedly.'
});

export class RunnerError extends Error {
  constructor(code, {stage = 'unknown', httpStatus = null, elapsedMs = null} = {}) {
    super(ERROR_MESSAGES[code] || ERROR_MESSAGES.UNEXPECTED_ERROR);
    this.name = 'RunnerError';
    this.code = Object.hasOwn(ERROR_MESSAGES, code) ? code : 'UNEXPECTED_ERROR';
    this.stage = stage;
    this.httpStatus = httpStatus;
    this.elapsedMs = elapsedMs;
  }
}

// Never log raw exception messages, URLs, headers or response bodies: they can
// contain credentials or untrusted upstream text. Do not mistake this process's
// failure for proof that the server did not finish after a request timeout.
export function safeFailure(error) {
  const known = error instanceof RunnerError;
  return {
    status: 'error', technicalSuccess: false, scanCompleted: false,
    outcome: 'unknown', code: known ? error.code : 'UNEXPECTED_ERROR',
    message: known ? ERROR_MESSAGES[error.code] : ERROR_MESSAGES.UNEXPECTED_ERROR,
    stage: known ? error.stage : 'unknown',
    httpStatus: known ? error.httpStatus : null,
    elapsedMs: known ? error.elapsedMs : null
  };
}

function requestError(error, signal, details) {
  const name = signal?.aborted ? signal.reason?.name : error?.name;
  const code = name === 'TimeoutError' ? 'REQUEST_TIMEOUT'
    : name === 'AbortError' ? 'REQUEST_ABORTED' : 'NETWORK_ERROR';
  return new RunnerError(code, details);
}

function validCount(value) {
  return value == null || (Number.isSafeInteger(value) && value >= 0);
}

function validTimestamp(value) {
  if (value == null) return true;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) return false;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value;
}

function validSummary(result) {
  // Match the server's observed run-<epoch milliseconds>-<8 hex> IDs. Any
  // future API contract change must be reviewed, never copied into logs.
  return (result.runId == null || (typeof result.runId === 'string' && /^run-\d{13}-[a-f0-9]{8}$/.test(result.runId)))
    && ['candidateCount', 'exceptionalCount', 'newCount'].every(field => validCount(result[field]))
    && validCount(result.retryAfter?.pending)
    && validCount(result.retryAfter?.sent)
    && validTimestamp(result.completedAt)
    && (result.telegramSent == null || typeof result.telegramSent === 'boolean');
}

const SUPPRESSION_REASONS = new Set([
  'MARKET_DATA_PAUSED', 'ENGINE_DISABLED', 'CYCLE_ALREADY_RUNNING',
  'LEASE_LOST', 'FEED_UNAVAILABLE', 'FEED_NOT_CONFIGURED',
  'FEED_DATABASE_UNAVAILABLE', 'PROVIDER_INACTIVE', 'FEED_MISSING',
  'FEED_PROVIDER_MISMATCH', 'FEED_TIMESTAMP_INVALID',
  'FEED_TIMESTAMP_FUTURE', 'FEED_STALE', 'FEED_PAYLOAD_INVALID',
  'NO_FRESH_QUOTES', 'SECONDARY_WORK_DEFERRED'
]);

export async function runOnce({
  origin = process.env.MALKIPULSE_ORIGIN || 'https://malkipulse.com',
  key = process.env.BACKGROUND_RUNNER_KEY,
  fetchImpl = fetch,
  work = null
} = {}) {
  let url;
  try { url = new URL('/api/background-run', origin); }
  catch { throw new RunnerError('CONFIGURATION', {stage: 'configuration'}); }
  if (url.protocol !== 'https:' || url.username || url.password || typeof key !== 'string' || key.length < 32) {
    throw new RunnerError('CONFIGURATION', {stage: 'configuration'});
  }

  if (work && !['scan', 'maintenance', 'delivery'].includes(work)) throw new RunnerError('CONFIGURATION', {stage: 'configuration'});
  if (work) url.searchParams.set('work', work);

  const started = performance.now();
  const elapsedMs = () => Math.max(0, Math.round(performance.now() - started));
  const signal = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
  let response;
  try {

  response = await fetchImpl(url, {
    method: 'POST',
    redirect: 'error',
    headers: {'content-type': 'application/json', 'x-background-runner-key': key},
    body: '{}',
    signal
  });
  } catch (error) {
    throw requestError(error, signal, {stage: 'request', elapsedMs: elapsedMs()});
  }
  if (!response.ok) throw new RunnerError('HTTP_ERROR', {
    stage: 'response', httpStatus: response.status, elapsedMs: elapsedMs()
  });

  let result;
  try { result = await response.json(); }
  catch (error) {
    const details = {stage: 'body', httpStatus: response.status, elapsedMs: elapsedMs()};
    if (error?.name === 'SyntaxError' && !signal.aborted) throw new RunnerError('INVALID_JSON', details);
    throw requestError(error, signal, details);
  }
  if (!result || typeof result !== 'object' || Array.isArray(result) || result.ok !== true || !['success', 'completed', 'suppressed'].includes(result.status) || !validSummary(result)) {
    throw new RunnerError('INVALID_RESULT', {stage: 'validation', httpStatus: response.status, elapsedMs: elapsedMs()});
  }
  if (result.telegramError || result.retryBefore?.failed || result.retryAfter?.failed) {
    throw new RunnerError('TELEGRAM_FAILURE', {stage: 'validation', httpStatus: response.status, elapsedMs: elapsedMs()});
  }

  if ((work === 'maintenance' && result.maintenanceOnly !== true) || (work === 'delivery' && result.deliveryOnly !== true)) {
    throw new RunnerError('INVALID_RESULT', {stage: 'validation', elapsedMs: elapsedMs()});
  }
  if (work === 'maintenance' || work === 'delivery') {
    const states = new Set(['active','partial','source-error','cooldown','running','refreshing','disabled','unavailable','waiting','stale']);
    return {technicalSuccess: true, scanCompleted: false, elapsedMs: elapsedMs(),
      ...(work === 'maintenance' ? {
        secState: states.has(result.secFeed?.state) ? result.secFeed.state : 'unknown',
        acquisitionState: states.has(result.acquisitions?.state) ? result.acquisitions.state : 'unknown',
        monitorAvailable: result.maintenance?.available === true
      } : {sent: result.retryAfter?.sent ?? 0, pending: result.retryAfter?.pending ?? 0})};
  }
  const code = result.suppressionReason || result.feedDiagnostics?.suppressionReason;
  const suppressionReason = result.status === 'suppressed'
    ? (SUPPRESSION_REASONS.has(code) ? code : 'SUPPRESSION_UNSPECIFIED')
    : null;

  return {
    status: result.status,
    technicalSuccess: true,
    elapsedMs: elapsedMs(),
    scanCompleted: ['success', 'completed'].includes(result.status),
    suppressionReason,
    runId: result.runId || null,
    candidateCount: result.candidateCount ?? null,
    exceptionalCount: result.exceptionalCount ?? null,
    newCount: result.newCount ?? null,
    telegramSent: Boolean(result.telegramSent),
    completedAt: result.completedAt || null,
    pending: result.retryAfter?.pending ?? 0
  };
}

// All lanes start together and every started request is awaited, including on
// failure. Never retry an ambiguous timed-out POST or report it as completion.
export async function runScheduledCycle(options = {}) {
  const work = ['scan', 'maintenance', 'delivery'];
  const results = await Promise.allSettled(work.map(phase => runOnce({...options, work: phase})));
  const phases = Object.fromEntries(results.map((result, index) => [work[index],
    result.status === 'fulfilled' ? result.value : safeFailure(result.reason)]));
  return {status: results.every(result => result.status === 'fulfilled') ? 'completed' : 'failed',
    technicalSuccess: results.every(result => result.status === 'fulfilled'), ...phases};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const result = await runScheduledCycle();
    console.log(JSON.stringify(result));
    if (!result.technicalSuccess) process.exitCode = 1;
  } catch (error) {
    console.error(JSON.stringify(safeFailure(error)));
    process.exitCode = 1;
  }
}
