import {pathToFileURL} from 'node:url';

const SUPPRESSION_REASONS = new Set([
  'MARKET_DATA_PAUSED', 'ENGINE_DISABLED', 'CYCLE_ALREADY_RUNNING',
  'LEASE_LOST', 'FEED_UNAVAILABLE', 'FEED_NOT_CONFIGURED',
  'FEED_DATABASE_UNAVAILABLE', 'PROVIDER_INACTIVE', 'FEED_MISSING',
  'FEED_PROVIDER_MISMATCH', 'FEED_TIMESTAMP_INVALID',
  'FEED_TIMESTAMP_FUTURE', 'FEED_STALE', 'FEED_PAYLOAD_INVALID',
  'NO_FRESH_QUOTES'
]);

export async function runOnce({
  origin = process.env.MALKIPULSE_ORIGIN || 'https://malkipulse.com',
  key = process.env.BACKGROUND_RUNNER_KEY,
  fetchImpl = fetch
} = {}) {
  const url = new URL('/api/background-run', origin);
  if (url.protocol !== 'https:') throw Error('The runner requires an HTTPS origin.');
  if (!key || key.length < 32) throw Error('Configure BACKGROUND_RUNNER_KEY in the scheduler secret store.');

  const response = await fetchImpl(url, {
    method: 'POST',
    redirect: 'error',
    headers: {'content-type': 'application/json', 'x-background-runner-key': key},
    body: '{}',
    signal: AbortSignal.timeout(55_000)
  });
  if (!response.ok) throw Error('Background endpoint returned HTTP ' + response.status + '.');

  const result = await response.json();
  if (!result.ok || !['success', 'completed', 'suppressed'].includes(result.status)) {
    throw Error('Background scan did not complete successfully.');
  }
  if (result.telegramError || result.retryBefore?.failed || result.retryAfter?.failed) {
    throw Error('Telegram has failed deliveries; inspect the owner dashboard.');
  }

  const code = result.suppressionReason || result.feedDiagnostics?.suppressionReason;
  const suppressionReason = result.status === 'suppressed'
    ? (SUPPRESSION_REASONS.has(code) ? code : 'SUPPRESSION_UNSPECIFIED')
    : null;

  return {
    status: result.status,
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

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    console.log(JSON.stringify(await runOnce()));
  } catch (error) {
    const safe = ['Background', 'Configure', 'Telegram'].some(prefix => error.message?.startsWith(prefix));
    console.error(safe ? error.message : 'Background runner request failed.');
    process.exitCode = 1;
  }
}
