import test from 'node:test';
import assert from 'node:assert/strict';
import {sampleData, StompParser, stompFrame} from '../quotemedia-stream-runner.mjs';
import {runScheduledCycle} from '../background-runner.mjs';

test('stream samples reject non-numeric values after STOMP/JSON decoding', () => {
  for (const value of [' ', '\t\n', true, false, [], [42], {}, null, '', 'NaN', 'Infinity', '0x10', '0b11']) {
    const message = {'@T': 'D1', symbol: 'TEST', last: value, volume: value, vwap: value};
    const [frame] = new StompParser().push(stompFrame('MESSAGE', {}, JSON.stringify(message)));
    const sample = sampleData(JSON.parse(frame.body), 123);
    assert.equal(sample.price, null, `price: ${JSON.stringify(value)}`);
    assert.equal(sample.volume, null, `volume: ${JSON.stringify(value)}`);
    assert.equal(sample.vwap, null, `vwap: ${JSON.stringify(value)}`);
  }
});

test('numeric quotes, zero volume and valid fallback fields remain usable', () => {
  const sample = sampleData({'@T': 'D1', symbol: 'TEST', last: ' 12.50 ', accumulatedVolume: 0, vwap: 12.4}, 123);
  assert.equal(sample.price, 12.5);
  assert.equal(sample.volume, 0);
  assert.equal(sample.vwap, 12.4);
  assert.equal(sample.receivedAt, 123);
  const fallback = sampleData({last: ' ', price: '2.5', accumulatedVolume: true, volume: '120'});
  assert.equal(fallback.price, 2.5);
  assert.equal(fallback.volume, 120);
  for (const value of [NaN, Infinity, -Infinity]) {
    assert.equal(sampleData({last: value}).price, null);
  }
});

const run = (scan, overrides = {}) => {
  const called = [];
  return runScheduledCycle({
    origin: 'https://example.test', key: 'x'.repeat(32),
    fetchImpl: async url => {
      const work = url.searchParams.get('work');
      called.push(work);
      const result = overrides[work] ?? {
        scan,
        maintenance: {ok: true, status: 'success', maintenanceOnly: true},
        provider: {ok: true, status: 'success', providerOnly: true, received: 0}
      }[work];
      return {ok: true, status: 200, json: async () => result};
    }
  }).then(result => {
    assert.deepEqual(called.sort(), ['maintenance', 'provider', 'scan']);
    return result;
  });
};

test('missing feed cannot produce a completed cycle despite successful requests', async () => {
  const result = await run({ok: true, status: 'suppressed', suppressionReason: 'FEED_NOT_CONFIGURED'});
  assert.equal(result.status, 'suppressed');
  assert.equal(result.technicalSuccess, true);
  assert.equal(result.scanCompleted, false);
  assert.equal(result.scan.scanCompleted, false);
});

test('completed scans remain completed, including valid scans with no candidates', async () => {
  for (const status of ['success', 'completed']) {
    const result = await run({ok: true, status, candidateCount: 0});
    assert.equal(result.status, 'completed');
    assert.equal(result.technicalSuccess, true);
    assert.equal(result.scanCompleted, true);
    assert.equal(result.scan.scanCompleted, true);
  }
});

test('technical failures retain failed status and truthful scan completion', async () => {
  const failedScan = await run({ok: false, status: 'failed'});
  assert.equal(failedScan.status, 'failed');
  assert.equal(failedScan.technicalSuccess, false);
  assert.equal(failedScan.scanCompleted, false);
  const failedProvider = await run({ok: true, status: 'completed'}, {provider: {ok: false}});
  assert.equal(failedProvider.status, 'failed');
  assert.equal(failedProvider.technicalSuccess, false);
  assert.equal(failedProvider.scanCompleted, true);
});
