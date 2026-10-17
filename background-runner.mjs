import {pathToFileURL} from 'node:url';


// Invoke once per minute from a durable external scheduler. Never run from a browser.
// Log only known status codes; never echo raw endpoint messages or provider payloads.
const SUPPRESSION_REASONS=new Set(['MARKET_DATA_PAUSED','ENGINE_DISABLED','CYCLE_ALREADY_RUNNING','LEASE_LOST','FEED_UNAVAILABLE','FEED_NOT_CONFIGURED','FEED_DATABASE_UNAVAILABLE','PROVIDER_INACTIVE','FEED_MISSING','FEED_PROVIDER_MISMATCH','FEED_TIMESTAMP_INVALID','FEED_TIMESTAMP_FUTURE','FEED_STALE','FEED_PAYLOAD_INVALID','NO_FRESH_QUOTES']);
export async function runOnce({origin=process.env.MALKIPULSE_ORIGIN||'https://malkipulse.com',key=process.env.BACKGROUND_RUNNER_KEY,fetchImpl=fetch}={}){
 const url=new URL('/api/background-run',origin);
 if(url.protocol!=='https:')throw Error('The runner requires an HTTPS origin.');
 if(!key||key.length<32)throw Error('Configure BACKGROUND_RUNNER_KEY in the scheduler secret store.');
 const response=await fetchImpl(url,{method:'POST',redirect:'error',headers:{'content-type':'application/json','x-background-runner-key':key},body:'{}',signal:AbortSignal.timeout(55_000)});
if (!result.ok || !['success', 'completed', 'suppressed'].includes(result.status)) {
  throw Error('Background scan did not complete successfully.');
}
 const result=await response.json();
 if(!result.ok||!['success','completed','suppressed'].includes(result.status)throw Error('Background scan did not complete successfully.');
 if(result.telegramError||result.retryBefore?.failed||result.retryAfter?.failed)throw Error('Telegram has failed deliveries; inspect the owner dashboard.');
 const code=result.suppressionReason||result.feedDiagnostics?.suppressionReason;
 const suppressionReason=result.status==='suppressed'?(SUPPRESSION_REASONS.has(code)?code:'SUPPRESSION_UNSPECIFIED'):null;
 return {status:result.status,scanCompleted:['success','completed'].includes(result.status)',
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{console.log(JSON.stringify(await runOnce()))}
 catch(error){console.error(error.message?.startsWith('Background')||error.message?.startsWith('Configure')||error.message?.startsWith('Telegram')?error.message:'Background runner request failed.');process.exitCode=1}
}

