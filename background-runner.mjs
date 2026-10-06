import {pathToFileURL} from 'node:url';

// Invoke once per minute from a durable external scheduler. Never run from a browser.
export async function runOnce({origin=process.env.MALKIPULSE_ORIGIN||'https://malkipulse.com',key=process.env.BACKGROUND_RUNNER_KEY,fetchImpl=fetch}={}){
 const url=new URL('/api/background-run',origin);
 if(url.protocol!=='https:')throw Error('The runner requires an HTTPS origin.');
 if(!key||key.length<32)throw Error('Configure BACKGROUND_RUNNER_KEY in the scheduler secret store.');
 const response=await fetchImpl(url,{method:'POST',redirect:'error',headers:{'content-type':'application/json','x-background-runner-key':key},body:'{}',signal:AbortSignal.timeout(55_000)});
 if(!response.ok)throw Error('Background endpoint returned HTTP '+response.status+'.');
 const result=await response.json();
 if(!result.ok||!['success','suppressed'].includes(result.status))throw Error('Background scan did not complete successfully.');
 if(result.telegramError||result.retryBefore?.failed||result.retryAfter?.failed)throw Error('Telegram has failed deliveries; inspect the owner dashboard.');
 return {status:result.status,runId:result.runId||null,candidateCount:result.candidateCount??null,exceptionalCount:result.exceptionalCount??null,newCount:result.newCount??null,telegramSent:Boolean(result.telegramSent),completedAt:result.completedAt||null,pending:result.retryAfter?.pending??0};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{console.log(JSON.stringify(await runOnce()))}
 catch(error){console.error(error.message?.startsWith('Background')||error.message?.startsWith('Configure')||error.message?.startsWith('Telegram')?error.message:'Background runner request failed.');process.exitCode=1}
}
