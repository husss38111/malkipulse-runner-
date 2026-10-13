import {pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';

export function stompFrame(command,headers={},body=''){
 if(body)headers={...headers,...(command==='SEND'?{}:{'content-type':'application/json'}),'content-length':Buffer.byteLength(body)};
 if(command==='SEND'){headers={...headers};for(const name of Object.keys(headers))if(name.toLowerCase()==='content-type')delete headers[name]}
 return command+'\n'+Object.entries(headers).map(([k,v])=>k+':'+v).join('\n')+'\n\n'+body+'\0';
}
export class StompParser{
 buffer=Buffer.alloc(0);
 push(value){this.buffer=Buffer.concat([this.buffer,Buffer.from(value)]);if(this.buffer.length>1_000_000)throw Error('protocol');const out=[];
  while(this.buffer.length){while(this.buffer[0]===10||this.buffer[0]===13)this.buffer=this.buffer.subarray(1);if(!this.buffer.length)break;let h=this.buffer.indexOf('\n\n'),sep=2;if(h<0){h=this.buffer.indexOf('\r\n\r\n');sep=4}if(h<0)break;
   const lines=this.buffer.subarray(0,h).toString().split(/\r?\n/),command=lines.shift(),headers=Object.fromEntries(lines.map(x=>{const n=x.indexOf(':');return [x.slice(0,n),x.slice(n+1)]}));
   const len=headers['content-length'];let end;if(len!==undefined){if(!/^\d+$/.test(len)||Number(len)>1_000_000)throw Error('protocol');end=h+sep+Number(len);if(this.buffer.length<=end)break;if(this.buffer[end]!==0)throw Error('protocol')}else{end=this.buffer.indexOf(0,h+sep);if(end<0)break}
   out.push({command,headers,body:this.buffer.subarray(h+sep,end).toString()});this.buffer=this.buffer.subarray(end+1);
  }return out;
 }
}
export function sampleData(m,now=Date.now()){
 const keys=Object.keys(m),lower=Object.fromEntries(keys.map(k=>[k.replace(/[^a-z0-9]/ig,'').toLowerCase(),m[k]]));
 const number=(...ks)=>{for(const k of ks){const v=lower[k];if(v!==undefined&&v!==null&&v!==''&&Number.isFinite(Number(v)))return Number(v)}return null};
 const rawTime=lower.lasttradetime??lower.timestamp??lower.time;let providerTime=null;if(rawTime!==undefined){const n=Number(rawTime);if(Number.isFinite(n)&&n>1e9)providerTime=n<1e11?n*1000:n;else{const p=Date.parse(String(rawTime));if(Number.isFinite(p))providerTime=p}}
 return {type:m['@T'],receivedAt:now,symbol:lower.symbol??null,providerTime,price:number('last','price'),volume:number('accumulatedvolume','volume'),vwap:number('vwap'),fields:keys};
}
const delay=ms=>new Promise(r=>setTimeout(r,ms));
export async function runStreaming({origin=process.env.MALKIPULSE_ORIGIN||'https://malkipulse.com',key=process.env.QM_STREAM_RUNNER_KEY,fetchImpl=fetch,WebSocketImpl=WebSocket,testMs=0,log=console.log}={}){
 const base=new URL(origin);if(base.protocol!=='https:'||!key||key.length<32)throw Error('Configure HTTPS origin and QM_STREAM_RUNNER_KEY.');
 const runnerId=randomUUID(),state={authCode:null,connected:false,startedAt:Date.now(),lastMessageAt:null,lastDataAt:null,messageCount:0,dataCount:0,reconnectCount:0,heartbeatCount:0,flowControlCount:0,types:{},entitlements:[],rejectedSymbols:[],samples:[],error:null};
 let stopped=false,socket=null,lease=false,ingesting=false,ingestFailures=0,testTimer;
 const post=async(path,data)=>{const r=await fetchImpl(new URL(path,base),{method:'POST',redirect:'error',headers:{'content-type':'application/json','x-qm-stream-key':key},body:JSON.stringify({runnerId,...data}),signal:AbortSignal.timeout(12000)});if(!r.ok)throw Error(r.status===409?'lease':'ingest');return r.json()};
 const ingest=async()=>{if(!lease||ingesting)return;ingesting=true;try{await post('/api/quotemedia/runner/ingest',{state});ingestFailures=0}catch(e){state.error='ingest';if(e.message==='lease'||++ingestFailures>=2){lease=false;socket?.close()}}finally{ingesting=false}};
 const stop=()=>{stopped=true;state.connected=false;state.error='stopped';socket?.close()};process.on('SIGTERM',stop);process.on('SIGINT',stop);if(testMs)testTimer=setTimeout(stop,testMs);
 const interval=setInterval(()=>{if(socket?.readyState===1)socket.send('\n');if(state.connected&&Date.now()-state.lastMessageAt>120000){state.error='stale';socket?.close()}void ingest()},10000);
 try{while(!stopped){
  try{const config=await post('/api/quotemedia/runner/bootstrap',{});lease=true;state.error=null;state.entitlements=[];state.rejectedSymbols=[];
   // Token remains in server memory; never printed or saved in source/logs.
   await new Promise(resolve=>{let finished=false,authenticated=false;const parser=new StompParser();socket=new WebSocketImpl(config.endpoint,['v12.stomp','v11.stomp','v10.stomp']);
    const timer=setTimeout(()=>{if(!authenticated){state.error='authentication';socket.close()}},20000);
    const done=()=>{if(finished)return;finished=true;clearTimeout(timer);state.connected=false;resolve()};
    const send=m=>socket.send(stompFrame('SEND',{destination:'/stream/message'},JSON.stringify(m)));
    socket.addEventListener('open',()=>socket.send(stompFrame('CONNECT',{'accept-version':'1.2,1.1,1.0',wmid:config.wmid,'heart-beat':'10000,10000'})));
    socket.addEventListener('error',()=>{state.error='transport';done();socket.close()});socket.addEventListener('close',done);
    socket.addEventListener('message',event=>{try{if(typeof event.data!=='string')throw Error('protocol');state.lastMessageAt=Date.now();for(const frame of parser.push(event.data)){
     if(frame.command==='CONNECTED'){socket.send(stompFrame('SUBSCRIBE',{id:'0',destination:'/user/queue/messages',ack:'auto'}));send({'@T':'C27',authenticationMethod:'enterprise',authorization:config.token.startsWith('Bearer ')?config.token:'Bearer '+config.token,wmid:String(config.wmid),conflation:150,rejectExcessiveConnection:true});continue}
     if(frame.command==='ERROR'){state.error='protocol';socket.close();continue}if(frame.command!=='MESSAGE')continue;
     const m=JSON.parse(frame.body);state.messageCount++;const type=m['@T'];
     if(type==='C28'){state.flowControlCount++;send(m);continue}if(type==='C1'){state.heartbeatCount++;continue}
     if(type==='C5'){state.authCode=m.code;if(m.code!==200){state.error='authentication';socket.close();continue}authenticated=true;clearTimeout(timer);state.connected=true;const size=Math.max(1,Math.min(50,Math.floor((m.maxEntitlementsPerSubscription||400)/config.types.length)));for(let i=0;i<config.symbols.length;i+=size)send({'@T':'C2',action:'SUBSCRIBE',symbols:config.symbols.slice(i,i+size),types:config.types,mimetype:'application/json',conflation:150,id:i+1});send({'@T':'C12'});void ingest();log(JSON.stringify({event:'authenticated',environment:config.environment,symbolCount:config.symbols.length}));continue}
     if(type==='C3'){state.entitlements.push(...(m.entitlements||[]));state.rejectedSymbols.push(...(m.rejectedSymbols||[]),...(m.invalidSymbols||[]));if(m.code!==200)state.error='subscription';log(JSON.stringify({event:'subscription',code:m.code,accepted:(m.entitlements||[]).length,rejected:(m.rejectedSymbols||[]).length}));continue}
     if(type==='C6'){state.authCode=m.code;state.error=m.code===401?'authentication':'transport';log(JSON.stringify({event:'connection-closed',code:m.code}));socket.close();continue}if(/^D\d+$/.test(type)){state.dataCount++;state.types[type]=(state.types[type]||0)+1;state.lastDataAt=Date.now();state.samples.push(sampleData(m));state.samples=state.samples.slice(-12)}
    }}catch{state.error='protocol';socket.close()}});
   });await ingest();state.reconnectCount++;
  }catch{state.error='ingest';log(JSON.stringify({event:'connection-unavailable'}))}
  if(!stopped)await delay(Math.min(30000,1000*2**Math.min(state.reconnectCount,5)));
 }}finally{clearInterval(interval);clearTimeout(testTimer);state.connected=false;state.error='stopped';await ingest();socket?.close();process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop)}
 log(JSON.stringify({event:'stopped',messageCount:state.messageCount,dataCount:state.dataCount,types:state.types}));return state;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)runStreaming().catch(()=>{console.error('Stream runner failed. Check private owner status.');process.exitCode=1});
