const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, "trade-data.json");
const BINANCE_REST = "https://api.binance.com/api/v3/klines";
const BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m";

const CONFIG = {
  startingBalance: 1000,       // USDT fictícios
  positionFraction: 0.10,      // usa 10% do saldo por entrada
  stopPct: 0.01,               // stop de 1%
  targetPct: 0.015,            // alvo de 1,5%
  feePct: 0.001,               // taxa simulada de 0,1% por lado
  minVolumeRatio: 1.20
};

let state = loadState();
let candles5 = [];
let candles15 = [];
let latestPrice = null;
let lastSignal = { decision: "CARREGANDO", score: 0, reasons: [] };
let socket;
let reconnectTimer;

function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    return s && typeof s.balance === "number" ? s : freshState();
  } catch { return freshState(); }
}
function freshState() {
  return { balance: CONFIG.startingBalance, initialBalance: CONFIG.startingBalance,
    position: null, trades: [], updatedAt: new Date().toISOString() };
}
function saveState() {
  state.updatedAt = new Date().toISOString();
  fs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2));
}
function ema(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  let v = values.slice(0, period).reduce((a,b)=>a+b,0) / period;
  for (let i=period; i<values.length; i++) v = values[i]*k + v*(1-k);
  return v;
}
function rsi(values, period=14) {
  if (values.length <= period) return null;
  let gains=0, losses=0;
  for(let i=values.length-period;i<values.length;i++){
    const d=values[i]-values[i-1];
    if(d>0) gains+=d; else losses-=d;
  }
  if(losses===0) return 100;
  const rs=(gains/period)/(losses/period);
  return 100-(100/(1+rs));
}
function macdHistogram(values) {
  const fast=emaSeries(values,12), slow=emaSeries(values,26);
  const line=fast.map((v,i)=>v===null||slow[i]===null?null:v-slow[i]).filter(v=>v!==null);
  if(line.length<9) return null;
  const sig=ema(line,9);
  return line[line.length-1]-sig;
}
function emaSeries(values, p) {
  const out=Array(values.length).fill(null);
  if(values.length<p) return out;
  const k=2/(p+1);
  let v=values.slice(0,p).reduce((a,b)=>a+b,0)/p;
  out[p-1]=v;
  for(let i=p;i<values.length;i++){v=values[i]*k+v*(1-k);out[i]=v;}
  return out;
}
function analyze() {
  if(candles5.length<60 || candles15.length<60) return lastSignal;
  const c5=candles5, c15=candles15;
  const closes5=c5.map(c=>c.close), closes15=c15.map(c=>c.close);
  const e9=ema(closes5,9), e21=ema(closes5,21);
  const e20_15=ema(closes15,20), e50_15=ema(closes15,50);
  const rv=rsi(closes5,14), mh=macdHistogram(closes5);
  const vols=c5.slice(-21,-1).map(c=>c.volume);
  const avgVol=vols.reduce((a,b)=>a+b,0)/(vols.length||1);
  const volRatio=avgVol ? c5[c5.length-1].volume/avgVol : 0;
  const last=c5[c5.length-1], prev=c5[c5.length-2];
  const trend=e20_15>e50_15;
  const emaBull=e9>e21;
  const rsiOk=rv>=50 && rv<=65;
  const macdOk=mh>0;
  const volumeOk=volRatio>=CONFIG.minVolumeRatio;
  const breakout=last.close>prev.high;
  const checks=[trend,emaBull,rsiOk,macdOk,volumeOk,breakout];
  const score=Math.round(checks.filter(Boolean).length/6*100);
  const reasons=[
    `Tendência 15m: ${trend?"favorável":"não favorável"}`,
    `EMA9 acima da EMA21: ${emaBull?"sim":"não"}`,
    `RSI ${rv.toFixed(1)}: ${rsiOk?"dentro da faixa 50–65":"fora da faixa 50–65"}`,
    `MACD: ${macdOk?"positivo":"não positivo"}`,
    `Volume relativo ${volRatio.toFixed(2)}x: ${volumeOk?"suficiente":"baixo"}`,
    `Rompimento da máxima anterior: ${breakout?"confirmado":"não confirmado"}`
  ];
  lastSignal={decision:checks.every(Boolean)?"COMPRA SIMULADA":"AGUARDAR",
    score, reasons, indicators:{rsi:+rv.toFixed(2),ema9:+e9.toFixed(2),ema21:+e21.toFixed(2),
    macd:+mh.toFixed(4),volumeRatio:+volRatio.toFixed(2),trend:trend?"Alta":"Baixa"},
    candleTime:last.closeTime};
  return lastSignal;
}
function closePosition(price, reason, time) {
  const p=state.position;
  if(!p) return;
  const exitValue=p.quantity*price;
  const exitFee=exitValue*CONFIG.feePct;
  const gross=(price-p.entryPrice)*p.quantity;
  const net=gross-p.entryFee-exitFee;
  state.balance += exitValue-exitFee;
  state.trades.unshift({openedAt:p.openedAt,closedAt:time||new Date().toISOString(),
    entryPrice:p.entryPrice,exitPrice:price,quantity:p.quantity,
    result:net,reason,won:net>0});
  state.position=null;
  saveState();
}
function openPosition(price, time) {
  if(state.position || state.balance<=0) return;
  const allocation=state.balance*CONFIG.positionFraction;
  const entryFee=allocation*CONFIG.feePct;
  const quantity=(allocation-entryFee)/price;
  const cost=quantity*price+entryFee;
  if(cost>state.balance) return;
  state.balance-=cost;
  state.position={openedAt:time||new Date().toISOString(),entryPrice:price,
    quantity,entryFee,stopPrice:price*(1-CONFIG.stopPct),targetPrice:price*(1+CONFIG.targetPct)};
  saveState();
}
function processClosedCandle(c) {
  latestPrice=c.close;
  if(state.position) {
    // Se stop e alvo forem tocados no mesmo candle, assume stop primeiro (cenário conservador).
    if(c.low<=state.position.stopPrice) closePosition(state.position.stopPrice,"STOP",new Date(c.closeTime).toISOString());
    else if(c.high>=state.position.targetPrice) closePosition(state.position.targetPrice,"ALVO",new Date(c.closeTime).toISOString());
  }
  analyze();
  if(!state.position && lastSignal.decision==="COMPRA SIMULADA")
    openPosition(c.close,new Date(c.closeTime).toISOString());
}
function parseKline(k) {
  return {openTime:k[0],open:+k[1],high:+k[2],low:+k[3],close:+k[4],volume:+k[5],closeTime:k[6]};
}
async function loadCandles(interval, limit=200) {
  const url=`${BINANCE_REST}?symbol=BTCUSDT&interval=${interval}&limit=${limit}`;
  const r=await fetch(url);
  if(!r.ok) throw new Error(`Binance HTTP ${r.status}`);
  const rows=await r.json();
  return rows.slice(0,-1).map(parseKline); // somente candles fechados
}
async function initialize() {
  try {
    candles5=await loadCandles("5m");
    candles15=await loadCandles("15m");
    latestPrice=candles5.at(-1)?.close ?? null;
    analyze();
    connectSocket();
  } catch(e) {
    console.error("Falha ao carregar candles:",e.message);
    setTimeout(initialize,10000);
  }
}
function connectSocket() {
  clearTimeout(reconnectTimer);
  try {
    socket=new WebSocket(BINANCE_WS);
    socket.on("open",()=>console.log("Binance WebSocket conectado"));
    socket.on("message",async raw=>{
      try {
        const msg=JSON.parse(raw.toString());
        const k=msg.k;
        latestPrice=+k.c;
        if(k.x) {
          const c=parseKline([k.t,k.o,k.h,k.l,k.c,k.v,k.T]);
          candles5.push(c); if(candles5.length>250) candles5.shift();
          processClosedCandle(c);
          // atualiza candles de 15m após fechamento de candle 5m
          if((k.T+1)%(15*60*1000)<5*60*1000) {
            try { candles15=await loadCandles("15m"); } catch(e) { console.error(e.message); }
          }
        }
      } catch(e) { console.error("Mensagem inválida:",e.message); }
    });
    socket.on("close",()=>{console.log("WebSocket desconectado; reconectando..."); reconnectTimer=setTimeout(connectSocket,5000);});
    socket.on("error",e=>console.error("WebSocket:",e.message));
  } catch(e) { reconnectTimer=setTimeout(connectSocket,5000); }
}
function json(res,obj,status=200){res.writeHead(status,{"Content-Type":"application/json","Access-Control-Allow-Origin":"*","Cache-Control":"no-store"});res.end(JSON.stringify(obj));}
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,"http://localhost");
  if(url.pathname==="/api/state"){
    return json(res,{connected:socket?.readyState===WebSocket.OPEN,price:latestPrice,
      signal:lastSignal,config:CONFIG,state});
  }
  if(url.pathname==="/api/reset" && req.method==="POST"){
    state=freshState();saveState();return json(res,{ok:true,state});
  }
  let file=url.pathname==="/" ? "/index.html" : url.pathname;
  const full=path.join(__dirname,"public",path.normalize(file).replace(/^(\.\.[/\\])+/, ""));
  fs.readFile(full,(err,data)=>{
    if(err){res.writeHead(404);return res.end("Not found");}
    const ext=path.extname(full);
    res.writeHead(200,{"Content-Type":ext===".html"?"text/html; charset=utf-8":"application/octet-stream"});
    res.end(data);
  });
});
server.listen(PORT,()=>{console.log(`Trade IA em http://localhost:${PORT}`);initialize();});
