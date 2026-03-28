<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<title>Chat Guard</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root {
  --bg0: #0d0f14; --bg1: #111318; --bg2: #16181f; --bg3: #1c1f28; --bg4: #222630;
  --br: rgba(255,255,255,.07); --br2: rgba(255,255,255,.12);
  --t1: #e8eaf0; --t2: #7c8299; --t3: #3d4259;
  --acc: #5865f2; --acc2: #4752c4; --acc-g: rgba(88,101,242,.15);
  --green: #23a55a; --green-g: rgba(35,165,90,.15);
  --red: #f23f42; --red-g: rgba(242,63,66,.15);
  --ylw: #f0b132; --ylw-g: rgba(240,177,50,.15);
  --r: 10px; --sbot: env(safe-area-inset-bottom, 16px);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent;}
html,body{height:100%;background:var(--bg0);color:var(--t1);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;font-size:15px;overflow:hidden;}
#app{display:flex;flex-direction:column;height:100vh;height:100dvh;}
#cnt{flex:1;overflow-y:auto;overflow-x:hidden;-webkit-overflow-scrolling:touch;padding-bottom:calc(64px + var(--sbot));}
#cnt::-webkit-scrollbar{width:3px;}#cnt::-webkit-scrollbar-track{background:transparent;}#cnt::-webkit-scrollbar-thumb{background:var(--bg4);border-radius:10px;}
.hdr{display:flex;align-items:center;gap:10px;padding:16px 16px 12px;border-bottom:1px solid var(--br);}
.hdr-logo{width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:900;color:#fff;flex-shrink:0;background:var(--acc);}
.hdr-title{font-size:15px;font-weight:700;flex:1;letter-spacing:.3px;}
.hdr-badge{font-size:11px;font-weight:600;padding:3px 9px;border-radius:20px;background:var(--green-g);color:var(--green);border:1px solid rgba(35,165,90,.2);}
#tabs{position:fixed;bottom:0;left:0;right:0;background:var(--bg1);border-top:1px solid var(--br);display:flex;justify-content:space-around;align-items:center;padding:6px 0 calc(6px + var(--sbot));z-index:100;}
.tab{display:flex;flex-direction:column;align-items:center;gap:3px;color:var(--t3);font-size:10px;font-weight:600;padding:5px 16px;border-radius:8px;cursor:pointer;transition:color .15s;border:none;background:none;letter-spacing:.3px;}
.tab.active{color:var(--acc);}
.ti{font-size:20px;line-height:1;}
.page{display:none;padding:16px;}.page.active{display:block;}
#ptr{height:0;overflow:hidden;display:flex;align-items:center;justify-content:center;color:var(--t2);font-size:12px;transition:height .2s;gap:6px;}#ptr.vis{height:40px;}
.sec{background:var(--bg2);border:1px solid var(--br);border-radius:var(--r);margin-bottom:12px;overflow:hidden;}
.sec-hdr{font-size:11px;font-weight:700;color:var(--t2);padding:10px 14px 8px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid var(--br);display:flex;align-items:center;gap:6px;}
.sgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;}
.sc{background:var(--bg2);border:1px solid var(--br);border-radius:var(--r);padding:14px 14px 12px;position:relative;overflow:hidden;}
.sc::after{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--c,var(--acc));}
.sc-icon{font-size:18px;margin-bottom:8px;display:block;}
.sc-val{font-size:24px;font-weight:800;line-height:1;}
.sc-lbl{font-size:11px;color:var(--t2);margin-top:4px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;}
.itm{display:flex;align-items:center;gap:12px;padding:11px 14px;border-bottom:1px solid var(--br);transition:background .1s;}
.itm:last-child{border-bottom:none;}.itm:active{background:rgba(255,255,255,.03);}
.ii{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0;}
.ib{flex:1;min-width:0;}.it{font-size:14px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.is{font-size:12px;color:var(--t2);margin-top:2px;}
.di{display:flex;align-items:center;gap:10px;padding:10px 14px;border-bottom:1px solid var(--br);}.di:last-child{border-bottom:none;}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);flex-shrink:0;box-shadow:0 0 6px rgba(35,165,90,.5);}
.pill{font-size:11px;padding:2px 8px;border-radius:20px;font-weight:600;}
.pr{background:var(--red-g);color:var(--red);border:1px solid rgba(242,63,66,.2);}
.pa{background:var(--ylw-g);color:var(--ylw);border:1px solid rgba(240,177,50,.2);}
.pg{background:var(--green-g);color:var(--green);border:1px solid rgba(35,165,90,.2);}
.pb{background:var(--acc-g);color:var(--acc);border:1px solid rgba(88,101,242,.2);}
.ab{background:var(--red-g);border:1px solid rgba(242,63,66,.25);border-radius:var(--r);padding:10px 14px;margin-bottom:12px;display:none;align-items:center;gap:8px;font-size:13px;font-weight:500;color:var(--red);}
.fld{margin-bottom:12px;}.fld label{display:block;font-size:11px;font-weight:700;color:var(--t2);margin-bottom:6px;text-transform:uppercase;letter-spacing:.8px;}
.fld input,.fld select{width:100%;padding:11px 13px;background:var(--bg3);border:1px solid var(--br2);border-radius:var(--r);color:var(--t1);font-size:14px;font-family:inherit;-webkit-appearance:none;appearance:none;transition:border-color .15s;}
.fld input:focus,.fld select:focus{outline:none;border-color:var(--acc);}
.fld-pad{padding:14px;}
.btn{width:100%;padding:13px;border-radius:var(--r);font-size:14px;font-weight:700;border:none;cursor:pointer;font-family:inherit;transition:opacity .15s,transform .1s;letter-spacing:.2px;}
.btn:active{opacity:.8;transform:scale(.98);}
.btp{background:var(--acc);color:#fff;}.btd{background:var(--red-g);color:var(--red);border:1px solid rgba(242,63,66,.25);}
.btw{background:var(--ylw-g);color:var(--ylw);border:1px solid rgba(240,177,50,.25);}
.btg{background:var(--bg3);color:var(--t1);border:1px solid var(--br2);}
.brow{display:flex;gap:8px;margin-bottom:8px;}.brow .btn{flex:1;padding:11px 6px;font-size:13px;}
.ld{display:flex;align-items:center;justify-content:center;padding:36px;color:var(--t2);flex-direction:column;gap:10px;}
.sp{width:24px;height:24px;border:2px solid var(--br2);border-top-color:var(--acc);border-radius:50%;animation:spin .7s linear infinite;}
@keyframes spin{to{transform:rotate(360deg);}}
.mt{text-align:center;padding:32px 20px;color:var(--t2);}.mi{font-size:36px;margin-bottom:10px;display:block;}
.prof-avatar{width:60px;height:60px;border-radius:50%;background:var(--acc-g);border:2px solid rgba(88,101,242,.3);display:flex;align-items:center;justify-content:center;font-size:26px;margin:0 auto 10px;}
.prof-name{font-size:17px;font-weight:700;text-align:center;}
.prof-rank{font-size:12px;font-weight:600;padding:3px 12px;border-radius:20px;background:var(--acc-g);color:var(--acc);border:1px solid rgba(88,101,242,.2);display:inline-block;margin-top:6px;}
.prof-head{padding:20px;text-align:center;border-bottom:1px solid var(--br);}
#toast{position:fixed;top:16px;left:50%;transform:translateX(-50%);background:var(--bg4);border:1px solid var(--br2);border-radius:20px;padding:9px 18px;font-size:13px;font-weight:600;z-index:999;opacity:0;transition:opacity .2s;white-space:nowrap;pointer-events:none;box-shadow:0 4px 16px rgba(0,0,0,.5);}
#toast.show{opacity:1;}
</style>
</head>
<body>
<div id="app">
  <div id="ptr">↓ Потяни чтобы обновить</div>
  <div id="cnt">
    <!-- ОБЗОР -->
    <div class="page active" id="page-overview">
      <div class="hdr">
        <div class="hdr-logo">⚔</div>
        <span class="hdr-title">Chat Guard</span>
        <span class="hdr-badge" id="hdr-online">● —</span>
      </div>
      <div style="height:14px;"></div>
      <div class="ab" id="ab">🚨 <span id="ab-txt"></span></div>
      <div class="sgrid">
        <div class="sc" style="--c:#5865f2;"><span class="sc-icon">👥</span><div class="sc-val" id="s0">—</div><div class="sc-lbl">Онлайн</div></div>
        <div class="sc" style="--c:#f0b132;"><span class="sc-icon">🚨</span><div class="sc-val" id="s1">—</div><div class="sc-lbl">Алертов</div></div>
        <div class="sc" style="--c:#23a55a;"><span class="sc-icon">🎫</span><div class="sc-val" id="s2">—</div><div class="sc-lbl">Тикеты</div></div>
        <div class="sc" style="--c:#f23f42;"><span class="sc-icon">🔨</span><div class="sc-val" id="s3">—</div><div class="sc-lbl">Банов</div></div>
      </div>
      <div class="sec"><div class="sec-hdr">🟢 На дежурстве</div><div id="duty"><div class="ld"><div class="sp"></div></div></div></div>
      <div class="sec"><div class="sec-hdr">⚡ Последние события</div><div id="evts"><div class="ld"><div class="sp"></div></div></div></div>
    </div>
    <!-- ТИКЕТЫ -->
    <div class="page" id="page-tickets">
      <div class="hdr">
        <div class="hdr-logo" style="background:var(--ylw);">🎫</div>
        <span class="hdr-title">Тикеты</span>
        <span class="hdr-badge" id="tc" style="background:var(--ylw-g);color:var(--ylw);border-color:rgba(240,177,50,.2);">—</span>
      </div>
      <div style="height:14px;"></div>
      <div style="display:flex;gap:6px;margin-bottom:12px;overflow-x:auto;padding-bottom:4px;">
        <button class="pill pb" style="white-space:nowrap;padding:6px 14px;cursor:pointer;border:none;font-size:12px;font-weight:700;" onclick="filterT('open')" id="tf-open">Открытые</button>
        <button class="pill" style="white-space:nowrap;padding:6px 14px;cursor:pointer;background:var(--bg3);border:1px solid var(--br2);color:var(--t2);font-size:12px;font-weight:700;border-radius:20px;" onclick="filterT('in_progress')" id="tf-prog">В работе</button>
        <button class="pill" style="white-space:nowrap;padding:6px 14px;cursor:pointer;background:var(--bg3);border:1px solid var(--br2);color:var(--t2);font-size:12px;font-weight:700;border-radius:20px;" onclick="filterT('closed')" id="tf-closed">Закрытые</button>
      </div>
      <div class="sec"><div id="tlist"><div class="ld"><div class="sp"></div></div></div></div>
    </div>
    <!-- ДЕЙСТВИЯ -->
    <div class="page" id="page-actions">
      <div class="hdr">
        <div class="hdr-logo" style="background:var(--red);">⚡</div>
        <span class="hdr-title">Действия</span>
      </div>
      <div style="height:14px;"></div>
      <div class="sec" style="margin-bottom:12px;">
        <div class="sec-hdr">👤 Пользователь</div>
        <div class="fld-pad">
          <div class="fld"><label>Telegram ID</label><input type="number" id="uid" placeholder="123456789" inputmode="numeric"></div>
          <div class="fld" style="margin-bottom:0;"><label>Чат</label><select id="cid"><option value="">— выбрать чат —</option></select></div>
        </div>
      </div>
      <div class="sec" style="margin-bottom:12px;">
        <div class="sec-hdr">🔨 Модерация</div>
        <div class="fld-pad">
          <div class="brow"><button class="btn btd" onclick="act('ban')">🔨 Бан</button><button class="btn btg" onclick="act('unban')">🕊️ Разбан</button></div>
          <div class="brow"><button class="btn btw" onclick="actMute()">🔇 Мут</button><button class="btn btg" onclick="act('unmute')">🔊 Размут</button></div>
          <div class="brow" style="margin-bottom:0;"><button class="btn btg" onclick="act('kick')">👟 Кик</button><button class="btn btw" onclick="act('warn')">⚠️ Варн</button></div>
        </div>
      </div>
      <div class="sec" style="margin-bottom:12px;">
        <div class="sec-hdr">📨 Сообщение в чат</div>
        <div class="fld-pad">
          <div class="fld"><label>Текст</label><input type="text" id="msg" placeholder="Введи сообщение..."></div>
          <button class="btn btp" onclick="doSend()">Отправить</button>
        </div>
      </div>
      <div class="sec">
        <div class="sec-hdr">🔒 Управление чатом</div>
        <div class="fld-pad">
          <div class="brow" style="margin-bottom:0;"><button class="btn btd" onclick="chatAct('lock')">🔒 Локдаун</button><button class="btn btg" onclick="chatAct('unlock')">🔓 Открыть</button></div>
        </div>
      </div>
    </div>
    <!-- ПРОФИЛЬ -->
    <div class="page" id="page-profile">
      <div class="hdr">
        <div class="hdr-logo" style="background:var(--green);">👤</div>
        <span class="hdr-title">Профиль</span>
      </div>
      <div style="height:14px;"></div>
      <div id="prof"><div class="ld"><div class="sp"></div></div></div>
    </div>
  </div>
  <nav id="tabs">
    <button class="tab active" onclick="go('overview',this)"><span class="ti">📊</span><span>Обзор</span></button>
    <button class="tab" onclick="go('tickets',this)"><span class="ti">🎫</span><span>Тикеты</span></button>
    <button class="tab" onclick="go('actions',this)"><span class="ti">⚡</span><span>Действия</span></button>
    <button class="tab" onclick="go('profile',this)"><span class="ti">👤</span><span>Профиль</span></button>
  </nav>
</div>
<div id="toast"></div>
<script>
const tg=window.Telegram.WebApp;
tg.ready();tg.expand();
tg.setBackgroundColor('#0d0f14');
if(tg.setHeaderColor)tg.setHeaderColor('#0d0f14');

const BASE='';
let tok='',tf='open';

function e(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

let _tt;
function toast(msg,type){
  const el=document.getElementById('toast');
  el.textContent=msg;
  el.style.background=type==='d'?'rgba(242,63,66,.9)':'';
  el.style.color=type==='d'?'#fff':'';
  el.classList.add('show');clearTimeout(_tt);
  _tt=setTimeout(()=>el.classList.remove('show'),2500);
}

async function auth(){
  const id=tg.initData;
  if(!id){toast('❌ Открой через Telegram','d');return false;}
  try{
    const r=await fetch(BASE+'/api/mini/auth',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({initData:id})});
    const d=await r.json();
    if(d.ok){tok=d.token;return true;}
    toast('❌ '+(d.error||'Нет доступа'),'d');
    tg.showAlert('Нет доступа: '+(d.error||''));return false;
  }catch(err){toast('❌ Ошибка авторизации','d');return false;}
}

async function api(path,opts={}){
  opts.headers=opts.headers||{};opts.headers['X-Mini-Token']=tok;
  if(opts.body&&!opts.headers['Content-Type'])opts.headers['Content-Type']='application/json';
  try{
    const r=await fetch(BASE+path,opts);
    if(r.status===401){toast('❌ Сессия истекла','d');return null;}
    return r.json();
  }catch(err){toast('❌ Сеть','d');return null;}
}

function go(name,el){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  if(el)el.classList.add('active');
  document.getElementById('cnt').scrollTop=0;
  if(name==='overview')loadOv();if(name==='tickets')loadT();
  if(name==='actions')loadChats();if(name==='profile')loadProf();
  tg.HapticFeedback.impactOccurred('light');
}

async function loadOv(){
  const d=await api('/api/mini/stats');if(!d)return;
  document.getElementById('s0').textContent=d.online??'—';
  document.getElementById('s1').textContent=d.alerts??'—';
  document.getElementById('s2').textContent=d.tickets_open??'—';
  document.getElementById('s3').textContent=d.bans??'—';
  document.getElementById('hdr-online').textContent='● '+(d.online||0)+' онлайн';
  const ab=document.getElementById('ab');
  if(d.alerts>0){ab.style.display='flex';document.getElementById('ab-txt').textContent=d.alerts+' активных алертов';}
  else ab.style.display='none';
  const duty=document.getElementById('duty');
  if(d.duty&&d.duty.length){
    duty.innerHTML=d.duty.map(m=>`<div class="di"><div class="dot"></div><div style="flex:1;font-size:14px;font-weight:500;">${e(m.name)}</div><div style="font-size:11px;color:var(--t2);font-weight:600;">${e(m.rank_name||'')}</div></div>`).join('');
  }else duty.innerHTML='<div class="mt"><span class="mi">😴</span>Никто не дежурит</div>';
  const evts=document.getElementById('evts');
  if(d.events&&d.events.length){
    evts.innerHTML=d.events.map(ev=>`<div class="itm"><div class="ii" style="background:var(--acc-g);">${ev.icon||'⚡'}</div><div class="ib"><div class="it">${e(ev.text)}</div><div class="is">${e(ev.time||'')}</div></div></div>`).join('');
  }else evts.innerHTML='<div class="mt"><span class="mi">🌙</span>Событий нет</div>';
}

async function loadT(){
  const d=await api('/api/mini/tickets?status='+tf);if(!d)return;
  const list=document.getElementById('tlist');
  const cnt=d.tickets||[];
  document.getElementById('tc').textContent=cnt.length;
  if(!cnt.length){list.innerHTML='<div class="mt"><span class="mi">✅</span>Тикетов нет</div>';return;}
  list.innerHTML=cnt.map(t=>{
    const cls=t.status==='open'?'pr':t.status==='in_progress'?'pa':'pg';
    const lbl=t.status==='open'?'Открыт':t.status==='in_progress'?'В работе':'Закрыт';
    return `<div class="itm"><div class="ii" style="background:var(--acc-g);">🎫</div><div class="ib"><div class="it">#${t.id} ${e(t.subject||'—')}</div><div class="is">${e(t.user_name||'')} · ${e((t.created_at||'').slice(0,16))}</div></div><span class="pill ${cls}">${lbl}</span></div>`;
  }).join('');
}

function filterT(s){
  tf=s;
  ['open','prog','closed'].forEach(k=>{
    const el=document.getElementById('tf-'+k);el.className='pill';
    el.style.cssText='white-space:nowrap;padding:6px 14px;cursor:pointer;background:var(--bg3);border:1px solid var(--br2);color:var(--t2);font-size:12px;font-weight:700;border-radius:20px;';
  });
  const active=document.getElementById('tf-'+(s==='in_progress'?'prog':s));
  if(active){active.className='pill pb';active.style.cssText='white-space:nowrap;padding:6px 14px;cursor:pointer;border:none;font-size:12px;font-weight:700;';}
  loadT();
}

async function loadChats(){
  const d=await api('/api/mini/chats');if(!d)return;
  const sel=document.getElementById('cid');const cur=sel.value;
  sel.innerHTML='<option value="">— выбрать чат —</option>';
  (d.chats||[]).forEach(c=>{const o=document.createElement('option');o.value=c.cid;o.textContent=c.title;sel.appendChild(o);});
  if(cur)sel.value=cur;
}

async function act(action){
  const uid=document.getElementById('uid').value;const cid=document.getElementById('cid').value;
  if(!uid){tg.showAlert('Введи Telegram ID');return;}if(!cid){tg.showAlert('Выбери чат');return;}
  const r=await api('/api/mini/action',{method:'POST',body:JSON.stringify({action,user_id:+uid,chat_id:+cid})});
  if(r?.ok){toast('✅ '+r.msg);tg.HapticFeedback.notificationOccurred('success');}
  else{toast('❌ '+(r?.msg||'Ошибка'),'d');tg.HapticFeedback.notificationOccurred('error');}
}

async function actMute(){
  const uid=document.getElementById('uid').value;const cid=document.getElementById('cid').value;
  if(!uid){tg.showAlert('Введи Telegram ID');return;}if(!cid){tg.showAlert('Выбери чат');return;}
  tg.showPopup({title:'Мут',message:'Выбери длительность:',
    buttons:[{id:'30',type:'default',text:'30 мин'},{id:'60',type:'default',text:'1 час'},{id:'1440',type:'default',text:'24 часа'},{id:'cancel',type:'cancel',text:'Отмена'}]
  },async b=>{
    if(b==='cancel'||!b)return;
    const r=await api('/api/mini/action',{method:'POST',body:JSON.stringify({action:'mute',user_id:+uid,chat_id:+cid,arg:b})});
    if(r?.ok){toast('✅ '+r.msg);tg.HapticFeedback.notificationOccurred('success');}else toast('❌ '+(r?.msg||'Ошибка'),'d');
  });
}

async function doSend(){
  const msg=document.getElementById('msg').value.trim();const cid=document.getElementById('cid').value;
  if(!msg){tg.showAlert('Введи текст');return;}if(!cid){tg.showAlert('Выбери чат');return;}
  const r=await api('/api/mini/action',{method:'POST',body:JSON.stringify({action:'send_message',chat_id:+cid,arg:msg})});
  if(r?.ok){toast('✅ Отправлено');document.getElementById('msg').value='';tg.HapticFeedback.notificationOccurred('success');}
  else toast('❌ '+(r?.msg||'Ошибка'),'d');
}

async function chatAct(action){
  const cid=document.getElementById('cid').value;if(!cid){tg.showAlert('Выбери чат');return;}
  tg.showConfirm(action==='lock'?'Заблокировать чат?':'Разблокировать чат?',async ok=>{
    if(!ok)return;
    const r=await api('/api/mini/action',{method:'POST',body:JSON.stringify({action,chat_id:+cid})});
    if(r?.ok)toast('✅ '+r.msg);else toast('❌ '+(r?.msg||'Ошибка'),'d');
  });
}

async function loadProf(){
  const el=document.getElementById('prof');const d=await api('/api/mini/me');
  if(!d||!d.admin){el.innerHTML='<div class="mt"><span class="mi">👤</span>Профиль не найден</div>';return;}
  const a=d.admin,st=d.stats||{};
  el.innerHTML=`
    <div class="sec" style="margin-bottom:12px;">
      <div class="prof-head">
        <div class="prof-avatar">👤</div>
        <div class="prof-name">${e(a.name)}</div>
        <div><span class="prof-rank">${e(a.rank_name||'Модератор')}</span></div>
      </div>
    </div>
    <div class="sec" style="margin-bottom:12px;">
      <div class="sec-hdr">📊 Статистика</div>
      <div class="sgrid" style="padding:12px;margin-bottom:0;">
        <div class="sc" style="--c:var(--red);"><span class="sc-icon">🔨</span><div class="sc-val">${st.bans||0}</div><div class="sc-lbl">Банов</div></div>
        <div class="sc" style="--c:var(--ylw);"><span class="sc-icon">⚠️</span><div class="sc-val">${st.warns||0}</div><div class="sc-lbl">Варнов</div></div>
        <div class="sc" style="--c:var(--acc);"><span class="sc-icon">🔇</span><div class="sc-val">${st.mutes||0}</div><div class="sc-lbl">Мутов</div></div>
        <div class="sc" style="--c:var(--green);"><span class="sc-icon">🎫</span><div class="sc-val">${st.tickets||0}</div><div class="sc-lbl">Тикетов</div></div>
      </div>
    </div>
    <div class="sec">
      <div class="sec-hdr">⏰ Последние действия</div>
      ${(d.recent||[]).length
        ?d.recent.map(r=>`<div class="itm"><div class="ii" style="background:var(--acc-g);">📋</div><div class="ib"><div class="it">${e(r.action||'—')}</div><div class="is">${e((r.created_at||'').slice(0,16))}</div></div></div>`).join('')
        :'<div class="mt" style="padding:20px;">Нет действий</div>'}
    </div>`;
}

let ps=0,pp=false;
const cnt=document.getElementById('cnt'),ptr=document.getElementById('ptr');
cnt.addEventListener('touchstart',ev=>{if(cnt.scrollTop===0){ps=ev.touches[0].clientY;pp=true;}},{passive:true});
cnt.addEventListener('touchmove',ev=>{if(pp&&ev.touches[0].clientY-ps>0)ptr.classList.add('vis');},{passive:true});
cnt.addEventListener('touchend',()=>{
  if(pp){ptr.classList.remove('vis');
  const p=document.querySelector('.page.active')?.id?.replace('page-','');
  if(p==='overview')loadOv();if(p==='tickets')loadT();
  pp=false;tg.HapticFeedback.impactOccurred('medium');}
});

(async()=>{
  const ok=await auth();if(!ok)return;
  loadOv();
  setInterval(()=>{if(document.querySelector('.page.active')?.id==='page-overview')loadOv();},30000);
})();
</script>
</body>
</html>
