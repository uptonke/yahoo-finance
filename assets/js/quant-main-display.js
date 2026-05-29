(() => {
  "use strict";

  const Q_PANEL_ID = "quant-meta-inline-panel-v7";
  const MC_PANEL_ID = "market-compass-inline-panel-v3";
  const STYLE_ID = "quant-compass-inline-style-v7";
  const SUPABASE_URL = "https://yrccanqxzrcoknzabifz.supabase.co";
  const SUPABASE_KEY = "sb_publishable_lDfwRDxgMhzRwVk0-Qu3vg_9HTmTFZy";
  const TABLE = "portfolio_db";
  const ROW_ID = 1;

  let cache = { stockMeta: null, ledgerData: null, macroMeta: null, lastFetch: 0, error: null };

  function injectStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
      #${Q_PANEL_ID},#${MC_PANEL_ID}{margin-bottom:16px;border:1px solid rgba(255,255,255,.12);border-radius:18px;background:rgba(15,23,42,.78);backdrop-filter:blur(18px);box-shadow:0 18px 45px rgba(0,0,0,.24);overflow:hidden}
      #${Q_PANEL_ID} .qmp-head,#${MC_PANEL_ID} .mcp-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid rgba(255,255,255,.10);background:linear-gradient(135deg,rgba(250,204,21,.08),rgba(56,189,248,.05))}
      #${Q_PANEL_ID} .qmp-title,#${MC_PANEL_ID} .mcp-title{color:#fff;font-weight:900;letter-spacing:.02em;font-size:15px}
      #${Q_PANEL_ID} .qmp-sub,#${MC_PANEL_ID} .mcp-sub{color:rgba(203,213,225,.78);font-size:11px;line-height:1.6;margin-top:5px}
      #${Q_PANEL_ID} .qmp-meta,#${MC_PANEL_ID} .mcp-meta{color:rgba(148,163,184,.9);font-size:10px;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;text-align:right;white-space:nowrap}
      #${Q_PANEL_ID} .qmp-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}
      #${Q_PANEL_ID} table{width:max-content;min-width:100%;border-collapse:collapse;font-size:12px;color:#e2e8f0}
      #${Q_PANEL_ID} th{position:sticky;top:0;z-index:1;background:rgba(15,23,42,.96);color:#94a3b8;font-size:10px;text-transform:uppercase;letter-spacing:.08em;font-weight:800;padding:10px;border-bottom:1px solid rgba(255,255,255,.10);text-align:right;white-space:nowrap}
      #${Q_PANEL_ID} th .hint{display:block;margin-top:3px;font-size:9px;letter-spacing:.02em;font-weight:900;opacity:.95;text-transform:none}
      #${Q_PANEL_ID} th .good-hint{color:#f87171} #${Q_PANEL_ID} th .risk-hint{color:#4ade80}
      #${Q_PANEL_ID} td{padding:10px;border-bottom:1px solid rgba(255,255,255,.06);text-align:right;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;white-space:nowrap}
      #${Q_PANEL_ID} th:first-child,#${Q_PANEL_ID} td:first-child{text-align:left;position:sticky;left:0;background:rgba(15,23,42,.98);z-index:2}
      #${Q_PANEL_ID} tr:hover td{background:rgba(255,255,255,.045)}
      #${Q_PANEL_ID} .ticker{color:#fff;font-weight:900}
      #${Q_PANEL_ID} .badge{display:inline-flex;align-items:center;justify-content:center;min-width:42px;padding:3px 7px;border-radius:999px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);font-weight:800}
      #${Q_PANEL_ID} .good{color:#f87171;background:rgba(239,68,68,.10);border-color:rgba(239,68,68,.18)}
      #${Q_PANEL_ID} .mid{color:#facc15;background:rgba(250,204,21,.10);border-color:rgba(250,204,21,.18)}
      #${Q_PANEL_ID} .bad{color:#4ade80;background:rgba(34,197,94,.10);border-color:rgba(34,197,94,.18)}
      #${Q_PANEL_ID} .risk-high{color:#f87171;background:rgba(239,68,68,.10);border-color:rgba(239,68,68,.18)}
      #${Q_PANEL_ID} .risk-mid{color:#facc15;background:rgba(250,204,21,.10);border-color:rgba(250,204,21,.18)}
      #${Q_PANEL_ID} .risk-low{color:#4ade80;background:rgba(34,197,94,.10);border-color:rgba(34,197,94,.18)}
      #${Q_PANEL_ID} .quality-ok{color:#4ade80} #${Q_PANEL_ID} .quality-thin{color:#facc15} #${Q_PANEL_ID} .quality-stale{color:#fb7185}
      #${Q_PANEL_ID} .src{max-width:190px;overflow:hidden;text-overflow:ellipsis;display:inline-block;vertical-align:bottom;color:#93c5fd}
      #${Q_PANEL_ID} .pivot-cell{line-height:1.45;color:#e2e8f0} #${Q_PANEL_ID} .pivot-cell .hint{display:block;margin-top:2px;color:#94a3b8;font-size:10px;font-weight:800}
      #${MC_PANEL_ID} .mcp-body{padding:16px 18px;display:grid;grid-template-columns:1fr;gap:14px}
      #${MC_PANEL_ID} .mcp-score-row{display:grid;grid-template-columns:minmax(300px,440px) 1fr;gap:14px;align-items:stretch}
      #${MC_PANEL_ID} .mcp-score-stack{display:grid;grid-template-columns:1fr 1fr;gap:10px}
      #${MC_PANEL_ID} .mcp-score-card{border:1px solid rgba(255,255,255,.10);background:rgba(0,0,0,.22);border-radius:16px;padding:16px;display:flex;flex-direction:column;justify-content:center;min-height:130px}
      #${MC_PANEL_ID} .mcp-score-card.adjusted{border-color:rgba(250,204,21,.25);background:rgba(250,204,21,.055)}
      #${MC_PANEL_ID} .mcp-score{font-size:38px;line-height:1;font-weight:900;color:#fff;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
      #${MC_PANEL_ID} .mcp-score small{font-size:15px;color:#94a3b8;margin-left:3px}
      #${MC_PANEL_ID} .mcp-regime{margin-top:10px;display:inline-flex;width:max-content;align-items:center;gap:7px;padding:5px 9px;border-radius:999px;font-size:12px;font-weight:900;border:1px solid rgba(255,255,255,.12)}
      #${MC_PANEL_ID} .mcp-regime.hot{color:#fb7185;background:rgba(239,68,68,.12);border-color:rgba(239,68,68,.25)}
      #${MC_PANEL_ID} .mcp-regime.cold{color:#60a5fa;background:rgba(59,130,246,.12);border-color:rgba(59,130,246,.25)}
      #${MC_PANEL_ID} .mcp-regime.ok{color:#4ade80;background:rgba(34,197,94,.12);border-color:rgba(34,197,94,.25)}
      #${MC_PANEL_ID} .mcp-regime.mid{color:#facc15;background:rgba(250,204,21,.12);border-color:rgba(250,204,21,.25)}
      #${MC_PANEL_ID} .mcp-reasons,#${MC_PANEL_ID} .mcp-ai-card{border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.035);border-radius:16px;padding:14px;color:#cbd5e1;font-size:12px;line-height:1.65}
      #${MC_PANEL_ID} .mcp-reasons .label,#${MC_PANEL_ID} .mcp-ai-card .label{color:#94a3b8;font-size:10px;font-weight:900;text-transform:uppercase;letter-spacing:.14em;margin-bottom:6px}
      #${MC_PANEL_ID} .mcp-quality-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}
      #${MC_PANEL_ID} .mcp-chip{border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:9px 10px;background:rgba(0,0,0,.18)}
      #${MC_PANEL_ID} .mcp-chip .k{font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:.12em;font-weight:900}
      #${MC_PANEL_ID} .mcp-chip .v{margin-top:4px;font-size:13px;font-weight:900;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
      #${MC_PANEL_ID} .mcp-penalties{border:1px solid rgba(251,113,133,.18);background:rgba(239,68,68,.06);border-radius:16px;padding:12px;color:#fecdd3;font-size:11px;line-height:1.65}
      #${MC_PANEL_ID} .mcp-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}
      #${MC_PANEL_ID} .mcp-module{border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.035);border-radius:14px;padding:12px;min-height:116px}
      #${MC_PANEL_ID} .mcp-kicker{font-size:10px;color:#94a3b8;font-weight:900;text-transform:uppercase;letter-spacing:.12em}
      #${MC_PANEL_ID} .mcp-module-title{margin-top:4px;color:#fff;font-weight:900;font-size:13px;display:flex;align-items:center;justify-content:space-between;gap:8px}
      #${MC_PANEL_ID} .mcp-module-score{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:20px;font-weight:900;margin-top:10px}
      #${MC_PANEL_ID} .mcp-module-note{margin-top:7px;color:#94a3b8;font-size:10px;line-height:1.45;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
      #${MC_PANEL_ID} .mcp-ai-section{margin-top:10px;border-top:1px solid rgba(255,255,255,.08);padding-top:10px}
      #${MC_PANEL_ID} .mcp-ai-section h4{margin:0 0 6px;color:#fff;font-size:12px;font-weight:900}
      #${MC_PANEL_ID} .mcp-ai-section div{margin:3px 0}
      #${MC_PANEL_ID} .s-good{color:#4ade80} #${MC_PANEL_ID} .s-mid{color:#facc15} #${MC_PANEL_ID} .s-bad{color:#fb7185}
      #${MC_PANEL_ID} .mcp-source{font-size:10px;color:#64748b;line-height:1.5;border-top:1px solid rgba(255,255,255,.08);padding-top:10px}
      @media(max-width:1200px){#${MC_PANEL_ID} .mcp-grid{grid-template-columns:repeat(3,minmax(0,1fr))}#${MC_PANEL_ID} .mcp-score-row{grid-template-columns:1fr}}
      @media(max-width:768px){#${Q_PANEL_ID},#${MC_PANEL_ID}{border-radius:14px;margin:0 6px 14px}#${Q_PANEL_ID} .qmp-head,#${MC_PANEL_ID} .mcp-head{flex-direction:column;padding:14px}#${Q_PANEL_ID} .qmp-meta,#${MC_PANEL_ID} .mcp-meta{text-align:left}#${Q_PANEL_ID} th,#${Q_PANEL_ID} td{padding:9px 8px;font-size:11px}#${MC_PANEL_ID} .mcp-body{padding:14px}#${MC_PANEL_ID} .mcp-score-stack{grid-template-columns:1fr}#${MC_PANEL_ID} .mcp-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#${MC_PANEL_ID} .mcp-quality-grid{grid-template-columns:repeat(2,minmax(0,1fr))}#${MC_PANEL_ID} .mcp-score{font-size:34px}}
    `;
    document.head.appendChild(style);
  }

  function escapeHtml(value){return String(value??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#039;");}
  function num(value,digits=2){const n=Number(value);return Number.isFinite(n)?n.toFixed(digits):"N/A";}
  function scoreClass(value,inverse=false){const n=Number(value);if(!Number.isFinite(n))return"";if(!inverse)return n>=7?"good":n>=4?"mid":"bad";return n>=7?"risk-high":n>=4?"risk-mid":"risk-low";}
  function qualityClass(value){const q=String(value||"").toLowerCase();if(q==="ok")return"quality-ok";if(q==="thin")return"quality-thin";if(q==="stale")return"quality-stale";return"";}
  function qualityLabel(value){const q=String(value||"").toLowerCase();if(q==="ok")return"正常";if(q==="thin")return"資料偏少";if(q==="stale")return"資料過舊";return"N/A";}
  function sourceLabel(value){return String(value||"N/A").replace("YahooChart:","雅虎圖表：").replace("yfinance:","Yahoo Finance：").replace("FinMind","FinMind");}
  function fmtPct(value){const n=Number(value);if(!Number.isFinite(n))return"N/A";return`${n>=0?"+":""}${n.toFixed(2)}%`;}
  function fmtPrice(value){const n=Number(value);if(!Number.isFinite(n))return"N/A";if(Math.abs(n)>=1000)return n.toLocaleString(undefined,{maximumFractionDigits:0});if(Math.abs(n)>=100)return n.toFixed(1);return n.toFixed(2);}
  function fmtZone(low,high){const a=Number(low),b=Number(high);if(!Number.isFinite(a)||!Number.isFinite(b))return"N/A";if(Math.abs(a-b)<Math.max(Math.abs(a),1)*0.0005)return fmtPrice((a+b)/2);return`${fmtPrice(a)}–${fmtPrice(b)}`;}
  function pivotCell(meta,side){const low=meta[`pivot_${side}_zone_low`];const high=meta[`pivot_${side}_zone_high`];const dist=meta[`pivot_${side}_distance_pct`];const conf=meta[`pivot_${side}_confluence`]||"N/A";const methods=meta[`pivot_${side}_methods`];const title=Array.isArray(methods)?methods.join(", "):"";return`<span class="pivot-cell" title="${escapeHtml(title)}">${escapeHtml(fmtZone(low,high))}<br><span class="hint">${escapeHtml(fmtPct(dist))}｜${escapeHtml(conf)}</span></span>`;}

  function ensureQuantPanel(){let panel=document.getElementById(Q_PANEL_ID);if(panel)return panel;const section=document.querySelector(".holdings-terminal-view");if(!section)return null;panel=document.createElement("div");panel.id=Q_PANEL_ID;section.insertBefore(panel,section.firstChild);return panel;}
  function ensureCompassPanel(){let panel=document.getElementById(MC_PANEL_ID);if(panel)return panel;const anchor=document.querySelector(".analytics-command-toolbar");if(!anchor||!anchor.parentNode)return null;panel=document.createElement("div");panel.id=MC_PANEL_ID;anchor.parentNode.insertBefore(panel,anchor);return panel;}
  function activeTickers(ledger){const map=new Map();(ledger||[]).forEach((tx)=>{const ticker=String(tx?.ticker||"").trim().toUpperCase();if(!ticker)return;const type=String(tx?.type||"");if(type!=="Buy"&&type!=="Sell")return;const shares=Number(tx?.shares)||0;map.set(ticker,(map.get(ticker)||0)+(type==="Buy"?shares:-shares));});return Array.from(map.entries()).filter(([,shares])=>shares>0.0001).map(([ticker])=>ticker).sort();}

  async function fetchSupabaseData(force=false){
    const now=Date.now();
    if(!force&&cache.stockMeta&&now-cache.lastFetch<60000)return cache;
    if(!window.supabase?.createClient){cache.error="Supabase JS 尚未載入。";return cache;}
    try{
      const client=window.supabase.createClient(SUPABASE_URL,SUPABASE_KEY);
      const{data,error}=await client.from(TABLE).select("stock_meta, ledger_data, macro_meta").eq("id",ROW_ID).single();
      if(error)throw error;
      cache.stockMeta=data?.stock_meta||{};cache.ledgerData=data?.ledger_data||[];cache.macroMeta=data?.macro_meta||{};cache.lastFetch=now;cache.error=null;window.__quantMetaDisplayCache=cache;return cache;
    }catch(err){cache.error=err?.message||String(err);window.__quantMetaDisplayCache=cache;return cache;}
  }

  function row(ticker,meta){return`<tr>
    <td><span class="ticker">${escapeHtml(ticker)}</span></td>
    <td><span class="badge">${num(meta.beta,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.trend_score)}">${num(meta.trend_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.momentum_score)}">${num(meta.momentum_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.risk_score,true)}">${num(meta.risk_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.technical_score)}">${num(meta.technical_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.valuation_score)}">${num(meta.valuation_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.chip_score)}">${num(meta.chip_score,2)}</span></td>
    <td><span class="badge ${scoreClass(meta.quant_health_score)}">${num(meta.quant_health_score,2)}</span></td>
    <td>${pivotCell(meta,"support")}</td>
    <td>${pivotCell(meta,"resistance")}</td>
    <td>${escapeHtml(meta.pivot_status||"N/A")}</td>
    <td><span class="${qualityClass(meta.data_quality)}">${escapeHtml(qualityLabel(meta.data_quality))}</span></td>
    <td><span class="src" title="${escapeHtml(meta.source||"N/A")}">${escapeHtml(sourceLabel(meta.source))}</span></td>
    <td>${escapeHtml(meta.updated_at||"N/A")}</td>
  </tr>`;}

  function renderQuantPanel(data){
    const panel=ensureQuantPanel();if(!panel)return;
    const stockMeta=data.stockMeta||{};
    const tickers=activeTickers(data.ledgerData||[]);
    const list=tickers.length?tickers:Object.keys(stockMeta).sort();
    const latest=list.map((t)=>stockMeta?.[t]?.updated_at).filter(Boolean).sort().slice(-1)[0]||"N/A";
    if(data.error){panel.innerHTML=`<div class="qmp-head"><div><div class="qmp-title">量化因子總覽 <span style="color:#fb7185">讀取失敗</span></div><div class="qmp-sub">讀取 Supabase stock_meta 失敗。原因：${escapeHtml(data.error)}</div></div></div>`;return;}
    panel.innerHTML=`<div class="qmp-head"><div><div class="qmp-title">量化因子總覽</div><div class="qmp-sub">直接讀取 Supabase 的 portfolio_db.stock_meta。貝塔顯示小數點後兩位；風險分數越高代表風險越高，其餘分數越高通常越佳。支撐/壓力使用 monthly 五法樞軸共振區，不是五法平均值。</div></div><div class="qmp-meta">持倉數=${list.length}<br>最新更新=${escapeHtml(latest)}</div></div><div class="qmp-wrap"><table><thead><tr><th>代號</th><th title="Beta 不是好壞分數；越高代表對市場越敏感">貝塔<br><span class="hint">低穩/高敏</span></th><th title="越高代表趨勢越強">趨勢<br><span class="hint good-hint">高好</span></th><th title="越高代表近期與中期動能越強">動能<br><span class="hint good-hint">高好</span></th><th title="越低代表波動、回撤與系統性風險較低">風險<br><span class="hint risk-hint">低好</span></th><th title="越高代表 RSI、MACD、均線狀態越偏多">技術面<br><span class="hint good-hint">高好</span></th><th title="越高代表估值壓力相對較低；美股 ETF 可能資料不足">估值<br><span class="hint good-hint">高好</span></th><th title="越高代表台股籌碼較強；非台股可能為 N/A">籌碼<br><span class="hint good-hint">高好</span></th><th title="綜合趨勢、技術、籌碼、估值與風險後的總分">健康度<br><span class="hint good-hint">高好</span></th><th title="目前價格下方最近的月線五法樞軸共振區；越近且共振越高，下方防守越明確">月支撐區<br><span class="hint good-hint">近支好</span></th><th title="目前價格上方最近的月線五法樞軸共振區；越遠越有上方空間，越近且共振高代表追價風險較高">月壓力區<br><span class="hint risk-hint">壓遠好</span></th><th title="接近月支撐、接近月壓力、月線區間中段、月線樞軸壓縮、突破或跌破月樞軸">月樞軸狀態</th><th title="正常最好；資料偏少或過舊時不要過度解讀">資料品質<br><span class="hint good-hint">正常好</span></th><th title="本次量化資料來源">資料來源</th><th title="量化資料更新日期">更新日</th></tr></thead><tbody>${list.map((ticker)=>row(ticker,stockMeta[ticker]||{})).join("")}</tbody></table></div>`;
  }

  function scoreTone(score){const n=Number(score);if(!Number.isFinite(n))return"mid";if(n>=70)return"ok";if(n>=55)return"mid";if(n>=40)return"cold";return"hot";}
  function scoreTextClass(score){const n=Number(score);if(!Number.isFinite(n))return"s-mid";if(n>=65)return"s-good";if(n>=45)return"s-mid";return"s-bad";}
  function statusClass(value){const v=String(value||"").toUpperCase();if(["ON","OK","FULL"].includes(v))return"s-good";if(["OFF","FAILED","LOW","INVALID","PROXY_ONLY"].includes(v))return"s-bad";return"s-mid";}
  function moduleName(key){return ({trend:"趨勢",breadth:"廣度",credit:"信用",volatility:"波動",liquidity:"流動性",cross_asset:"跨資產"})[key]||key;}
  function moduleIcon(key){return ({trend:"fa-arrow-trend-up",breadth:"fa-ruler-combined",credit:"fa-building-columns",volatility:"fa-wave-square",liquidity:"fa-water",cross_asset:"fa-globe"})[key]||"fa-circle-nodes";}
  function renderModule(key,mod){const score=Number(mod?.score);return`<div class="mcp-module"><div class="mcp-kicker">${escapeHtml(key)}</div><div class="mcp-module-title"><span><i class="fas ${moduleIcon(key)}"></i> ${moduleName(key)}</span><span class="${scoreTextClass(score)}">${escapeHtml(mod?.label||"")}</span></div><div class="mcp-module-score ${scoreTextClass(score)}">${Number.isFinite(score)?score.toFixed(1):"N/A"}</div><div class="mcp-module-note" title="${escapeHtml(mod?.note||"")}">${escapeHtml(mod?.note||"N/A")}</div></div>`;}
  function renderChip(label,value){return`<div class="mcp-chip"><div class="k">${escapeHtml(label)}</div><div class="v ${statusClass(value)}">${escapeHtml(value||"N/A")}</div></div>`;}
  function renderList(items){return Array.isArray(items)&&items.length?items.map((x)=>`<div>• ${escapeHtml(x)}</div>`).join(""):`<div>• N/A</div>`;}
  function renderAiSection(title,items){return`<div class="mcp-ai-section"><h4>${escapeHtml(title)}</h4>${renderList(items)}</div>`;}

  function renderAiCard(compass){
    const ai=compass.ai_data_deep_analysis||{};
    if(!ai || !Object.keys(ai).length) return "";
    return `<div class="mcp-ai-card"><div class="label">${escapeHtml(ai.title||"AI 數據深度分析")}</div><div style="color:#94a3b8;font-size:11px;margin-bottom:8px;">${escapeHtml(ai.subtitle||"只整理數據，不輸出操作建議。")}</div>${renderAiSection("市場結構摘要",ai.market_structure_summary)}${renderAiSection("分數拆解",ai.score_decomposition)}${renderAiSection("主要背離",ai.major_divergences)}${renderAiSection("資料品質與盲區",ai.data_quality_and_blindspots)}${renderAiSection("投組暴露來源",ai.portfolio_exposure_sources)}${renderAiSection("需要人工檢查的資料點",ai.manual_checkpoints)}</div>`;
  }

  function renderCompassPanel(data){
    const panel=ensureCompassPanel();if(!panel)return;
    const compass=data?.macroMeta?.market_compass;
    if(data.error){panel.innerHTML=`<div class="mcp-head"><div><div class="mcp-title">Market Compass 讀取失敗</div><div class="mcp-sub">${escapeHtml(data.error)}</div></div></div>`;return;}
    if(!compass){panel.innerHTML=`<div class="mcp-head"><div><div class="mcp-title">Market Compass</div><div class="mcp-sub">尚未偵測到 macro_meta.market_compass。請先跑 Daily Quant Pipeline，並確認已加入 FRED_API_KEY。</div></div><div class="mcp-meta">Phase 3<br>等待資料</div></div>`;return;}
    const raw=Number(compass.raw_score ?? compass.score);
    const adjusted=Number(compass.adjusted_score ?? compass.score);
    const penaltyPoints=Number(compass.penalty_points ?? 0);
    const mods=compass.modules||{};
    const dq=compass.data_quality||{};
    const order=["trend","breadth","credit","volatility","liquidity","cross_asset"];
    const reasons=Array.isArray(compass.reasons)?compass.reasons:[];
    const penalties=Array.isArray(compass.penalties)?compass.penalties:[];
    const fredStatus=compass.fred_status||dq.fred_status||(compass.fred_enabled?"ON":"OFF");
    const creditQuality=dq.credit_quality||mods?.credit?.confidence||"N/A";
    panel.innerHTML=`<div class="mcp-head"><div><div class="mcp-title">🧭 Market Compass Phase 3</div><div class="mcp-sub">Raw Score 保留原始加權；Adjusted Score 扣除趨勢/廣度背離、信用資料缺失與跨資產未確認。AI 數據深度分析只整理資料，不輸出買賣、加減碼或目標價。</div></div><div class="mcp-meta">更新=${escapeHtml(compass.date||"N/A")}<br>FRED=${escapeHtml(fredStatus)}</div></div><div class="mcp-body"><div class="mcp-score-row"><div class="mcp-score-stack"><div class="mcp-score-card"><div class="mcp-kicker">Raw Score</div><div class="mcp-score">${Number.isFinite(raw)?raw.toFixed(1):"N/A"}<small>/100</small></div><div class="mcp-regime mid"><i class="fas fa-calculator"></i>原始加權</div></div><div class="mcp-score-card adjusted"><div class="mcp-kicker">Adjusted Score</div><div class="mcp-score">${Number.isFinite(adjusted)?adjusted.toFixed(1):"N/A"}<small>/100</small></div><div class="mcp-regime ${scoreTone(adjusted)}"><i class="fas fa-signal"></i>${escapeHtml(compass.regime||compass.label||"N/A")}</div></div></div><div class="mcp-reasons"><div class="label">Primary Reasons</div>${renderList(reasons)}</div></div><div class="mcp-quality-grid">${renderChip("Data Quality",dq.overall||"N/A")}${renderChip("FRED",fredStatus)}${renderChip("Credit",creditQuality)}${renderChip("Yahoo",dq.yahoo_status||"N/A")}</div>${penalties.length?`<div class="mcp-penalties"><div class="mcp-kicker">Penalty Reasons · ${penaltyPoints.toFixed(1)} pts</div>${penalties.map((p)=>`<div>• ${escapeHtml(p.reason||p.code||"Penalty")} <span class="s-bad">(${Number(p.points||0).toFixed(0)})</span></div>`).join("")}</div>`:""}<div class="mcp-grid">${order.map((k)=>renderModule(k,mods[k]||{})).join("")}</div>${renderAiCard(compass)}<div class="mcp-source">Source: ${escapeHtml(compass?.data_sources?.market_prices||"YahooChart")} / ${escapeHtml(compass?.data_sources?.macro_credit||"FRED")}. ${Array.isArray(compass.fred_errors)&&compass.fred_errors.length?`FRED warnings: ${escapeHtml(compass.fred_errors.slice(0,2).join(" | "))}`:""}</div></div>`;
  }

  async function render(force=false){
    injectStyle();
    const data=await fetchSupabaseData(force);
    renderQuantPanel(data);
    renderCompassPanel(data);
  }

  function boot(){let tries=0;const timer=setInterval(()=>{tries+=1;render(false);if(tries>=18&&(document.querySelector(".holdings-terminal-view")||document.querySelector(".analytics-command-toolbar")))clearInterval(timer);},800);document.addEventListener("click",()=>setTimeout(()=>render(false),250),true);document.addEventListener("change",()=>setTimeout(()=>render(true),300),true);setInterval(()=>render(true),120000);}
  document.readyState==="loading"?document.addEventListener("DOMContentLoaded",boot):boot();
})();
