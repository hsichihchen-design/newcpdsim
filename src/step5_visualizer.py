import pandas as pd
import json
import os
import numpy as np

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')
LOG_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_HTML = os.path.join(LOG_DIR, 'dashboard_report.html')
# ----------------------------------------

def load_map_fixed(filename, rows_limit, cols_limit):
    path = os.path.join(DATA_MAP_DIR, filename)
    df = None
    if os.path.exists(path):
        try: df = pd.read_excel(path, header=None)
        except: pass
    if df is None:
        csv_path = path.replace('.xlsx', '.csv')
        if os.path.exists(csv_path):
            try: df = pd.read_csv(csv_path, header=None)
            except: pass
    
    if df is not None:
        df = df.iloc[0:rows_limit, 0:cols_limit]
        grid = df.fillna(0).values.tolist()
        return grid
    return []

def load_shelf_map():
    path = os.path.join(MAPPING_DIR, 'shelf_coordinate_map.csv')
    shelf_set = {'2F': set(), '3F': set()}
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            for _, r in df.iterrows():
                if 0 <= r['x'] < 61 and 0 <= r['y'] < 32:
                    shelf_set[r['floor']].add((int(r['x']), int(r['y'])))
        except: pass
    return shelf_set

def main():
    print("🚀 [Step 5] 啟動視覺化 (V39: UI Split & Data Logic Fix)...")

    map_2f = load_map_fixed('2F_map.xlsx', 32, 61)
    map_3f = load_map_fixed('3F_map.xlsx', 32, 61)
    shelf_data = load_shelf_map()
    
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): 
        print("❌ 找不到 simulation_events.csv")
        return

    try:
        df_events = pd.read_csv(events_path, on_bad_lines='skip', engine='python')
    except Exception as e:
        print(f"❌ Error reading events: {e}")
        return
    
    # 日期過濾與解析
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], errors='coerce')
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], errors='coerce')
    df_events = df_events.dropna(subset=['start_ts', 'end_ts'])
    df_events = df_events[df_events['start_ts'].dt.year > 2020]
    
    if df_events.empty: 
        print("⚠️ 無有效事件資料")
        return

    df_events['start_ts'] = df_events['start_ts'].astype('int64') // 10**9
    df_events['end_ts'] = df_events['end_ts'].astype('int64') // 10**9
    df_events = df_events.sort_values('start_ts')
    df_events['text'] = df_events['text'].fillna('').astype(str)
    
    min_time = int(df_events['start_ts'].min())
    max_time = int(df_events['end_ts'].max())
    print(f"   📅 時間範圍: {pd.to_datetime(min_time, unit='s')} ~ {pd.to_datetime(max_time, unit='s')}")

    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].values.tolist()
    
    # 抓取 AGV ID (只保留數字部分，例如 '16')
    all_agvs = sorted(list(df_events[df_events['obj_id'].str.contains('AGV')]['obj_id'].unique()))
    
    # 抓取 Station ID
    all_stations = df_events[df_events['obj_id'].str.startswith('WS_')]['obj_id'].unique().tolist()
    try: all_stations.sort(key=lambda x: int(x.split('_')[1]))
    except: pass

    # --- KPI Processing (Auto-Count Logic) ---
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    kpi_raw = []
    
    # 自動統計總量，修復 Step 4 輸出為 0 的問題
    calc_wave_totals = {}
    calc_recv_totals = {}

    try:
        df_kpi = pd.read_csv(kpi_path, on_bad_lines='skip', engine='python')
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], errors='coerce')
        df_kpi = df_kpi.dropna(subset=['finish_ts'])
        df_kpi = df_kpi[df_kpi['finish_ts'].dt.year > 2020]
        
        df_kpi['date'] = df_kpi['finish_ts'].dt.strftime('%Y-%m-%d')
        df_kpi['finish_ts'] = df_kpi['finish_ts'].astype('int64') // 10**9
        df_kpi = df_kpi.sort_values('finish_ts')
        
        # 統計邏輯
        for _, row in df_kpi.iterrows():
            wid = str(row['wave_id'])
            if row['type'] == 'RECEIVING':
                d = row['date']
                calc_recv_totals[d] = calc_recv_totals.get(d, 0) + 1
            else:
                calc_wave_totals[wid] = calc_wave_totals.get(wid, 0) + 1
                
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave', 'deadline_ts']].values.tolist()

    except Exception as e: 
        print(f"⚠️ KPI Error: {e}")

    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor V39</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; background: #eef1f5; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; overflow: hidden; }
        .floor-container { flex: 1; background: #fff; border: 1px solid #ccc; position: relative; display: flex; flex-direction: column; overflow: hidden; }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.9); padding: 2px 6px; font-weight: bold; font-size: 12px; z-index: 10; border: 1px solid #999; }
        .canvas-wrap { flex: 1; width: 100%; height: 100%; position: relative; }
        canvas { display: block; width: 100%; height: 100%; }
        .dash-section { flex: 1; min-width: 400px; max-width: 480px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 8px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        .station-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 5px; }
        .station-card { border: 1px solid #ddd; padding: 4px; font-size: 10px; text-align: center; background: #fff; border-radius: 3px; display: flex; flex-direction: column; justify-content: center; height: 35px; }
        .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 3px; }
        .wave-item { font-size:11px; margin-bottom:5px; background:#fff; padding:5px; border:1px solid #ddd; }
        .progress-bg { height:6px; background:#eee; margin-top:2px; border-radius:3px; overflow:hidden; }
        .progress-fill { height:100%; transition:width 0.3s; }
        .warn-text { color: #dc3545; font-weight:bold; margin-left: 5px; font-size: 10px; }
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; }
        .legend { display: flex; gap: 10px; font-size: 11px; margin-bottom: 5px; flex-wrap: wrap; }
        .box { width: 12px; height: 12px; margin-right: 3px; border: 1px solid #666; }
        .floor-subtitle { font-size: 12px; font-weight: bold; color: #555; margin: 5px 0 2px 0; border-bottom: 1px dashed #ccc; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲戰情室 (V39: UI Split & Fix)</h3>
        <div style="flex:1"></div>
        <span id="timeDisplay" style="font-weight: bold;">--</span>
    </div>
    <div class="main">
        <div class="map-section">
            <div class="legend">
                <div style="display:flex;align-items:center"><div class="box" style="background:#8d6e63"></div>料架</div>
                <div style="display:flex;align-items:center"><div class="box" style="border-radius:50%;background:#00e5ff;border:1px solid #000"></div>AGV(空)</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:#d500f9;border:1px solid #fff"></div>AGV(載貨)</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:orange"></div>讓路</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:purple"></div>移庫</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:red"></div>瞬移</div>
            </div>
            <div class="floor-container">
                <div class="floor-label">2F Map</div>
                <div class="canvas-wrap"><canvas id="c2"></canvas></div>
            </div>
            <div class="floor-container">
                <div class="floor-label">3F Map</div>
                <div class="canvas-wrap"><canvas id="c3"></canvas></div>
            </div>
        </div>
        <div class="dash-section">
            <div class="dash-content">
                <div class="panel">
                    <h4>📡 工作站狀態</h4>
                    <div class="floor-subtitle">2F Stations</div>
                    <div id="st-list-2f" class="station-grid">Wait...</div>
                    <div class="floor-subtitle" style="margin-top:10px">3F Stations</div>
                    <div id="st-list-3f" class="station-grid">Wait...</div>
                </div>
                <div class="panel">
                    <h4>🌊 波次進度 (Outbound)</h4>
                    <div id="wave-list">Wait...</div>
                </div>
                <div class="panel">
                    <h4>🚛 進貨進度 (Inbound)</h4>
                    <div id="recv-list">Wait...</div>
                </div>
                <div class="panel">
                    <h4>📊 統計指標</h4>
                    <div>Active AGV: <span id="val-active">0</span></div>
                    <div>Done: <span id="val-done">0</span> | Delay: <span id="val-delay" style="color:red">0</span></div>
                </div>
            </div>
            <div class="controls">
                <button onclick="togglePlay()" id="playBtn">Play</button>
                <input type="range" id="slider" style="flex:1">
                <select id="speed">
                    <option value="5">5s/s (Slow)</option>
                    <option value="10" selected>10s/s (Normal)</option>
                    <option value="30">30s/s (Fast)</option>
                    <option value="60">1m/s</option>
                    <option value="300">5m/s</option>
                </select>
            </div>
        </div>
    </div>
<script>
    const map2F = __MAP2F__;
    const map3F = __MAP3F__;
    const shelfData = __SHELF_DATA__; 
    const events = __EVENTS__;
    const kpiRaw = __KPI_RAW__;
    const agvIds = __AGV_IDS__;
    const stIds = __STATION_IDS__;
    
    // [V39] Calculated Totals
    const waveTotals = __WAVE_TOTALS__;
    const recvTotals = __RECV_TOTALS__;
    
    let minTime = Number(__MIN_TIME__);
    let maxTime = Number(__MAX_TIME__);
    if (isNaN(minTime)) minTime = Math.floor(Date.now()/1000);
    if (isNaN(maxTime)) maxTime = minTime + 3600;
    
    document.getElementById('slider').min = minTime;
    document.getElementById('slider').max = maxTime;
    document.getElementById('slider').value = minTime;

    const initialShelfSets = { '2F': new Set(shelfData['2F']), '3F': new Set(shelfData['3F']) };
    
    // [V39] Logic Fix: Loaded state persistence
    let agvState = {};
    agvIds.forEach(id => { agvState[id] = { floor: '2F', x: -1, y: -1, visible: false, color: '#00e5ff', loaded: false }; });
    let tempObjects = []; 
    let stState = {};
    stIds.forEach(id => { 
        const num = parseInt(id.replace('WS_',''));
        const f = num >= 100 ? '3F' : '2F';
        stState[id] = { status: 'IDLE', color: 'WHITE', floor: f, x:-1, y:-1, wave:'-' }; 
    });

    let currentShelves = { '2F': new Set(), '3F': new Set() };

    function setupCanvas(id, mapData) {
        const c = document.getElementById(id);
        const ctx = c.getContext('2d');
        const parent = c.parentElement;
        c.width = parent.clientWidth || 800; c.height = parent.clientHeight || 400;
        const rows = mapData.length || 10;
        const cols = mapData[0]?.length || 10;
        const scaleX = c.width / cols;
        const scaleY = c.height / rows;
        const size = Math.min(scaleX, scaleY);
        const ox = (c.width - cols*size)/2;
        const oy = (c.height - rows*size)/2;
        return { ctx, rows, cols, size, ox, oy, map: mapData };
    }
    
    let f2 = setupCanvas('c2', map2F);
    let f3 = setupCanvas('c3', map3F);
    window.onresize = () => { f2 = setupCanvas('c2', map2F); f3 = setupCanvas('c3', map3F); render(); };

    function drawMap(obj, floorName) {
        const ctx = obj.ctx;
        ctx.fillStyle = '#fafafa'; ctx.fillRect(0,0, ctx.canvas.width, ctx.canvas.height);
        if(!obj.map) return;
        
        for(let r=0; r<obj.rows; r++) {
            for(let c=0; c<obj.cols; c++) {
                const val = obj.map[r][c];
                const x = obj.ox + c * obj.size;
                const y = obj.oy + r * obj.size;
                const s = obj.size;
                if(val==1) { 
                    const key = c + "," + r;
                    ctx.fillStyle = currentShelves[floorName].has(key) ? '#8d6e63' : '#eee';
                    ctx.fillRect(x,y,s,s); 
                } 
                else if(val==-1) { ctx.fillStyle = '#ccc'; ctx.fillRect(x,y,s,s); } 
                else if(val==2) { ctx.strokeStyle='#bbb'; ctx.strokeRect(x,y,s,s); } 
                else { ctx.fillStyle = 'white'; ctx.fillRect(x,y,s,s); }
            }
        }
        
        Object.keys(stState).forEach(sid => {
            const s = stState[sid];
            if(s.floor === floorName && s.x !== -1) {
                const x=obj.ox+s.x*obj.size, y=obj.oy+s.y*obj.size, sz=obj.size;
                ctx.fillStyle = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : '#ddd';
                ctx.fillRect(x+1, y+1, sz-2, sz-2);
                ctx.fillStyle = 'black'; ctx.font = 'bold 8px Arial';
                ctx.fillText(sid.replace('WS_',''), x+2, y+sz/1.5);
            }
        });
    }

    let currTime = minTime;
    let isPlaying = false;
    let lastFrameTime = 0;

    const slider = document.getElementById('slider');

    function updateState(time) {
        currentShelves['2F'] = new Set(initialShelfSets['2F']);
        currentShelves['3F'] = new Set(initialShelfSets['3F']);
        tempObjects = [];
        
        for(let i=0; i<events.length; i++) {
            const e = events[i];
            if (e[0] > time) break; 
            if (e[8] === 'SHELF_LOAD' || e[8] === 'SHUFFLE') { 
                const key = e[4] + "," + e[5];
                currentShelves[e[2]].delete(key);
            } 
            if (e[8] === 'SHELF_UNLOAD' || e[8] === 'SHUFFLE') { 
                if(e[8] === 'SHUFFLE' && e[1] <= time) {
                    const key = e[6] + "," + e[7];
                    currentShelves[e[2]].add(key);
                } else if(e[8] === 'SHELF_UNLOAD') {
                    const key = e[4] + "," + e[5];
                    currentShelves[e[2]].add(key);
                }
            }
        }

        agvIds.forEach(id => { agvState[id].visible = false; agvState[id].loaded = false; });

        for(let i=events.length-1; i>=0; i--) {
            const e = events[i];
            
            // [V39 Fix] Improved Loaded State Logic
            // If an event is 'SHELF_LOAD' and happened before now, AGV is loaded
            // UNLESS a subsequent 'SHELF_UNLOAD' also happened before now
            if (e[3].startsWith('AGV') && e[0] <= time) {
                const id = e[3];
                if (e[8] === 'SHELF_LOAD') {
                    // Check if there is a corresponding UNLOAD in the future (relative to event, but before now)
                    let hasUnloaded = false;
                    for(let k=i+1; k<events.length; k++) {
                        const nextE = events[k];
                        if (nextE[0] > time) break; // Future event relative to NOW
                        if (nextE[3] === id && nextE[8] === 'SHELF_UNLOAD') {
                            hasUnloaded = true;
                            break;
                        }
                    }
                    if (!hasUnloaded) agvState[id].loaded = true;
                }
            }

            if (e[0] <= time && e[1] >= time) {
                const p = (time - e[0]) / (e[1] - e[0]);
                const curX = e[4]+(e[6]-e[4])*p;
                const curY = e[5]+(e[7]-e[5])*p;
                
                if (e[3].startsWith('AGV')) {
                    const id = e[3];
                    if (agvState[id]) {
                        agvState[id].floor = e[2];
                        agvState[id].x = curX; agvState[id].y = curY;
                        agvState[id].visible = true;
                        
                        if (e[8] === 'NUDGE' || e[8] === 'YIELD') agvState[id].color = 'orange';
                        else if (e[8] === 'PARKING') agvState[id].color = 'green';
                        else if (e[8].includes('TELE') || e[8] === 'FORCE_TELE') agvState[id].color = 'red';
                        else {
                            agvState[id].color = agvState[id].loaded ? '#d500f9' : '#00e5ff';
                        }
                    }
                } else if (e[8] === 'SHUFFLE') {
                    tempObjects.push({ floor: e[2], x: curX, y: curY, color: 'purple' });
                }
            }
        }
        
        Object.keys(stState).forEach(sid => { stState[sid].status = 'IDLE'; stState[sid].color = 'WHITE'; });
        for(let i=0; i<events.length; i++) {
            const e = events[i];
            if (e[8] === 'STATION_STATUS') {
                if (e[0] > time) break;
                if (e[1] >= time) {
                    const sid = e[3];
                    if(stState[sid]) {
                        const parts = e[9].split('|');
                        stState[sid].color = parts[0];
                        stState[sid].wave = parts[1];
                        stState[sid].status = parts[0] === 'BLUE' ? 'WORK' : 'IDLE';
                    }
                }
            }
        }
    }

    function render() {
        updateState(currTime);
        drawMap(f2, '2F');
        drawMap(f3, '3F');
        
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            if(!obj.map) return;
            const sz = obj.size;
            const px = obj.ox + s.x * sz + sz/2;
            const py = obj.oy + s.y * sz + sz/2;
            
            obj.ctx.fillStyle = s.color;
            obj.ctx.beginPath();
            obj.ctx.arc(px, py, sz/2.1, 0, Math.PI*2);
            obj.ctx.fill();
            obj.ctx.strokeStyle = '#333';
            obj.ctx.lineWidth = 1;
            obj.ctx.stroke();
            
            if (s.loaded && !['red','orange','green'].includes(s.color)) {
                obj.ctx.fillStyle = '#fff';
                obj.ctx.fillRect(px-sz/4, py-sz/4, sz/2, sz/2);
            }

            if (sz > 8) {
                obj.ctx.fillStyle = 'black'; obj.ctx.font = 'bold 10px Arial';
                obj.ctx.textAlign = 'center';
                // [V39 Fix] Only number
                obj.ctx.fillText(id.replace('AGV_',''), px, py+4);
            }
        });
        
        tempObjects.forEach(o => {
            const obj = o.floor == '2F' ? f2 : f3;
            if(!obj.map) return;
            const sz = obj.size;
            const px = obj.ox + o.x * sz;
            const py = obj.oy + o.y * sz;
            obj.ctx.fillStyle = o.color;
            obj.ctx.fillRect(px+2, py+2, sz-4, sz-4);
        });
        
        // Draw Station Grid UI (Split)
        let h2 = '', h3 = '';
        stIds.forEach(sid => {
            const s = stState[sid];
            const color = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : '#ddd';
            const card = `<div class="station-card"><div style="font-weight:bold">${sid.replace('WS_','')}</div><div style="margin-top:2px"><span class="status-dot" style="background:${color}"></span>${s.wave}</div></div>`;
            if (s.floor === '2F') h2 += card; else h3 += card;
        });
        document.getElementById('st-list-2f').innerHTML = h2 || 'No Data';
        document.getElementById('st-list-3f').innerHTML = h3 || 'No Data';

        // KPI Dashboard
        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-done').innerText = doneTasks.length;
        
        const dObj = new Date(currTime*1000);
        document.getElementById('timeDisplay').innerText = dObj.toLocaleString();
        document.getElementById('slider').value = currTime;
        
        const waveProgress = {};
        const recvProgress = {};
        let totalDelay = 0;

        doneTasks.forEach(k => {
            if(k[1]=='RECEIVING') recvProgress[k[4]] = (recvProgress[k[4]]||0)+1;
            else {
                waveProgress[k[2]] = (waveProgress[k[2]]||0)+1;
                if(k[3]=='Y') totalDelay++;
            }
        });
        document.getElementById('val-delay').innerText = totalDelay;

        // Render Wave List
        let wHtml = '';
        const activeWaves = Object.keys(waveTotals).sort(); 
        
        activeWaves.forEach(wid => {
            const total = waveTotals[wid];
            const done = waveProgress[wid] || 0;
            // Only show relevant
            if (done < total || (done >= total && doneTasks.some(k=>k[2]==wid && k[0] > currTime - 1800))) {
                const pct = Math.min(100, (done/total*100)).toFixed(0);
                wHtml += `<div class="wave-item"><div style="display:flex;justify-content:space-between"><span>${wid}</span><span>${done}/${total}</span></div><div class="progress-bg"><div class="progress-fill" style="width:${pct}%;background:#007bff"></div></div></div>`;
            }
        });
        document.getElementById('wave-list').innerHTML = wHtml || '<div style="color:#999;padding:5px">No Active Waves</div>';
        
        // Render Inbound List
        let rHtml = '';
        const todayStr = `${dObj.getFullYear()}-${String(dObj.getMonth()+1).padStart(2,'0')}-${String(dObj.getDate()).padStart(2,'0')}`;
        
        if (recvTotals[todayStr]) {
             const total = recvTotals[todayStr];
             const done = recvProgress[todayStr] || 0;
             const pct = total > 0 ? Math.min(100, (done/total*100)).toFixed(0) : 0;
             rHtml += `<div class="wave-item"><div style="display:flex;justify-content:space-between"><span>📅 ${todayStr} (Inbound)</span><span>${done}/${total}</span></div><div class="progress-bg"><div class="progress-fill" style="width:${pct}%;background:#28a745"></div></div></div>`;
        } else {
             rHtml = `<div style="color:#999;padding:5px">No Inbound Plan for ${todayStr}</div>`;
        }
        document.getElementById('recv-list').innerHTML = rHtml;
    }

    function animate() {
        if(isPlaying) {
            const speed = parseInt(document.getElementById('speed').value);
            currTime += (1/30) * speed; 
            if(currTime > maxTime) { isPlaying=false; currTime=minTime; }
            document.getElementById('slider').value = currTime;
            render();
        }
        requestAnimationFrame(animate);
    }
    
    function togglePlay() { isPlaying=!isPlaying; }
    document.getElementById('slider').addEventListener('input', e=>{ currTime=parseInt(e.target.value); render(); });
    
    render();
    animate();
    
</script>
</body>
</html>
"""
    
    js_shelf_data = {'2F': [], '3F': []}
    for f in shelf_data: js_shelf_data[f] = [f"{c[0]},{c[1]}" for c in shelf_data[f]]

    final_html = html_template.replace('__MAP2F__', json.dumps(map_2f)) \
                              .replace('__MAP3F__', json.dumps(map_3f)) \
                              .replace('__SHELF_DATA__', json.dumps(js_shelf_data)) \
                              .replace('__EVENTS__', json.dumps(events_data)) \
                              .replace('__KPI_RAW__', json.dumps(kpi_raw)) \
                              .replace('__AGV_IDS__', json.dumps(all_agvs)) \
                              .replace('__STATION_IDS__', json.dumps(all_stations)) \
                              .replace('__MIN_TIME__', str(min_time)) \
                              .replace('__MAX_TIME__', str(max_time)) \
                              .replace('__WAVE_TOTALS__', json.dumps(calc_wave_totals)) \
                              .replace('__RECV_TOTALS__', json.dumps(calc_recv_totals))

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"✅ 視覺化生成完畢: {OUTPUT_HTML} (V39 Auto-Count)")

if __name__ == "__main__":
    main()