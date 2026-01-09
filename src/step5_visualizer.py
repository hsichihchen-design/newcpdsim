import pandas as pd
import json
import os
import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')
LOG_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_HTML = os.path.join(LOG_DIR, 'dashboard_report.html')

def load_map_robust(filename):
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
        grid = df.fillna(0).values.tolist()
        for r in range(len(grid)):
            for c in range(len(grid[0])):
                if grid[r][c] == -1: grid[r][c] = 1
        return grid
    return []

def load_shelf_map():
    path = os.path.join(MAPPING_DIR, 'shelf_coordinate_map.csv')
    shelf_set = {'2F': set(), '3F': set()}
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            for _, r in df.iterrows():
                shelf_set[r['floor']].add((int(r['x']), int(r['y'])))
        except: pass
    return shelf_set

def main():
    print("🚀 [Step 5] 啟動視覺化 (V14: Variable Fix)...")

    map_2f = load_map_robust('2F_map.xlsx')
    map_3f = load_map_robust('3F_map.xlsx')
    shelf_data = load_shelf_map()
    
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): return
    df_events = pd.read_csv(events_path)
    
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], errors='coerce')
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], errors='coerce')
    df_events = df_events.dropna(subset=['start_ts', 'end_ts'])
    
    df_events = df_events[
        (df_events['start_ts'].dt.year >= 2024) & 
        (df_events['end_ts'].dt.year <= 2030)
    ]
    
    df_events['start_ts'] = df_events['start_ts'].astype('int64') // 10**9
    df_events['end_ts'] = df_events['end_ts'].astype('int64') // 10**9
    df_events = df_events.sort_values('start_ts')
    df_events['text'] = df_events['text'].fillna('').astype(str)
    
    if df_events.empty:
        print("❌ 錯誤：無有效事件資料。")
        return

    min_time = int(df_events['start_ts'].min())
    max_time = int(df_events['end_ts'].max())
    print(f"   📅 模擬時間範圍 (Timestamp): {min_time} ~ {max_time}")

    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].values.tolist()
    
    all_agvs = df_events[df_events['type']=='AGV_MOVE']['obj_id'].unique().tolist()
    all_stations = df_events[df_events['obj_id'].str.startswith('WS_')]['obj_id'].unique().tolist()
    try: all_stations.sort(key=lambda x: int(x.split('_')[1]))
    except: pass

    # KPI
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    kpi_raw = []
    
    wave_deadlines = {} 
    wave_totals_simple = {} 
    recv_totals_simple = {} 

    try:
        df_kpi = pd.read_csv(kpi_path)
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], errors='coerce')
        df_kpi = df_kpi.dropna(subset=['finish_ts'])
        df_kpi['date'] = df_kpi['finish_ts'].dt.strftime('%Y-%m-%d')
        df_kpi['finish_ts'] = df_kpi['finish_ts'].astype('int64') // 10**9
        df_kpi = df_kpi.sort_values('finish_ts')
        
        if 'total_in_wave' not in df_kpi.columns: df_kpi['total_in_wave'] = 0
        if 'deadline_ts' not in df_kpi.columns: df_kpi['deadline_ts'] = 0
        
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave', 'deadline_ts']].values.tolist()
        
        for _, row in df_kpi.iterrows():
            total = int(row['total_in_wave'])
            wid = str(row['wave_id'])
            deadline = int(row['deadline_ts'])
            
            if deadline > 0: wave_deadlines[wid] = deadline
            
            if row['type'] == 'RECEIVING':
                d = row['date']
                if total > recv_totals_simple.get(d, 0): recv_totals_simple[d] = total
            else:
                if total > wave_totals_simple.get(wid, 0): wave_totals_simple[wid] = total
                
    except Exception as e:
        print(f"⚠️ KPI Error: {e}")

    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor V14</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; background: #eef1f5; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; overflow: hidden; }
        .floor-container { flex: 1; background: #fff; border: 1px solid #ccc; position: relative; display: flex; flex-direction: column; overflow: hidden; }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.9); padding: 2px 6px; font-weight: bold; font-size: 12px; z-index: 10; border: 1px solid #999; }
        .canvas-wrap { flex: 1; width: 100%; height: 100%; position: relative; }
        canvas { display: block; width: 100%; height: 100%; }
        .dash-section { flex: 1; min-width: 400px; max-width: 500px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 8px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        .station-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(90px, 1fr)); gap: 5px; }
        .station-card { border: 1px solid #ddd; padding: 5px; font-size: 10px; text-align: center; background: #fff; border-radius: 3px; display: flex; flex-direction: column; justify-content: center; }
        .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 3px; }
        .st-wave { font-size: 9px; color: #666; margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .delay-badge { background: #dc3545; color: white; padding: 1px 3px; border-radius: 3px; font-size: 8px; font-weight: bold; }
        .wave-item { font-size:11px; margin-bottom:5px; background:#fff; padding:5px; border:1px solid #ddd; }
        .progress-bg { height:6px; background:#eee; margin-top:2px; border-radius:3px; overflow:hidden; }
        .progress-fill { height:100%; transition:width 0.3s; }
        .warn-tag { color: white; background: #dc3545; padding: 1px 4px; border-radius: 3px; font-size: 9px; margin-left: 5px; }
        .warn-text { color: #dc3545; font-weight:bold; font-size:9px; margin-left:5px; }
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; }
        .legend { display: flex; gap: 10px; font-size: 11px; margin-bottom: 5px; }
        .box { width: 12px; height: 12px; margin-right: 3px; border: 1px solid #666; }
        .floor-title { font-size: 11px; font-weight: bold; margin: 5px 0 2px 0; color: #555; }
        .stats-row { display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲戰情室 (V14)</h3>
        <div style="flex:1"></div>
        <span id="timeDisplay" style="font-weight: bold;">--</span>
    </div>
    <div class="main">
        <div class="map-section">
            <div class="legend">
                <div style="display:flex;align-items:center"><div class="box" style="background:blue"></div>出貨</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:green"></div>進貨</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:#8d6e63"></div>料架</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:#ccc"></div>牆壁</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:white"></div>空地</div>
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
                    <div class="floor-title">2F</div>
                    <div id="st-list-2f" class="station-grid">Wait...</div>
                    <div class="floor-title" style="margin-top:10px">3F</div>
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
                    <div class="stats-row"><span>Active AGV:</span> <span id="val-active">0</span></div>
                    <div class="stats-row"><span>Done Tasks:</span> <span id="val-done">0</span></div>
                    <div style="margin-top:5px; border-top:1px dashed #ccc; padding-top:5px;">
                        <div class="stats-row" style="color:#dc3545"><span>Total Outbound Delay:</span> <span id="val-delay-out">0</span></div>
                        <div class="stats-row" style="color:#dc3545"><span>Total Inbound Delay:</span> <span id="val-delay-in">0</span></div>
                    </div>
                </div>
            </div>
            <div class="controls">
                <button onclick="togglePlay()" id="playBtn">Play</button>
                <input type="range" id="slider" min="__MIN_TIME__" max="__MAX_TIME__" value="__MIN_TIME__" style="flex:1">
                <select id="speed">
                    <option value="10">10s/s</option>
                    <option value="30">30s/s</option>
                    <option value="60">1min/s</option>
                    <option value="300" selected>5min/s</option>
                    <option value="600">10min/s</option>
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
    
    const serverRecvTotals = __RECV_TOTALS__;
    const serverWaveTotals = __WAVE_TOTALS__;
    const serverWaveDeadlines = __WAVE_DEADLINES__;
    
    let minTime = parseInt(__MIN_TIME__);
    let maxTime = parseInt(__MAX_TIME__);
    
    if (isNaN(minTime) || minTime < 1600000000) minTime = Math.floor(Date.now()/1000);
    if (isNaN(maxTime) || maxTime <= minTime) maxTime = minTime + 3600;

    const shelfSets = { '2F': new Set(shelfData['2F']), '3F': new Set(shelfData['3F']) };
    const agvColors = {};
    const colorPalette = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe', '#008080', '#e6beff', '#9a6324', '#fffac8', '#800000', '#aaffc3', '#808000', '#ffd8b1', '#000075', '#808080', '#ffffff', '#000000'];
    agvIds.forEach((id, idx) => { agvColors[id] = colorPalette[idx % colorPalette.length]; });

    let agvState = {};
    agvIds.forEach(id => { agvState[id] = { floor: '2F', x: -1, y: -1, visible: false }; });
    let stState = {};
    stIds.forEach(id => { 
        const num = parseInt(id.replace('WS_',''));
        const f = num >= 100 ? '3F' : '2F';
        stState[id] = { status: 'IDLE', color: 'WHITE', floor: f, x:-1, y:-1, wave:'-', delay: false }; 
    });

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
                    ctx.fillStyle = shelfSets[floorName].has(key) ? '#8d6e63' : '#ccc';
                    ctx.fillRect(x,y,s,s); 
                } 
                else if(val==2) { ctx.strokeStyle='#bbb'; ctx.strokeRect(x,y,s,s); } 
                else if(val==3) { ctx.fillStyle='#e0f7fa'; ctx.fillRect(x,y,s,s); } 
            }
        }
        Object.keys(stState).forEach(sid => {
            const s = stState[sid];
            if(s.floor === floorName && s.x !== -1) {
                const x=obj.ox+s.x*obj.size, y=obj.oy+s.y*obj.size, sz=obj.size;
                ctx.fillStyle = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : s.color === 'ORANGE' ? '#fd7e14' : 'rgba(255,255,255,0.7)';
                ctx.fillRect(x+1, y+1, sz-2, sz-2);
                ctx.fillStyle = 'black'; ctx.font = 'bold 8px Arial';
                ctx.fillText(sid.replace('WS_',''), x+2, y+sz/1.5);
                if(s.delay) { ctx.fillStyle = 'red'; ctx.beginPath(); ctx.arc(x+sz-3, y+3, sz/5, 0, Math.PI*2); ctx.fill(); }
            }
        });
    }

    let currTime = minTime;
    let isPlaying = false;
    let lastFrameTime = 0;

    const slider = document.getElementById('slider');
    slider.min = minTime;
    slider.max = maxTime;
    slider.value = minTime;

    function updateState(time) {
        agvIds.forEach(id => {
            let activeEvt = null, lastEvt = null;
            for(let i=events.length-1; i>=0; i--) {
                const e = events[i];
                if(e[3] === id && e[8] === 'AGV_MOVE') {
                    if (e[0] <= time && time <= e[1]) { activeEvt = e; break; }
                    if (e[1] < time && !lastEvt) { lastEvt = e; }
                }
            }
            if (activeEvt) {
                const p = (time - activeEvt[0]) / (activeEvt[1] - activeEvt[0]);
                agvState[id] = { floor: activeEvt[2], x: activeEvt[4]+(activeEvt[6]-activeEvt[4])*p, y: activeEvt[5]+(activeEvt[7]-activeEvt[5])*p, visible: true };
            } else if (lastEvt) {
                agvState[id] = { floor: lastEvt[2], x: lastEvt[6], y: lastEvt[7], visible: true };
            }
        });
        Object.keys(stState).forEach(sid => { stState[sid].status = 'IDLE'; stState[sid].color = 'WHITE'; stState[sid].wave = '-'; stState[sid].delay = false; });
        for(let i=0; i<events.length; i++) {
            const e = events[i];
            if (e[8] === 'STATION_STATUS') {
                if (e[0] > time) break; 
                if (e[1] >= time) {
                    const sid = e[3];
                    if(stState[sid]) {
                        const parts = e[9].split('|');
                        stState[sid].status = parts[0] === 'BLUE' ? 'OUT' : (parts[0] === 'GREEN' ? 'IN' : 'REP');
                        stState[sid].color = parts[0];
                        stState[sid].floor = e[2];
                        stState[sid].x = e[4]; stState[sid].y = e[5];
                        stState[sid].wave = parts[1];
                        stState[sid].delay = parts[2] === 'True' || parts[2] === 'Y';
                    }
                }
            }
        }
    }

    function getLocalYMD(ts) {
        const d = new Date(ts * 1000);
        const y = d.getFullYear();
        const m = String(d.getMonth()+1).padStart(2,'0');
        const day = String(d.getDate()).padStart(2,'0');
        return `${y}-${m}-${day}`;
    }

    function render() {
        updateState(currTime);
        drawMap(f2, '2F');
        drawMap(f3, '3F');
        
        let activeCount = 0;
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            if(!obj.map) return;
            const sz = obj.size;
            const px = obj.ox + s.x * sz + sz/2;
            const py = obj.oy + s.y * sz + sz/2;
            obj.ctx.fillStyle = agvColors[id];
            obj.ctx.beginPath(); obj.ctx.arc(px, py, sz/2.2, 0, Math.PI*2); obj.ctx.fill();
            activeCount++;
        });
        document.getElementById('val-active').innerText = activeCount;
        
        let html2 = '', html3 = '';
        stIds.forEach(sid => {
            const s = stState[sid];
            const color = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : s.color === 'ORANGE' ? '#fd7e14' : 'rgba(255,255,255,0.7)';
            const delayHtml = s.delay ? '<div class="delay-badge">DELAY</div>' : '';
            const card = `<div class="station-card"><div style="font-weight:bold;display:flex;justify-content:space-between;width:100%">${sid.replace('WS_','')} ${delayHtml}</div><div style="margin-top:2px"><span class="status-dot" style="background:${color}"></span>${s.status}</div><div class="st-wave" title="${s.wave}">${s.wave}</div></div>`;
            if (s.floor === '2F') html2 += card; else html3 += card;
        });
        document.getElementById('st-list-2f').innerHTML = html2 || 'No Data';
        document.getElementById('st-list-3f').innerHTML = html3 || 'No Data';

        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-done').innerText = doneTasks.length;
        document.getElementById('timeDisplay').innerText = new Date(currTime*1000).toLocaleString();
        document.getElementById('slider').value = currTime;
        
        // Stats
        let delayIn = 0;
        let delayOut = 0;
        const doneRecv = {};
        const doneByWave = {};

        doneTasks.forEach(k => {
            const wid = k[2], type = k[1], isD = k[3], date = k[4];
            if(type === 'RECEIVING') {
                doneRecv[date] = (doneRecv[date] || 0) + 1;
                if(isD === 'Y') delayIn++;
            } else {
                doneByWave[wid] = (doneByWave[wid] || 0) + 1;
                if(isD === 'Y') delayOut++;
            }
        });
        
        document.getElementById('val-delay-in').innerText = delayIn;
        document.getElementById('val-delay-out').innerText = delayOut;
        
        const waveInfoLive = {}, recvInfoLive = {};
        for(let d in serverRecvTotals) recvInfoLive[d] = {total: serverRecvTotals[d]};
        for(let w in serverWaveTotals) waveInfoLive[w] = {total: serverWaveTotals[w]};
        
        kpiRaw.forEach(k => {
            const wid = k[2], type = k[1], date = k[4], total = k[6];
            if(type === 'RECEIVING') {
                if(!recvInfoLive[date]) recvInfoLive[date] = {total: 0};
                if(total > recvInfoLive[date].total) recvInfoLive[date].total = total;
            } else {
                if(!waveInfoLive[wid]) waveInfoLive[wid] = {total: 0};
                if(total > waveInfoLive[wid].total) waveInfoLive[wid].total = total;
            }
        });

        let wHtml = '';
        Object.keys(waveInfoLive).sort().forEach(wid => {
            const info = waveInfoLive[wid];
            const done = doneByWave[wid] || 0;
            const total = info.total || 1;
            
            // Get Deadline & Delay Status
            const deadline = serverWaveDeadlines[wid] || 0;
            const isLateNow = (deadline > 0 && currTime > deadline && done < total);
            
            let delayTxt = '';
            if (isLateNow) {
                const diffMins = Math.floor((currTime - deadline) / 60);
                delayTxt = `<span class="warn-text">Delay ${diffMins}m</span>`;
            }
            
            if (total > 0) {
                const isDone = done >= total;
                const pct = (done/total*100).toFixed(0);
                const barColor = isLateNow ? '#dc3545' : (isDone ? '#28a745' : '#007bff');
                const warn = isLateNow ? '<span class="warn-tag">DELAY</span>' : '';
                wHtml += `<div class="wave-item"><div style="display:flex;justify-content:space-between"><span>${wid} ${warn} ${delayTxt}</span><span>${done}/${total}</span></div><div class="progress-bg"><div class="progress-fill" style="width:${pct}%;background:${barColor}"></div></div></div>`;
            }
        });
        document.getElementById('wave-list').innerHTML = wHtml || '<div style="color:#999;padding:5px">Waiting...</div>';
        
        let rHtml = '';
        const todayStr = getLocalYMD(currTime);
        if (recvInfoLive[todayStr]) {
            const info = recvInfoLive[todayStr];
            const done = doneRecv[todayStr] || 0;
            const pct = info.total > 0 ? (done/info.total*100).toFixed(0) : 0;
            rHtml = `<div class="wave-item"><div style="display:flex;justify-content:space-between"><span>📅 ${todayStr}</span><span>${done}/${info.total}</span></div><div class="progress-bg"><div class="progress-fill" style="width:${pct}%;background:#28a745"></div></div></div>`;
        } else {
            rHtml = `<div style="color:#999;padding:5px">No receiving orders on ${todayStr}</div>`;
        }
        document.getElementById('recv-list').innerHTML = rHtml;
    }

    function animate() {
        requestAnimationFrame(animate);
        if(!isPlaying) { lastFrameTime = performance.now(); return; }
        const now = performance.now();
        const dt = (now - lastFrameTime) / 1000;
        lastFrameTime = now;
        
        let speed = parseInt(document.getElementById('speed').value);
        if(isNaN(speed)) speed = 10;
        
        currTime += dt * speed; 
        if(isNaN(currTime) || currTime > maxTime) { currTime = minTime; isPlaying = false; }
        render();
    }
    
    function togglePlay() { isPlaying=!isPlaying; if(isPlaying) lastFrameTime = performance.now(); }
    document.getElementById('slider').addEventListener('input', e=>{ currTime=parseInt(e.target.value); render(); });
    
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
                              .replace('__RECV_TOTALS__', json.dumps(recv_totals_simple)) \
                              .replace('__WAVE_TOTALS__', json.dumps(wave_totals_simple)) \
                              .replace('__WAVE_DEADLINES__', json.dumps(wave_deadlines))

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"✅ 視覺化生成完畢: {OUTPUT_HTML}")

if __name__ == "__main__":
    main()