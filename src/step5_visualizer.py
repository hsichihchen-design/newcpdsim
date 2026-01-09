import pandas as pd
import json
import os

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
    print("🚀 [Step 5] 啟動視覺化 (Real-Time Speed Control)...")

    map_2f = load_map_robust('2F_map.xlsx')
    map_3f = load_map_robust('3F_map.xlsx')
    shelf_data = load_shelf_map()
    
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): return
    df_events = pd.read_csv(events_path)
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], format='mixed').astype('int64') // 10**9
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], format='mixed').astype('int64') // 10**9
    df_events = df_events.sort_values('start_ts')
    df_events['text'] = df_events['text'].fillna('').astype(str)
    
    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].values.tolist()
    min_time = df_events['start_ts'].min() if not df_events.empty else 0
    max_time = df_events['end_ts'].max() if not df_events.empty else 1
    
    all_agvs = df_events[df_events['type']=='AGV_MOVE']['obj_id'].unique().tolist()
    all_stations = df_events[df_events['obj_id'].str.startswith('WS_')]['obj_id'].unique().tolist()
    try: all_stations.sort(key=lambda x: int(x.split('_')[1]))
    except: pass

    # KPI
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    kpi_raw = []
    wave_info = {} 
    recv_info = {} 
    try:
        df_kpi = pd.read_csv(kpi_path)
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').astype('int64') // 10**9
        df_kpi['date'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').dt.date.astype(str)
        if 'total_in_wave' not in df_kpi.columns: df_kpi['total_in_wave'] = 0
        
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation']].values.tolist()
        
        for _, row in df_kpi.iterrows():
            wid = str(row['wave_id'])
            # 區分 Receiving 與 Wave
            if 'RECEIVING' in wid:
                d = str(row['date'])
                if d not in recv_info: recv_info[d] = {'total': int(row['total_in_wave']), 'done': 0}
                if row['total_in_wave'] > recv_info[d]['total']: recv_info[d]['total'] = int(row['total_in_wave'])
                recv_info[d]['done'] += 1
            else:
                if wid not in wave_info:
                    wave_info[wid] = {'total': int(row['total_in_wave']), 'delayed': 0}
                if row['total_in_wave'] > wave_info[wid]['total']: 
                     wave_info[wid]['total'] = int(row['total_in_wave'])
                if row['is_delayed'] == 'Y': wave_info[wid]['delayed'] += 1
    except: pass

    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor V5</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; background: #eef1f5; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; overflow: hidden; }
        .floor-container { 
            flex: 1; background: #fff; border: 1px solid #ccc; position: relative; 
            display: flex; flex-direction: column; overflow: hidden; 
        }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.9); padding: 2px 6px; font-weight: bold; font-size: 12px; z-index: 10; border: 1px solid #999; }
        .canvas-wrap { flex: 1; width: 100%; height: 100%; position: relative; }
        canvas { display: block; width: 100%; height: 100%; }
        
        .dash-section { flex: 1; min-width: 400px; max-width: 500px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 8px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        
        .station-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(90px, 1fr)); gap: 5px; }
        .station-card { 
            border: 1px solid #ddd; padding: 5px; font-size: 10px; text-align: center; background: #fff; border-radius: 3px; 
            display: flex; flex-direction: column; justify-content: center;
        }
        .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 3px; }
        .st-wave { font-size: 9px; color: #666; margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .delay-badge { background: #dc3545; color: white; padding: 1px 3px; border-radius: 3px; font-size: 8px; font-weight: bold; }
        
        .wave-item { font-size:11px; margin-bottom:5px; background:#fff; padding:5px; border:1px solid #ddd; }
        .progress-bg { height:6px; background:#eee; margin-top:2px; border-radius:3px; overflow:hidden; }
        .progress-fill { height:100%; transition:width 0.3s; }
        .warn-tag { color: white; background: #dc3545; padding: 1px 4px; border-radius: 3px; font-size: 9px; margin-left: 5px; }
        
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; }
        .legend { display: flex; gap: 10px; font-size: 11px; margin-bottom: 5px; }
        .box { width: 12px; height: 12px; margin-right: 3px; border: 1px solid #666; }
        
        .floor-title { font-size: 11px; font-weight: bold; margin: 5px 0 2px 0; color: #555; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲戰情室 (Speed & Receiving Fixed)</h3>
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
                    <h4>🌊 波次進度</h4>
                    <div id="wave-list">Wait...</div>
                </div>
                
                <div class="panel">
                    <h4>🚛 進貨進度</h4>
                    <div id="recv-list">Wait...</div>
                </div>
                
                <div class="panel">
                    <h4>📊 即時指標</h4>
                    <div>Active AGV: <span id="val-active">0</span></div>
                    <div>Done Tasks: <span id="val-done">0</span></div>
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
    const waveInfo = __WAVE_INFO__;
    const recvInfo = __RECV_INFO__;
    const agvIds = __AGV_IDS__;
    const stIds = __STATION_IDS__;

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
                ctx.fillStyle = s.color === 'BLUE' ? '#007bff' : 
                                s.color === 'GREEN' ? '#28a745' : 
                                s.color === 'ORANGE' ? '#fd7e14' : 'rgba(255,255,255,0.7)';
                ctx.fillRect(x+1, y+1, sz-2, sz-2);
                ctx.fillStyle = 'black'; ctx.font = 'bold 8px Arial';
                ctx.fillText(sid.replace('WS_',''), x+2, y+sz/1.5);
                if(s.delay) { ctx.fillStyle = 'red'; ctx.beginPath(); ctx.arc(x+sz-3, y+3, sz/5, 0, Math.PI*2); ctx.fill(); }
            }
        });
    }

    let currTime = __MIN_TIME__;
    let isPlaying = false;
    let lastFrameTime = 0; // For real delta time

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

        Object.keys(stState).forEach(sid => { 
            stState[sid].status = 'IDLE'; stState[sid].color = 'WHITE'; stState[sid].wave = '-'; stState[sid].delay = false;
        });
        
        for(let i=0; i<events.length; i++) {
            const e = events[i];
            if (e[8] === 'STATION_STATUS') {
                if (e[0] > time) break; 
                if (e[1] >= time) {
                    const sid = e[3];
                    if(stState[sid]) {
                        const parts = e[9].split('|');
                        const color = parts[0] || 'WHITE';
                        const wid = parts[1] || '?';
                        const isDelay = parts[2] === 'True' || parts[2] === 'Y';
                        stState[sid].status = color === 'BLUE' ? 'OUT' : (color === 'GREEN' ? 'IN' : 'REP');
                        stState[sid].color = color;
                        stState[sid].floor = e[2];
                        stState[sid].x = e[4]; stState[sid].y = e[5];
                        stState[sid].wave = wid;
                        stState[sid].delay = isDelay;
                    }
                }
            }
        }
    }

    function render() {
        updateState(currTime);
        drawMap(f2, '2F');
        drawMap(f3, '3F');
        
        let activeCount = 0;
        const occupancy = {};
        
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            if(!obj.map) return;
            
            const gx = Math.round(s.x);
            const gy = Math.round(s.y);
            const key = `${s.floor}_${gx}_${gy}`;
            occupancy[key] = (occupancy[key] || 0) + 1;
            const count = occupancy[key];
            
            let offX = 0, offY = 0;
            if (count > 1) {
                const shift = obj.size * 0.3;
                offX = ((count % 2) === 0 ? 1 : -1) * shift * (count/2);
                offY = ((count % 2) !== 0 ? 1 : -1) * shift * (count/3);
            }

            const sz = obj.size;
            const px = obj.ox + s.x * sz + sz/2 + offX;
            const py = obj.oy + s.y * sz + sz/2 + offY;
            
            obj.ctx.fillStyle = agvColors[id];
            obj.ctx.beginPath(); obj.ctx.arc(px, py, sz/2.2, 0, Math.PI*2); obj.ctx.fill();
            activeCount++;
        });
        document.getElementById('val-active').innerText = activeCount;
        
        let html2 = '', html3 = '';
        stIds.forEach(sid => {
            const s = stState[sid];
            const color = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : s.color==='ORANGE'?'#fd7e14':'#eee';
            const delayHtml = s.delay ? '<div class="delay-badge">DELAY</div>' : '';
            const card = `<div class="station-card">
                <div style="font-weight:bold;display:flex;justify-content:space-between;width:100%">${sid.replace('WS_','')} ${delayHtml}</div>
                <div style="margin-top:2px"><span class="status-dot" style="background:${color}"></span>${s.status}</div>
                <div class="st-wave" title="${s.wave}">${s.wave}</div>
            </div>`;
            if (s.floor === '2F') html2 += card; else html3 += card;
        });
        document.getElementById('st-list-2f').innerHTML = html2 || 'No Data';
        document.getElementById('st-list-3f').innerHTML = html3 || 'No Data';

        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-done').innerText = doneTasks.length;
        document.getElementById('timeDisplay').innerText = new Date(currTime*1000).toLocaleString();
        document.getElementById('slider').value = currTime;
        
        // Active Waves
        const doneByWave = {};
        doneTasks.filter(k=>k[1]=='PICKING').forEach(k=>{ doneByWave[k[2]]=(doneByWave[k[2]]||0)+1 });
        let wHtml = '';
        Object.keys(waveInfo).sort().forEach(wid => {
            const info = waveInfo[wid];
            const done = doneByWave[wid] || 0;
            const total = info.total || 1;
            if(done > 0 && done <= total) {
                const pct = (done/total*100).toFixed(0);
                const delayedSoFar = doneTasks.filter(k => k[2] === wid && k[3] === 'Y').length;
                const isDelayed = delayedSoFar > 0;
                const barColor = isDelayed ? '#dc3545' : '#007bff';
                const warn = isDelayed ? '<span class="warn-tag">DELAY</span>' : '';
                wHtml += `<div class="wave-item">
                    <div style="display:flex;justify-content:space-between"><span>${wid} ${warn}</span><span>${done}/${total}</span></div>
                    <div class="progress-bg"><div class="progress-fill" style="width:${pct}%;background:${barColor}"></div></div>
                </div>`;
            }
        });
        document.getElementById('wave-list').innerHTML = wHtml || '<div style="color:#999;padding:5px">Waiting...</div>';
        
        // Receiving (Now reads from precomputed recvInfo)
        let rHtml = '';
        const todayStr = new Date(currTime*1000).toISOString().split('T')[0];
        
        // Need to calculate Done for today's Receiving
        const rDone = doneTasks.filter(k => k[4] === todayStr && k[1] == 'RECEIVING').length;
        const rTotal = recvInfo[todayStr] ? recvInfo[todayStr].total : 0;
        
        if (rTotal > 0) {
            const rPct = (rDone/rTotal*100).toFixed(0);
            rHtml = `<div class="wave-item">
                <div style="display:flex;justify-content:space-between"><span>📅 ${todayStr}</span><span>${rDone} / ${rTotal}</span></div>
                <div class="progress-bg"><div class="progress-fill" style="width:${rPct}%;background:#28a745"></div></div>
            </div>`;
        } else {
            rHtml = '<div style="color:#999;padding:5px">No receiving orders today</div>';
        }
        document.getElementById('recv-list').innerHTML = rHtml;
    }

    function animate() {
        requestAnimationFrame(animate);
        if(!isPlaying) { lastFrameTime = performance.now(); return; }
        
        const now = performance.now();
        const dt = (now - lastFrameTime) / 1000; // seconds
        lastFrameTime = now;
        
        const speed = parseInt(document.getElementById('speed').value);
        // Correct time progression: dt (real sec) * speed (sim sec / real sec)
        currTime += dt * speed; 
        
        if(currTime > __MAX_TIME__) { currTime = __MIN_TIME__; isPlaying = false; }
        render();
    }
    
    function togglePlay() { 
        isPlaying=!isPlaying; 
        if(isPlaying) lastFrameTime = performance.now(); 
    }
    document.getElementById('slider').addEventListener('input', e=>{ currTime=parseInt(e.target.value); render(); });
    
    render();
</script>
</body>
</html>
"""
    js_shelf_data = {'2F': [], '3F': []}
    for f in shelf_data:
        js_shelf_data[f] = [f"{c[0]},{c[1]}" for c in shelf_data[f]]

    final_html = html_template.replace('__MAP2F__', json.dumps(map_2f)) \
                              .replace('__MAP3F__', json.dumps(map_3f)) \
                              .replace('__SHELF_DATA__', json.dumps(js_shelf_data)) \
                              .replace('__EVENTS__', json.dumps(events_data)) \
                              .replace('__KPI_RAW__', json.dumps(kpi_raw)) \
                              .replace('__WAVE_INFO__', json.dumps(wave_info)) \
                              .replace('__RECV_INFO__', json.dumps(recv_info)) \
                              .replace('__AGV_IDS__', json.dumps(all_agvs)) \
                              .replace('__STATION_IDS__', json.dumps(all_stations)) \
                              .replace('__MIN_TIME__', str(min_time)) \
                              .replace('__MAX_TIME__', str(max_time))

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"✅ 視覺化生成完畢: {OUTPUT_HTML}")

if __name__ == "__main__":
    main()