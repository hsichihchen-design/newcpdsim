import pandas as pd
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
LOG_DIR = os.path.join(BASE_DIR, 'logs')
OUTPUT_HTML = os.path.join(LOG_DIR, 'dashboard_report.html')

def load_map_robust(filename):
    path = os.path.join(DATA_MAP_DIR, filename)
    if os.path.exists(path):
        try: return pd.read_excel(path, header=None).fillna(0).values.tolist()
        except: pass
    csv_path = os.path.join(DATA_MAP_DIR, os.path.splitext(filename)[0] + ".csv")
    if os.path.exists(csv_path):
        try: return pd.read_csv(csv_path, header=None).fillna(0).values.tolist()
        except: pass
    return []

def main():
    print("🚀 [Step 5] 啟動視覺化 (Fix Disappearing Cars & Layout)...")

    # 1. Map
    map_2f = load_map_robust('2F_map.xlsx')
    map_3f = load_map_robust('3F_map.xlsx')
    
    # 2. Events
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): return
    df_events = pd.read_csv(events_path)
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], format='mixed').astype('int64') // 10**9
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], format='mixed').astype('int64') // 10**9
    
    # Filter valid
    df_events = df_events[(df_events['sx']>=0) & (df_events['sy']>=0)]
    
    # Sort events by time to ensure correct playback state reconstruction
    df_events = df_events.sort_values('start_ts')
    
    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].fillna('').values.tolist()
    min_time = df_events['start_ts'].min() if not df_events.empty else 0
    max_time = df_events['end_ts'].max() if not df_events.empty else 1
    
    # Get all unique AGVs for initialization
    all_agvs = df_events[df_events['type']=='AGV_MOVE']['obj_id'].unique().tolist()

    # 3. KPI
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    try:
        df_kpi = pd.read_csv(kpi_path)
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').astype('int64') // 10**9
        df_kpi['date'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').dt.date.astype(str)
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation']].values.tolist()
        
        wave_stats = {}
        for _, row in df_kpi[df_kpi['type']=='PICKING'].iterrows():
            wid = row['wave_id']
            if wid not in wave_stats: wave_stats[wid] = {'total': 0, 'delayed': 0}
            wave_stats[wid]['total'] += 1
            if row['is_delayed'] == 'Y': wave_stats[wid]['delayed'] += 1
            
        recv_totals = df_kpi[df_kpi['type'] == 'RECEIVING'].groupby('date').size().to_dict()
    except:
        kpi_raw = []
        wave_stats = {}
        recv_totals = {}

    # --- Injection Template ---
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; background: #eef1f5; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        
        /* Map Section: Use Flexbox to ensure both floors fit */
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; overflow: hidden; }
        .floor-container { 
            flex: 1; /* Both floors take equal space */
            background: #fff; border: 1px solid #ccc; border-radius: 4px; padding: 5px; 
            position: relative; display: flex; flex-direction: column; 
            min-height: 0; /* Critical for nested flex scrolling/sizing */
        }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.8); padding: 2px 6px; font-weight: bold; font-size: 12px; z-index: 10; border: 1px solid #999; }
        .canvas-wrap { flex: 1; width: 100%; height: 100%; position: relative; }
        canvas { display: block; width: 100%; height: 100%; }
        
        /* Dashboard */
        .dash-section { flex: 1; min-width: 320px; max-width: 400px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 5px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        .wave-item { margin-bottom: 4px; padding: 4px; background: #fff; border: 1px solid #ddd; font-size: 11px; }
        .wave-item.active { border-color: #007bff; border-left-width: 3px; }
        .progress { height: 5px; background: #e9ecef; margin-top: 2px; }
        .progress-bar { height: 100%; transition: width 0.3s; }
        
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; flex-shrink: 0; }
        button { cursor: pointer; background: #007bff; color: white; border: none; padding: 5px 15px; border-radius: 4px; }
        input[type=range] { flex: 1; }
        .stat-row { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 3px; }
        .stat-val { font-weight: bold; color: #007bff; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲模擬 (Fix: Clipping & State)</h3>
        <div style="flex:1"></div>
        <span id="timeDisplay" style="font-weight: bold;">--</span>
    </div>
    <div class="main">
        <div class="map-section">
            <div class="floor-container">
                <div class="floor-label">2F</div>
                <div class="canvas-wrap"><canvas id="c2"></canvas></div>
            </div>
            <div class="floor-container">
                <div class="floor-label">3F</div>
                <div class="canvas-wrap"><canvas id="c3"></canvas></div>
            </div>
        </div>
        <div class="dash-section">
            <div class="dash-content">
                <div class="panel">
                    <h4>🌊 波次進度</h4>
                    <div id="wave-list">Loading...</div>
                </div>
                <div class="panel">
                    <h4>🚛 進貨</h4>
                    <div class="stat-row"><span>完成/目標</span> <span id="recv-txt">0 / 0</span></div>
                    <div class="progress"><div id="recv-bar" class="progress-bar" style="background:#17a2b8; width:0%"></div></div>
                </div>
                <div class="panel">
                    <h4>📊 統計</h4>
                    <div class="stat-row"><span>活躍 AGV</span> <span id="val-active">0</span></div>
                    <div class="stat-row"><span>已完成單</span> <span id="val-done">0</span></div>
                </div>
            </div>
            <div class="controls">
                <button onclick="togglePlay()" id="playBtn">Play</button>
                <input type="range" id="slider" min="__MIN_TIME__" max="__MAX_TIME__" value="__MIN_TIME__">
                <select id="speed"><option value="10">10x</option><option value="60">1 min/s</option><option value="600" selected>10 min/s</option></select>
            </div>
        </div>
    </div>
<script>
    const map2F = __MAP2F__;
    const map3F = __MAP3F__;
    const events = __EVENTS__;
    const kpiRaw = __KPI_RAW__;
    const waveStats = __WAVE_STATS__;
    const recvTotals = __RECV_TOTALS__;
    const agvIds = __AGV_IDS__;

    // State Memory
    let agvState = {};
    agvIds.forEach(id => { agvState[id] = { floor: '2F', x: -1, y: -1, visible: false }; });

    function setupCanvas(id, mapData) {
        const c = document.getElementById(id);
        const ctx = c.getContext('2d');
        const parent = c.parentElement;
        const w = parent.clientWidth;
        const h = parent.clientHeight;
        c.width = w; c.height = h;
        
        const rows = mapData.length || 10;
        const cols = mapData[0]?.length || 10;
        const scaleX = w / cols;
        const scaleY = h / rows;
        const size = Math.min(scaleX, scaleY);
        const ox = (w - cols*size)/2;
        const oy = (h - rows*size)/2;
        return { ctx, rows, cols, size, ox, oy, map: mapData };
    }
    
    let f2 = setupCanvas('c2', map2F);
    let f3 = setupCanvas('c3', map3F);
    window.onresize = () => { f2 = setupCanvas('c2', map2F); f3 = setupCanvas('c3', map3F); render(); };

    function drawMap(obj) {
        const ctx = obj.ctx;
        ctx.fillStyle = '#fafafa'; ctx.fillRect(0,0, ctx.canvas.width, ctx.canvas.height);
        for(let r=0; r<obj.rows; r++) {
            for(let c=0; c<obj.cols; c++) {
                const val = obj.map[r][c];
                const x=obj.ox+c*obj.size, y=obj.oy+r*obj.size, s=obj.size;
                if(val==1) { ctx.fillStyle='#ccc'; ctx.fillRect(x,y,s,s); }
                else if(val==2) { ctx.strokeStyle='#666'; ctx.strokeRect(x,y,s,s); }
                else if(val==3) { ctx.fillStyle='#cff4fc'; ctx.fillRect(x,y,s,s); }
            }
        }
    }

    let currTime = __MIN_TIME__;
    let isPlaying = false;

    // Helper: Find last known position if current event is missing
    // This solves "Disappearing Car" when using slider or sparse events
    function updateAGVStateStrict(time) {
        // Reset visible to false first? No, assume persistence.
        // We only update if we find a relevant event <= time
        
        // This is O(N*M) worst case, but manageable for 16 AGVs
        agvIds.forEach(id => {
            // Find the *latest* event for this AGV that started before 'time'
            // Since events are sorted by start_time, we can search backwards or filter
            // Optimization: Just filter valid events for this ID
            // Simple approach: Filter all events for this ID <= time, take last.
            
            // For performance in browser, we can rely on forward-play caching.
            // But for slider jumps, we need a full scan.
            // Let's do a simple full scan for now (safe).
            
            let lastEvt = null;
            // Iterate backwards is faster for "latest"
            for(let i=events.length-1; i>=0; i--) {
                const e = events[i];
                if(e[0] <= time && e[3] === id && e[8] === 'AGV_MOVE') {
                    lastEvt = e;
                    break;
                }
            }
            
            if(lastEvt) {
                // If the event spans across 'time' (it's happening now) -> Interpolate
                if (time <= lastEvt[1]) {
                    const totalDur = lastEvt[1] - lastEvt[0];
                    const p = (time - lastEvt[0]) / totalDur;
                    const sx=lastEvt[4], sy=lastEvt[5], ex=lastEvt[6], ey=lastEvt[7];
                    const cx = sx + (ex-sx)*p;
                    const cy = sy + (ey-sy)*p;
                    agvState[id] = { floor: lastEvt[2], x: cx, y: cy, visible: true };
                } else {
                    // Event finished in the past -> Car stays at 'ex, ey'
                    const ex=lastEvt[6], ey=lastEvt[7];
                    agvState[id] = { floor: lastEvt[2], x: ex, y: ey, visible: true };
                }
            }
        });
    }

    function render() {
        drawMap(f2); drawMap(f3);
        
        // Update State (Strict Mode)
        updateAGVStateStrict(currTime);
        
        let activeCount = 0;
        const occ = {};
        
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            const ctx = obj.ctx;
            const sz = obj.size;
            
            // Jitter
            const key = s.floor + '_' + Math.round(s.x) + '_' + Math.round(s.y);
            occ[key] = (occ[key]||0) + 1;
            let ox=0, oy=0;
            if(occ[key]>1) { ox=(occ[key]*2); oy=(occ[key]*2); }
            
            const px = obj.ox + s.x * sz + sz/2 + ox;
            const py = obj.oy + s.y * sz + sz/2 + oy;
            
            ctx.fillStyle = '#28a745';
            ctx.beginPath(); ctx.arc(px, py, sz/2.5, 0, Math.PI*2); ctx.fill();
            activeCount++;
        });

        // KPI
        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-active').innerText = activeCount;
        document.getElementById('val-done').innerText = doneTasks.length;
        
        // Waves
        const doneByWave = {};
        doneTasks.filter(k=>k[1]=='PICKING').forEach(k=>{ doneByWave[k[2]]=(doneByWave[k[2]]||0)+1 });
        
        let html = '';
        Object.keys(waveStats).sort().forEach(wid => {
            const stat = waveStats[wid];
            const done = doneByWave[wid] || 0;
            const isDone = done >= stat.total;
            if((done>0 && !isDone) || (isDone && stat.delayed>0)) {
                const pct = stat.total>0?(done/stat.total*100).toFixed(0):0;
                let color = isDone ? '#6c757d' : '#007bff';
                html += `<div class="wave-item ${done>0?'active':''}">
                    <div style="display:flex;justify-content:space-between"><span>${wid}</span><span>${done}/${stat.total}</span></div>
                    <div class="progress"><div class="progress-bar" style="width:${pct}%; background:${color}"></div></div>
                </div>`;
            }
        });
        document.getElementById('wave-list').innerHTML = html || '<div style="color:#999;text-align:center">Waiting...</div>';
        
        // Recv
        const nowStr = new Date(currTime*1000).toISOString().split('T')[0];
        const target = recvTotals[nowStr] || 0;
        const rDone = doneTasks.filter(k => k[4]==nowStr && k[1]=='RECEIVING').length;
        document.getElementById('recv-txt').innerText = `${rDone} / ${target}`;
        const rPct = target>0 ? (rDone/target*100) : 0;
        document.getElementById('recv-bar').style.width = rPct+'%';

        document.getElementById('timeDisplay').innerText = new Date(currTime*1000).toLocaleString();
        document.getElementById('slider').value = currTime;
    }

    function animate() {
        if(!isPlaying) return;
        const spd = parseInt(document.getElementById('speed').value);
        currTime += spd;
        if(currTime > __MAX_TIME__) currTime = __MIN_TIME__;
        render();
        requestAnimationFrame(animate);
    }
    
    function togglePlay() { isPlaying=!isPlaying; if(isPlaying) animate(); }
    document.getElementById('slider').addEventListener('input', e=>{ currTime=parseInt(e.target.value); render(); });
    
    render();
</script>
</body>
</html>
"""
    # Safe Injection
    final_html = html_template.replace('__MAP2F__', json.dumps(map_2f)) \
                              .replace('__MAP3F__', json.dumps(map_3f)) \
                              .replace('__EVENTS__', json.dumps(events_data)) \
                              .replace('__KPI_RAW__', json.dumps(kpi_raw)) \
                              .replace('__WAVE_STATS__', json.dumps(wave_stats)) \
                              .replace('__RECV_TOTALS__', json.dumps(recv_totals)) \
                              .replace('__AGV_IDS__', json.dumps(all_agvs)) \
                              .replace('__MIN_TIME__', str(min_time)) \
                              .replace('__MAX_TIME__', str(max_time))

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"✅ 視覺化生成完畢: {OUTPUT_HTML}")

if __name__ == "__main__":
    main()