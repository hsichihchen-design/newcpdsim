import pandas as pd
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
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
        # [Visualizer Fix] 標準化: 把 -1 變成 1，這樣才會畫出牆壁
        for r in range(len(grid)):
            for c in range(len(grid[0])):
                if grid[r][c] == -1: grid[r][c] = 1
        return grid
    return []

def main():
    print("🚀 [Step 5] 啟動視覺化 (Map Value Normalized)...")

    # 1. Map
    map_2f = load_map_robust('2F_map.xlsx')
    map_3f = load_map_robust('3F_map.xlsx')
    
    # 2. Events
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): return
    df_events = pd.read_csv(events_path)
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], format='mixed').astype('int64') // 10**9
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], format='mixed').astype('int64') // 10**9
    df_events = df_events.sort_values('start_ts')
    
    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].fillna('').values.tolist()
    
    min_time = df_events['start_ts'].min() if not df_events.empty else 0
    max_time = df_events['end_ts'].max() if not df_events.empty else 1
    
    all_agvs = df_events[df_events['type']=='AGV_MOVE']['obj_id'].unique().tolist()
    all_stations = df_events[df_events['obj_id'].str.startswith('WS_')]['obj_id'].unique().tolist()
    try: all_stations.sort(key=lambda x: int(x.split('_')[1]))
    except: pass

    # 3. KPI
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    kpi_raw = []
    wave_stats = {}
    try:
        df_kpi = pd.read_csv(kpi_path)
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').astype('int64') // 10**9
        df_kpi['date'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').dt.date.astype(str)
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation']].values.tolist()
        
        for _, row in df_kpi[df_kpi['type']=='PICKING'].iterrows():
            wid = row['wave_id']
            if wid not in wave_stats: wave_stats[wid] = {'total': 0, 'delayed': 0}
            wave_stats[wid]['total'] += 1
            if row['is_delayed'] == 'Y': wave_stats[wid]['delayed'] += 1
    except: pass

    # HTML Template (與之前相同，略微縮短以節省空間，功能不變)
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; background: #eef1f5; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; overflow: hidden; }
        .floor-container { flex: 1; background: #fff; border: 1px solid #ccc; position: relative; }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.8); padding: 2px 6px; font-weight: bold; font-size: 12px; z-index: 10; border: 1px solid #999; }
        canvas { display: block; width: 100%; height: 100%; }
        .dash-section { flex: 1; min-width: 320px; max-width: 400px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 5px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        .station-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(70px, 1fr)); gap: 5px; }
        .station-card { border: 1px solid #ddd; padding: 5px; font-size: 10px; text-align: center; background: #fff; border-radius: 3px; }
        .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 3px; }
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; }
        .legend { display: flex; gap: 10px; font-size: 11px; margin-bottom: 5px; }
        .box { width: 12px; height: 12px; margin-right: 3px; border: 1px solid #666; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲戰情室 (Values Fixed)</h3>
        <div style="flex:1"></div>
        <span id="timeDisplay" style="font-weight: bold;">--</span>
    </div>
    <div class="main">
        <div class="map-section">
            <div class="legend">
                <div style="display:flex;align-items:center"><div class="box" style="background:blue"></div>出貨(Out)</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:green"></div>進貨(In)</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:orange"></div>副倉(Rep)</div>
                <div style="display:flex;align-items:center"><div class="box" style="background:white"></div>空閒(Idle)</div>
            </div>
            <div class="floor-container">
                <div class="floor-label">2F</div>
                <canvas id="c2"></canvas>
            </div>
            <div class="floor-container">
                <div class="floor-label">3F</div>
                <canvas id="c3"></canvas>
            </div>
        </div>
        <div class="dash-section">
            <div class="dash-content">
                <div class="panel">
                    <h4>📡 工作站狀態</h4>
                    <div id="station-list" class="station-grid">Wait...</div>
                </div>
                <div class="panel">
                    <h4>📊 統計</h4>
                    <div>Active AGV: <span id="val-active">0</span></div>
                    <div>Done Orders: <span id="val-done">0</span></div>
                </div>
                <div class="panel">
                    <h4>🌊 波次進度</h4>
                    <div id="wave-list">Wait...</div>
                </div>
            </div>
            <div class="controls">
                <button onclick="togglePlay()" id="playBtn">Play</button>
                <input type="range" id="slider" min="__MIN_TIME__" max="__MAX_TIME__" value="__MIN_TIME__" style="flex:1">
                <select id="speed"><option value="10">1x</option><option value="60">1 min/s</option><option value="600" selected>10 min/s</option></select>
            </div>
        </div>
    </div>
<script>
    const map2F = __MAP2F__;
    const map3F = __MAP3F__;
    const events = __EVENTS__;
    const kpiRaw = __KPI_RAW__;
    const waveStats = __WAVE_STATS__;
    const agvIds = __AGV_IDS__;
    const stIds = __STATION_IDS__;

    let agvState = {};
    agvIds.forEach(id => { agvState[id] = { floor: '2F', x: -1, y: -1, visible: false }; });
    let stState = {};
    stIds.forEach(id => { stState[id] = { status: 'IDLE', color: 'WHITE', floor: '2F', x:-1, y:-1 }; });

    function setupCanvas(id, mapData) {
        const c = document.getElementById(id);
        const ctx = c.getContext('2d');
        const parent = c.parentElement;
        c.width = parent.clientWidth; c.height = parent.clientHeight;
        const rows = mapData.length || 10;
        const cols = mapData[0]?.length || 10;
        const size = Math.min(c.width / cols, c.height / rows);
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
        for(let r=0; r<obj.rows; r++) {
            for(let c=0; c<obj.cols; c++) {
                const val = obj.map[r][c];
                const x=obj.ox+c*obj.size, y=obj.oy+r*obj.size, s=obj.size;
                if(val==1) { ctx.fillStyle='#ccc'; ctx.fillRect(x,y,s,s); } // Wall
                else if(val==2) { ctx.strokeStyle='#999'; ctx.strokeRect(x,y,s,s); } // Station
                else if(val==3) { ctx.fillStyle='#cff4fc'; ctx.fillRect(x,y,s,s); } // Charger
            }
        }
        Object.keys(stState).forEach(sid => {
            const s = stState[sid];
            if(s.floor === floorName && s.x !== -1) {
                const x=obj.ox+s.x*obj.size, y=obj.oy+s.y*obj.size, sz=obj.size;
                ctx.fillStyle = s.color === 'BLUE' ? '#007bff' : 
                                s.color === 'GREEN' ? '#28a745' : 
                                s.color === 'ORANGE' ? '#fd7e14' : 'rgba(255,255,255,0.8)';
                ctx.fillRect(x+2, y+2, sz-4, sz-4);
                ctx.fillStyle = 'black'; ctx.font = '9px Arial';
                ctx.fillText(sid.replace('WS_',''), x+2, y+10);
            }
        });
    }

    let currTime = __MIN_TIME__;
    let isPlaying = false;

    function updateState(time) {
        // AGV
        agvIds.forEach(id => {
            let lastEvt = null;
            for(let i=events.length-1; i>=0; i--) {
                const e = events[i];
                if(e[0] <= time && e[3] === id && e[8] === 'AGV_MOVE') { lastEvt = e; break; }
            }
            if(lastEvt) {
                if (time <= lastEvt[1]) {
                    const p = (time - lastEvt[0]) / (lastEvt[1] - lastEvt[0]);
                    agvState[id] = { floor: lastEvt[2], x: lastEvt[4]+(lastEvt[6]-lastEvt[4])*p, y: lastEvt[5]+(lastEvt[7]-lastEvt[5])*p, visible: true };
                } else {
                    agvState[id] = { floor: lastEvt[2], x: lastEvt[6], y: lastEvt[7], visible: true };
                }
            }
        });

        // Stations
        Object.keys(stState).forEach(sid => {
            stState[sid].status = 'IDLE'; stState[sid].color = 'WHITE';
        });
        for(let i=0; i<events.length; i++) {
            const e = events[i];
            if (e[8] === 'STATION_STATUS') {
                if (e[0] > time) break; 
                if (e[1] >= time) {
                    const sid = e[3];
                    if(stState[sid]) {
                        stState[sid].status = e[9] === 'BLUE' ? 'OUT' : (e[9] === 'GREEN' ? 'IN' : 'REP');
                        stState[sid].color = e[9];
                        stState[sid].floor = e[2];
                        stState[sid].x = e[4]; 
                        stState[sid].y = e[5]; 
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
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            const sz = obj.size;
            const px = obj.ox + s.x * sz + sz/2;
            const py = obj.oy + s.y * sz + sz/2;
            obj.ctx.fillStyle = '#dc3545';
            obj.ctx.beginPath(); obj.ctx.arc(px, py, sz/2.5, 0, Math.PI*2); obj.ctx.fill();
            activeCount++;
        });

        document.getElementById('val-active').innerText = activeCount;
        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-done').innerText = doneTasks.length;
        
        let stHtml = '';
        stIds.forEach(sid => {
            const s = stState[sid];
            const color = s.color === 'BLUE' ? '#007bff' : s.color === 'GREEN' ? '#28a745' : s.color==='ORANGE'?'#fd7e14':'#eee';
            stHtml += `<div class="station-card">
                <div style="font-weight:bold">${sid.replace('WS_','')}</div>
                <div style="margin-top:2px"><span class="status-dot" style="background:${color}"></span>${s.status}</div>
            </div>`;
        });
        document.getElementById('station-list').innerHTML = stHtml;
        document.getElementById('timeDisplay').innerText = new Date(currTime*1000).toLocaleString();
        document.getElementById('slider').value = currTime;
        
        const doneByWave = {};
        doneTasks.filter(k=>k[1]=='PICKING').forEach(k=>{ doneByWave[k[2]]=(doneByWave[k[2]]||0)+1 });
        let wHtml = '';
        Object.keys(waveStats).sort().forEach(wid => {
            const stat = waveStats[wid];
            const done = doneByWave[wid] || 0;
            if(done > 0 && done < stat.total) {
                const pct = (done/stat.total*100).toFixed(0);
                wHtml += `<div style="font-size:11px;margin-bottom:3px">${wid}: ${done}/${stat.total} <div style="height:3px;background:#eee"><div style="width:${pct}%;height:100%;background:#007bff"></div></div></div>`;
            }
        });
        document.getElementById('wave-list').innerHTML = wHtml || '<div style="color:#999">No active waves</div>';
    }

    function animate() {
        if(!isPlaying) return;
        currTime += parseInt(document.getElementById('speed').value);
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
    final_html = html_template.replace('__MAP2F__', json.dumps(map_2f)) \
                              .replace('__MAP3F__', json.dumps(map_3f)) \
                              .replace('__EVENTS__', json.dumps(events_data)) \
                              .replace('__KPI_RAW__', json.dumps(kpi_raw)) \
                              .replace('__WAVE_STATS__', json.dumps(wave_stats)) \
                              .replace('__AGV_IDS__', json.dumps(all_agvs)) \
                              .replace('__STATION_IDS__', json.dumps(all_stations)) \
                              .replace('__MIN_TIME__', str(min_time)) \
                              .replace('__MAX_TIME__', str(max_time))

    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(final_html)
    print(f"✅ 視覺化生成完畢: {OUTPUT_HTML}")

if __name__ == "__main__":
    main()