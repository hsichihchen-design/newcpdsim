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
    print("🚀 [Step 5] 啟動視覺化 (Safe Injection Mode)...")

    # 1. Map
    map_2f = load_map_robust('2F_map.xlsx')
    map_3f = load_map_robust('3F_map.xlsx')
    
    # 2. Events
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(events_path): 
        print("❌ 找不到 simulation_events.csv")
        return
        
    df_events = pd.read_csv(events_path)
    df_events['start_ts'] = pd.to_datetime(df_events['start_time'], format='mixed').astype('int64') // 10**9
    df_events['end_ts'] = pd.to_datetime(df_events['end_time'], format='mixed').astype('int64') // 10**9
    df_events = df_events[(df_events['sx']>=0) & (df_events['sy']>=0)]
    
    min_time = df_events['start_ts'].min()
    max_time = df_events['end_ts'].max()
    all_agvs = df_events[df_events['type'] == 'AGV_MOVE']['obj_id'].unique().tolist()
    events_data = df_events[['start_ts', 'end_ts', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']].fillna('').values.tolist()

    # 3. KPI
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    try:
        df_kpi = pd.read_csv(kpi_path)
        df_kpi['finish_ts'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').astype('int64') // 10**9
        df_kpi['date'] = pd.to_datetime(df_kpi['finish_time'], format='mixed').dt.date.astype(str)
        kpi_raw = df_kpi[['finish_ts', 'type', 'wave_id', 'is_delayed', 'date', 'workstation']].values.tolist()
        
        # Stats
        wave_stats = {}
        for _, row in df_kpi[df_kpi['type']=='PICKING'].iterrows():
            wid = row['wave_id']
            if wid not in wave_stats: wave_stats[wid] = {'total': 0, 'delayed': 0}
            wave_stats[wid]['total'] += 1
            if row['is_delayed'] == 'Y': wave_stats[wid]['delayed'] += 1
            
        recv_totals = df_kpi[df_kpi['type'] == 'RECEIVING'].groupby('date').size().to_dict()
    except Exception as e:
        print(f"⚠️ KPI Error: {e}")
        kpi_raw = []
        wave_stats = {}
        recv_totals = {}

    # --- HTML Template (No f-string here to avoid brace conflicts) ---
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Warehouse Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', Roboto, sans-serif; background: #eef1f5; margin: 0; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }
        .header { background: #fff; height: 40px; padding: 0 20px; display: flex; align-items: center; border-bottom: 1px solid #ddd; flex-shrink: 0; }
        .main { display: flex; flex: 1; overflow: hidden; }
        .map-section { flex: 3; display: flex; flex-direction: column; padding: 10px; gap: 10px; }
        .floor-container { flex: 1; background: #fff; border: 1px solid #ccc; border-radius: 4px; padding: 5px; position: relative; display: flex; flex-direction: column; min-height: 0; }
        .floor-label { position: absolute; top: 5px; left: 5px; background: rgba(255,255,255,0.8); padding: 2px 6px; font-weight: bold; border: 1px solid #999; z-index: 5; font-size: 12px; }
        .canvas-wrap { flex: 1; position: relative; width: 100%; height: 100%; }
        canvas { display: block; width: 100%; height: 100%; }
        .dash-section { flex: 1; min-width: 320px; max-width: 380px; background: #fff; border-left: 1px solid #ccc; display: flex; flex-direction: column; }
        .dash-content { flex: 1; overflow-y: auto; padding: 10px; }
        .panel { margin-bottom: 10px; border: 1px solid #eee; padding: 8px; border-radius: 4px; background: #fafafa; }
        .panel h4 { margin: 0 0 5px 0; border-bottom: 2px solid #007bff; font-size: 14px; color: #333; }
        .wave-item { margin-bottom: 4px; padding: 4px; background: #fff; border: 1px solid #ddd; font-size: 11px; }
        .wave-item.active { border-color: #007bff; border-left-width: 3px; }
        .wave-item.delayed { border-left: 4px solid #dc3545; }
        .progress { height: 5px; background: #e9ecef; margin-top: 2px; }
        .progress-bar { height: 100%; transition: width 0.3s; }
        .controls { padding: 10px; background: #fff; border-top: 1px solid #ddd; display: flex; gap: 10px; align-items: center; flex-shrink: 0; }
        button { cursor: pointer; background: #007bff; color: white; border: none; padding: 5px 15px; border-radius: 4px; }
        input[type=range] { flex: 1; }
        .ws-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 4px; }
        .ws-box { background: #fff; border: 1px solid #ccc; padding: 2px; text-align: center; border-radius: 2px; font-size: 10px; }
        .ws-box.busy { background: #ffebeb; border-color: red; color: red; font-weight: bold; }
    </style>
</head>
<body>
    <div class="header">
        <h3>🏭 倉儲模擬 (Safe Mode)</h3>
        <div style="flex:1"></div>
        <span id="timeDisplay" style="font-weight: bold;">--</span>
    </div>
    <div class="main">
        <div class="map-section">
            <div class="floor-container"><div class="floor-label">2F</div><div class="canvas-wrap"><canvas id="c2"></canvas></div></div>
            <div class="floor-container"><div class="floor-label">3F</div><div class="canvas-wrap"><canvas id="c3"></canvas></div></div>
        </div>
        <div class="dash-section">
            <div class="dash-content">
                <div class="panel"><h4>🌊 波次進度</h4><div id="wave-list">Loading...</div></div>
                <div class="panel">
                    <h4>🚛 進貨狀況</h4>
                    <div>目標/完成: <span id="recv-txt">0/0</span></div>
                    <div class="progress"><div id="recv-bar" class="progress-bar" style="background:#17a2b8; width:0%"></div></div>
                </div>
                <div class="panel"><h4>📊 統計</h4><div>活躍 AGV: <span id="val-active">0</span></div></div>
                <div class="panel"><h4>🏭 工作站</h4><div style="margin-bottom:5px">2F</div><div id="ws-2f" class="ws-grid"></div><div style="margin-top:5px;margin-bottom:5px">3F</div><div id="ws-3f" class="ws-grid"></div></div>
            </div>
            <div class="controls">
                <button onclick="togglePlay()" id="playBtn">Play</button>
                <input type="range" id="slider" min="__MIN_TIME__" max="__MAX_TIME__" value="__MIN_TIME__">
                <select id="speed"><option value="1">1x</option><option value="10" selected>10x</option><option value="60">60x</option></select>
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

    let agvState = {};
    agvIds.forEach(id => { agvState[id] = { floor: '2F', x: 0, y: 0, visible: false }; });

    function setupCanvas(id, mapData) {
        const c = document.getElementById(id);
        const ctx = c.getContext('2d');
        const parent = c.parentElement;
        const w = parent.clientWidth;
        const h = parent.clientHeight;
        c.width = w; c.height = h;
        const rows = mapData.length || 10;
        const cols = mapData[0]?.length || 10;
        const size = Math.min(w/cols, h/rows);
        const ox = (w - cols*size)/2;
        const oy = (h - rows*size)/2;
        return { ctx, rows, cols, size, ox, oy, map: mapData };
    }
    
    let f2 = setupCanvas('c2', map2F);
    let f3 = setupCanvas('c3', map3F);
    window.onresize = () => { f2 = setupCanvas('c2', map2F); f3 = setupCanvas('c3', map3F); render(); };

    function initWS() {
        const w2 = document.getElementById('ws-2f');
        const w3 = document.getElementById('ws-3f');
        let h2='', h3='';
        for(let i=1; i<=8; i++) h2 += `<div id="ws-box-${i}" class="ws-box">WS_${i}</div>`;
        for(let i=101; i<=108; i++) h3 += `<div id="ws-box-${i}" class="ws-box">WS_${i}</div>`;
        w2.innerHTML = h2; w3.innerHTML = h3;
    }
    initWS();

    function drawMap(obj) {
        const ctx = obj.ctx;
        ctx.fillStyle = '#fafafa'; ctx.fillRect(0,0, ctx.canvas.width, ctx.canvas.height);
        for(let r=0; r<obj.rows; r++) {
            for(let c=0; c<obj.cols; c++) {
                const val = obj.map[r][c];
                const x = obj.ox + c*obj.size;
                const y = obj.oy + r*obj.size;
                const s = obj.size;
                if(val==1) { ctx.fillStyle='#ccc'; ctx.fillRect(x,y,s,s); }
                else if(val==2) { ctx.strokeStyle='#666'; ctx.strokeRect(x,y,s,s); }
                else if(val==3) { ctx.fillStyle='#cff4fc'; ctx.fillRect(x,y,s,s); }
            }
        }
    }

    let currTime = __MIN_TIME__;
    let isPlaying = false;

    function render() {
        drawMap(f2); drawMap(f3);
        const activeEvts = events.filter(e => currTime >= e[0] && currTime <= e[1] && e[8] == 'AGV_MOVE');
        
        activeEvts.forEach(e => {
            const totalDur = e[1] - e[0];
            const p = (currTime - e[0]) / totalDur;
            const sx=e[4], sy=e[5], ex=e[6], ey=e[7];
            const cx = sx + (ex-sx)*p;
            const cy = sy + (ey-sy)*p;
            agvState[e[3]] = { floor: e[2], x: cx, y: cy, visible: true };
        });
        
        let activeCount = 0;
        const occupancy = {};
        
        Object.keys(agvState).forEach(id => {
            const s = agvState[id];
            if (!s.visible) return;
            const obj = s.floor == '2F' ? f2 : f3;
            const ctx = obj.ctx;
            const sz = obj.size;
            
            const key = s.floor + '_' + Math.round(s.x) + '_' + Math.round(s.y);
            const occ = occupancy[key] || 0;
            occupancy[key] = occ + 1;
            let ox = 0, oy = 0;
            if(occ > 0) {
                ox = (occ%2==0?1:-1) * (occ*2);
                oy = (occ%3==0?1:-1) * (occ*2);
            }
            
            const px = obj.ox + s.x * sz + sz/2 + ox;
            const py = obj.oy + s.y * sz + sz/2 + oy;
            
            ctx.fillStyle = '#28a745';
            ctx.beginPath(); ctx.arc(px, py, sz/2.5, 0, Math.PI*2); ctx.fill();
            activeCount++;
        });

        // Update KPI
        const doneTasks = kpiRaw.filter(k => k[0] <= currTime);
        document.getElementById('val-active').innerText = activeCount;
        
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
        if(!html) html = '<div style="color:#999;text-align:center">Waiting...</div>';
        document.getElementById('wave-list').innerHTML = html;
        
        // Receiving
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
    
    # --- Safe Injection ---
    # 使用 replace 替換佔位符，完全避免 f-string 衝突
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