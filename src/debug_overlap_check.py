import pandas as pd
import numpy as np
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
MAP_DIR = os.path.join(BASE_DIR, 'data', 'mapping')

class Interval:
    def __init__(self, start, end, obj_type, obj_id):
        self.start = start
        self.end = end
        self.obj_type = obj_type # 'AGV_EMPTY', 'AGV_LOADED', 'SHELF_STATIC'
        self.obj_id = obj_id

    def overlaps(self, other):
        return max(self.start, other.start) < min(self.end, other.end) - 0.1 # 0.1s tolerance

def debug_overlap():
    print("🕵️‍♂️ [時空重疊驗證] 啟動嚴格碰撞檢查...")
    
    # 1. 載入事件
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path):
        print("❌ 找不到 simulation_events.csv")
        return

    try:
        df = pd.read_csv(evt_path)
        df['start_ts'] = pd.to_datetime(df['start_time'])
        df['end_ts'] = pd.to_datetime(df['end_time'])
        # 轉為秒數 (以第一筆為 0)
        base_time = df['start_ts'].min()
        df['s'] = (df['start_ts'] - base_time).dt.total_seconds()
        df['e'] = (df['end_ts'] - base_time).dt.total_seconds()
    except Exception as e:
        print(f"❌ 讀取事件失敗: {e}")
        return

    # 2. 載入初始料架位置
    shelf_map_path = os.path.join(MAP_DIR, 'shelf_coordinate_map.csv')
    initial_shelves = defaultdict(list) # key: floor, val: list of (x, y)
    try:
        df_shelf = pd.read_csv(shelf_map_path)
        for _, row in df_shelf.iterrows():
            initial_shelves[row['floor']].append((row['x'], row['y']))
        print(f"   已載入初始料架: 2F={len(initial_shelves['2F'])}, 3F={len(initial_shelves['3F'])}")
    except:
        print("⚠️ 無法載入 shelf_coordinate_map.csv，假設初始地圖無靜態料架")

    # 3. 建立時空網格 (Spatio-Temporal Grid)
    # grid_usage[(floor, x, y)] = list of Intervals
    grid_usage = defaultdict(list)
    
    # 3.1 處理 AGV 狀態與移動
    # 先整理每個 AGV 的載貨狀態時間軸
    agv_loaded_intervals = defaultdict(list) # agv_id -> list of (start, end) where it is LOADED
    
    agv_groups = df.groupby('obj_id')
    for agv_id, group in agv_groups:
        if not agv_id.startswith('AGV'): continue
        group = group.sort_values('s')
        
        is_loaded = False
        load_start_time = 0
        
        for _, row in group.iterrows():
            if row['type'] == 'SHELF_LOAD':
                is_loaded = True
                load_start_time = row['e'] # Load 完成後開始算 Loaded
            elif row['type'] == 'SHELF_UNLOAD':
                if is_loaded:
                    agv_loaded_intervals[agv_id].append((load_start_time, row['s'])) # Unload 開始前結束
                is_loaded = False
        
        # 如果最後還在載貨，持續到永遠
        if is_loaded:
            agv_loaded_intervals[agv_id].append((load_start_time, 999999))

    def is_agv_loaded(aid, t):
        for start, end in agv_loaded_intervals[aid]:
            if start <= t <= end: return True
        return False

    print("   正在構建 AGV 移動軌跡...")
    # 填入 AGV 佔用
    for _, row in df.iterrows():
        if not str(row['obj_id']).startswith('AGV'): continue
        if row['type'] not in ['AGV_MOVE', 'SHELF_LOAD', 'SHELF_UNLOAD', 'STATION_STATUS']: continue
        
        floor = row['floor']
        sx, sy = int(row['sx']), int(row['sy'])
        ex, ey = int(row['ex']), int(row['ey'])
        start_t, end_t = row['s'], row['e']
        agv_id = row['obj_id']
        
        # 判斷這段時間是否載貨 (取中間點判斷)
        mid_t = (start_t + end_t) / 2
        loaded = is_agv_loaded(agv_id, mid_t)
        obj_type = 'AGV_LOADED' if loaded else 'AGV_EMPTY'
        
        # 簡單插值 (假設走直線 Manhattan)
        # 注意：這裡簡化為佔用起點和終點的路徑上的所有格子
        # 嚴格來說應該根據時間插值，但為了捕捉「穿模」，我們標記整段路徑
        
        points = set()
        points.add((sx, sy))
        
        # 產生路徑點
        curr_x, curr_y = sx, sy
        while curr_x != ex:
            curr_x += 1 if ex > curr_x else -1
            points.add((curr_x, curr_y))
        while curr_y != ey:
            curr_y += 1 if ey > curr_y else -1
            points.add((curr_x, curr_y))
            
        for px, py in points:
            grid_usage[(floor, px, py)].append(Interval(start_t, end_t, obj_type, agv_id))

    # 3.2 處理靜態料架 (SHELF_STATIC)
    print("   正在構建料架狀態...")
    # 每個格子的料架狀態預設為：如果是初始位置，從 0 到 永遠。
    # 但會被 LOAD (移除) 切斷，被 UNLOAD (新增) 恢復。
    
    # 為了簡化，我們用事件流來切割時間軸
    # 對於每個格子，找出所有 LOAD/UNLOAD 事件
    shelf_events = df[df['type'].isin(['SHELF_LOAD', 'SHELF_UNLOAD'])].sort_values('s')
    
    # 整理每個座標的料架變更事件
    cell_shelf_timeline = defaultdict(list)
    for _, row in shelf_events.iterrows():
        key = (row['floor'], int(row['sx']), int(row['sy'])) # sx, sy 是發生地點
        cell_shelf_timeline[key].append((row['s'], row['type'])) # (time, type)

    # 針對地圖上每一個可能有料架的格子進行模擬
    # 聯集：初始位置 + 曾經發生過 LOAD/UNLOAD 的位置
    all_shelf_cells = set()
    for f, coords in initial_shelves.items():
        for x, y in coords: all_shelf_cells.add((f, x, y))
    for k in cell_shelf_timeline.keys():
        all_shelf_cells.add(k)
        
    for key in all_shelf_cells:
        floor, x, y = key
        events = cell_shelf_timeline.get(key, [])
        
        # 初始狀態
        has_shelf = (x, y) in initial_shelves[floor]
        current_t = 0
        
        for t, evt_type in events:
            if has_shelf:
                # 建立一段 STATIC 區間 [current_t, t]
                if t > current_t:
                    grid_usage[key].append(Interval(current_t, t, 'SHELF_STATIC', f"Shelf@{x},{y}"))
            
            # 更新狀態
            if evt_type == 'SHELF_LOAD': has_shelf = False
            elif evt_type == 'SHELF_UNLOAD': has_shelf = True
            current_t = t
            
        # 最後一段
        if has_shelf:
            grid_usage[key].append(Interval(current_t, 999999, 'SHELF_STATIC', f"Shelf@{x},{y}"))

    # 4. 進行碰撞檢測
    print("⚡ 開始全圖掃描檢測違規重疊...")
    violations = 0
    checked_cells = 0
    
    error_log = []

    for cell, intervals in grid_usage.items():
        checked_cells += 1
        # 依照時間排序
        intervals.sort(key=lambda i: i.start)
        
        # 雙層迴圈檢查重疊 (Sweeping Line would be faster but N is small per cell)
        for i in range(len(intervals)):
            for j in range(i+1, len(intervals)):
                a = intervals[i]
                b = intervals[j]
                
                # 如果時間不重疊，因為已排序，後面的也不會重疊 (除非 b.start < a.end)
                if b.start >= a.end - 0.1: 
                    continue # No overlap
                
                # 發生重疊，檢查類型
                if a.overlaps(b):
                    # 檢查是否為合法組合
                    # 合法：AGV_EMPTY + SHELF_STATIC
                    pair = sorted([a.obj_type, b.obj_type])
                    
                    is_valid = False
                    if pair == ['AGV_EMPTY', 'SHELF_STATIC']: is_valid = True
                    
                    if not is_valid:
                        # 排除自己跟自己 (例如同一台車連續移動的邊界微小重疊)
                        if a.obj_id == b.obj_id: continue 
                        
                        violations += 1
                        if violations <= 10:
                            t_start = max(a.start, b.start)
                            t_end = min(a.end, b.end)
                            msg = f"❌ [重疊違規] {cell} @ {t_start:.1f}s~{t_end:.1f}s: {a.obj_type}({a.obj_id}) 撞到 {b.obj_type}({b.obj_id})"
                            error_log.append(msg)

    print("\n====== 檢測報告 ======")
    print(f"掃描格子數: {checked_cells}")
    if violations == 0:
        print("✅ 完美！沒有發現任何違規重疊。")
    else:
        print(f"❌ 發現 {violations} 處違規重疊！")
        print("前 10 筆錯誤:")
        for msg in error_log:
            print(msg)
        print("...")

if __name__ == "__main__":
    debug_overlap()