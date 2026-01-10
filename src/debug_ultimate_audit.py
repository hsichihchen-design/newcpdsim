import pandas as pd
import numpy as np
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DATA_DIR = os.path.join(BASE_DIR, 'data', 'master')

def load_map(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path): return None
    try:
        df = pd.read_excel(path, header=None)
        # 轉成 2D array，空值補 0 (假設空值是路)，-1 是牆
        return df.fillna(0).values
    except: return None

def check_physics():
    print("🕵️‍♂️ [終極物理審計] 正在分析 simulation_events.csv ...")
    
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path): return

    df = pd.read_csv(evt_path)
    df['start_ts'] = pd.to_datetime(df['start_time'])
    df['end_ts'] = pd.to_datetime(df['end_time'])
    df = df.sort_values('start_ts')
    
    # Load Maps
    map_2f = load_map('2F_map.xlsx')
    map_3f = load_map('3F_map.xlsx')
    maps = {'2F': map_2f, '3F': map_3f}
    
    errors = {
        'teleport': 0,
        'wall_clip': 0,
        'overlap': 0
    }
    
    # 1. 軌跡檢查 (瞬移 + 撞牆)
    print("🔍 1. 軌跡檢查 (瞬移 & 撞牆)...")
    agv_groups = df[df['type'] == 'AGV_MOVE'].groupby('obj_id')
    
    for agv_id, group in agv_groups:
        last_pos = None
        last_time = None
        
        for _, row in group.iterrows():
            floor = row['floor']
            grid = maps.get(floor)
            
            curr_pos = (int(row['sx']), int(row['sy']))
            end_pos = (int(row['ex']), int(row['ey']))
            
            # A. 撞牆檢查 (起點或終點在牆上)
            if grid is not None:
                # 檢查起點
                if 0 <= curr_pos[0] < grid.shape[0] and 0 <= curr_pos[1] < grid.shape[1]:
                    if grid[curr_pos[0]][curr_pos[1]] == -1:
                        if errors['wall_clip'] < 5: print(f"   🧱 [撞牆] {agv_id} @ {row['start_time']} 位於牆壁 {curr_pos}")
                        errors['wall_clip'] += 1
                
                # 檢查路徑中間 (簡易版：只檢查終點)
                if 0 <= end_pos[0] < grid.shape[0] and 0 <= end_pos[1] < grid.shape[1]:
                    if grid[end_pos[0]][end_pos[1]] == -1:
                        if errors['wall_clip'] < 5: print(f"   🧱 [撞牆] {agv_id} @ {row['end_time']} 撞進牆壁 {end_pos}")
                        errors['wall_clip'] += 1

            # B. 瞬移檢查
            if last_pos and last_time:
                dist = abs(curr_pos[0] - last_pos[0]) + abs(curr_pos[1] - last_pos[1])
                dt = (row['start_ts'] - last_time).total_seconds()
                
                # 允許 2 秒誤差，如果距離超過 3 格且時間極短
                if dt < 1.0 and dist > 2:
                    if errors['teleport'] < 5: print(f"   ⚡ [瞬移] {agv_id} 從 {last_pos} 瞬移到 {curr_pos} (距離 {dist}, 時間 {dt}s)")
                    errors['teleport'] += 1
            
            last_pos = end_pos
            last_time = row['end_ts']

    # 2. 訂單數檢查
    print("\n🔍 2. 訂單完整性檢查...")
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    if os.path.exists(kpi_path):
        df_kpi = pd.read_csv(kpi_path)
        total_kpi = len(df_kpi)
        print(f"   📊 KPI 紀錄總數: {total_kpi}")
        if total_kpi < 20000:
            print(f"   ⚠️ 警告：訂單數 ({total_kpi}) 少於預期 (約 20117)。這是波次數字怪怪的主因。")
        else:
            print(f"   ✅ 訂單數正常。")
    
    print("\n====== 審計結果 ======")
    print(f"瞬移事件: {errors['teleport']}")
    print(f"撞牆事件: {errors['wall_clip']}")
    print(f"======================")

if __name__ == "__main__":
    check_physics()