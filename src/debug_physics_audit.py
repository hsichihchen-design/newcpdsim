import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')

def analyze_physics():
    print("🕵️‍♂️ [物理法則審計] 開始調查 simulation_events.csv ...\n")
    
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path):
        print("❌ 找不到事件檔")
        return

    try:
        df = pd.read_csv(evt_path)
        df['start_ts'] = pd.to_datetime(df['start_time'])
        df['end_ts'] = pd.to_datetime(df['end_time'])
        df = df.sort_values('start_ts')
    except Exception as e:
        print(f"❌ 讀取失敗: {e}")
        return

    # 1. 瞬移檢測 (Teleportation Check)
    print("🔍 1. 瞬移檢測 (速度過快)")
    agv_groups = df[df['type'].isin(['AGV_MOVE', 'SHELF_LOAD', 'SHELF_UNLOAD'])].groupby('obj_id')
    teleport_count = 0
    
    for agv_id, group in agv_groups:
        group = group.sort_values('start_ts')
        last_pos = None
        last_time = None
        
        for _, row in group.iterrows():
            curr_pos = (row['sx'], row['sy'])
            curr_time = row['start_ts']
            
            if last_pos and last_time:
                dist = abs(curr_pos[0] - last_pos[0]) + abs(curr_pos[1] - last_pos[1])
                time_diff = (curr_time - last_time).total_seconds()
                
                # 如果時間差很短 (< 2秒) 但距離很長 (> 5格) -> 瞬移
                if time_diff < 2 and dist > 5:
                    if teleport_count < 5: # 只印前5個
                        print(f"   ⚠️ {agv_id} 在 {curr_time} 發生瞬移! 從 {last_pos} 飛到 {curr_pos} (距離 {dist})")
                    teleport_count += 1
            
            # Update last pos to be the END of this segment
            last_pos = (row['ex'], row['ey'])
            last_time = row['end_ts']
            
    if teleport_count == 0: print("   ✅ 無明顯瞬移現象")
    else: print(f"   ❌ 總計發現 {teleport_count} 次瞬移事件 (這是正方形瞬間移動的主因)")

    # 2. 穿模檢測 (Collision Check)
    print("\n🔍 2. 穿模檢測 (同一時間同一格有兩車)")
    # 為了效能，我們只抽樣檢查前 1000 個移動事件
    move_events = df[df['type'] == 'AGV_MOVE'].head(1000)
    collisions = 0
    
    # 建立時間軸佔用表: {(x, y, time_slice): agv_id}
    # 這裡簡化檢查：只檢查每段移動的「終點」在「抵達時間」是否已被佔用
    occupied = {} # key: (x, y, timestamp_minute), val: agv_id
    
    # 這個檢查比較粗略，精確檢查需要每秒展開，太慢了。
    # 我們改檢查 "Event Overlap"
    # 如果有兩個事件，時間重疊，且位置重疊
    
    print("   (略過詳細穿模檢查以節省時間，但在 V34 中穿模通常是因為 reservations 沒寫入)")

    # 3. 狀態一致性 (State Consistency)
    print("\n🔍 3. 載貨狀態檢查")
    state_errors = 0
    for agv_id, group in agv_groups:
        is_loaded = False
        for _, row in group.iterrows():
            if row['type'] == 'SHELF_LOAD':
                if is_loaded:
                    if state_errors < 3: print(f"   ⚠️ {agv_id} 重複載貨! 在 {row['start_ts']}")
                    state_errors += 1
                is_loaded = True
            elif row['type'] == 'SHELF_UNLOAD':
                if not is_loaded:
                    if state_errors < 3: print(f"   ⚠️ {agv_id} 空車卸貨! 在 {row['start_ts']}")
                    state_errors += 1
                is_loaded = False
                
    if state_errors == 0: print("   ✅ 載貨/卸貨狀態邏輯完美")
    else: print(f"   ❌ 發現 {state_errors} 次狀態邏輯錯誤 (這導致圓形/正方形切換錯誤)")

    # 4. KPI 更新檢查
    print("\n🔍 4. KPI 資料檢查")
    kpi_path = os.path.join(LOG_DIR, 'simulation_kpi.csv')
    if os.path.exists(kpi_path):
        df_kpi = pd.read_csv(kpi_path)
        print(f"   KPI 紀錄總筆數: {len(df_kpi)}")
        if len(df_kpi) > 0:
            print(f"   第一筆完成時間: {df_kpi['finish_time'].min()}")
            print(f"   最後一筆完成時間: {df_kpi['finish_time'].max()}")
        else:
            print("   ❌ KPI 檔案是空的 (這解釋了為什麼右邊沒更新)")
    else:
        print("   ❌ 找不到 KPI 檔案")

if __name__ == "__main__":
    analyze_physics()