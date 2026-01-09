import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

def load_map(filename):
    path = os.path.join(DATA_MAP_DIR, filename)
    if os.path.exists(path):
        try: return pd.read_excel(path, header=None).fillna(0).values
        except: pass
    csv_path = path.replace('.xlsx', '.csv')
    if os.path.exists(csv_path):
        try: return pd.read_csv(csv_path, header=None).fillna(0).values
        except: pass
    return None

def check_1_map_integrity():
    print("\n🔍 [1. 地圖與座標映射檢查]")
    
    # 1. 讀取座標表
    map_file = os.path.join(MAPPING_DIR, 'shelf_coordinate_map.csv')
    if not os.path.exists(map_file):
        print("   ❌ 找不到 shelf_coordinate_map.csv")
        return
    
    df_coord = pd.read_csv(map_file)
    print(f"   -> 座標表共有 {len(df_coord)} 筆資料")
    
    # 2. 驗證座標是否合法
    maps = {'2F': load_map('2F_map.xlsx'), '3F': load_map('3F_map.xlsx')}
    
    for floor, grid in maps.items():
        if grid is None:
            print(f"   ⚠️ 無法讀取 {floor} 地圖檔")
            continue
            
        rows, cols = grid.shape
        df_floor = df_coord[df_coord['floor'] == floor]
        
        # 檢查邊界
        out_of_bounds = df_floor[
            (df_floor['y'] < 0) | (df_floor['y'] >= rows) |
            (df_floor['x'] < 0) | (df_floor['x'] >= cols)
        ]
        
        if not out_of_bounds.empty:
            print(f"   ❌ {floor} 有 {len(out_of_bounds)} 筆座標超出地圖邊界！")
            print(out_of_bounds.head(3))
        else:
            print(f"   ✅ {floor} 所有座標皆在地圖範圍內 ({rows}x{cols})")
            
        # 檢查地形 (是否放在牆壁或虛空上?)
        # 注意: x是對應 column, y是對應 row
        invalid_spots = 0
        for _, r in df_floor.iterrows():
            val = grid[int(r['y'])][int(r['x'])]
            # 假設 1=料架, 0=走道. 如果 mapping 指向 -1 (牆) 或其他怪數字就是錯的
            if val == -1: 
                invalid_spots += 1
        
        if invalid_spots > 0:
            print(f"   ⚠️ {floor} 有 {invalid_spots} 個料架被設定在「牆壁 (-1)」上！AGV 無法抵達。")
        else:
            print(f"   ✅ {floor} 料架位置地形檢核通過。")

def check_2_agv_behavior():
    print("\n🔍 [2. AGV 行為與軌跡檢查]")
    
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path):
        print("   ❌ 找不到 simulation_events.csv")
        return
        
    df = pd.read_csv(evt_path)
    moves = df[df['type'] == 'AGV_MOVE']
    
    if moves.empty:
        print("   ❌ 沒有任何 AGV 移動紀錄。")
        return
        
    # 檢查 "瞬移" (Teleport)
    # 定義：如果單次移動距離 > 1 (正常是一格一格走)，或者時間跨度極大但只有兩點
    # 這裡我們檢查每個事件的 (起點 -> 終點) 距離 vs 時間
    
    teleport_count = 0
    normal_count = 0
    
    for _, row in moves.iterrows():
        dist = abs(row['ex'] - row['sx']) + abs(row['ey'] - row['sy'])
        duration = (pd.to_datetime(row['end_time']) - pd.to_datetime(row['start_time'])).total_seconds()
        
        # 正常情況：移動 1 格約需 1 秒 (速度=1)
        # 如果移動了 10 格，卻只花 1 秒 -> 飛過去的
        # 如果移動了 50 格，花了 300 秒，但只有這一筆事件 -> 這是 "找不到路" 的 Fallback
        
        # 檢查是否為 "Fallback Path" (通常只有起點終點，距離很長)
        # 正常的 A* 路徑會被切分成很多小段 (每段 dist=1)
        if dist > 1.5: 
            teleport_count += 1
        else:
            normal_count += 1
            
    print(f"   -> 正常移動步數 (1格/步): {normal_count}")
    print(f"   -> 瞬移/長距離移動 (Teleport): {teleport_count}")
    
    if teleport_count > 0:
        print(f"   ❌ 嚴重警告：發現 {teleport_count} 次瞬移！")
        print("      這代表 A* 演算法「找不到路」，觸發了保底機制 (直接飛到目的地)。")
        print("      原因可能是：地圖被牆壁封死、起點/終點在牆壁裡、或者路被其他 AGV 堵死。")
    else:
        print("   ✅ AGV 移動軌跡看起來是連續的。")

def check_3_visualization_data():
    print("\n🔍 [3. 視覺化資料檢查]")
    # 檢查 Step 5 讀取的資料是否合理
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if os.path.exists(evt_path):
        df = pd.read_csv(evt_path)
        print(f"   -> 事件總數: {len(df)}")
        print(f"   -> AGV 數量: {df[df['type']=='AGV_MOVE']['obj_id'].nunique()}")
        print(f"   -> 時間範圍: {df['start_time'].min()} ~ {df['end_time'].max()}")
    else:
        print("   ❌ 無法讀取事件檔")

if __name__ == "__main__":
    check_1_map_integrity()
    check_2_agv_behavior()
    check_3_visualization_data()