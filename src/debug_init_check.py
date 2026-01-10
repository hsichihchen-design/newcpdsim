import pandas as pd
import numpy as np
import os
import random

# ================= 設定 =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAP_FILE = '2F_map.xlsx' # 預設檢查 2F
SHELF_COORD_FILE = 'shelf_coordinate_map.csv'

# ================= 載入函式 =================
def load_map_strict(filename, rows, cols):
    path = os.path.join(BASE_DIR, 'data', 'master', filename)
    if not os.path.exists(path):
        path = path.replace('.xlsx', '.csv')
        if not os.path.exists(path):
            print(f"❌ 找不到地圖檔案: {path}")
            return None

    print(f"📖 讀取地圖: {path}")
    try:
        if filename.endswith('.xlsx'):
            df = pd.read_excel(path, header=None)
        else:
            df = pd.read_csv(path, header=None)
            
        # 1. 檢查原始尺寸
        print(f"   -> 原始 Excel/CSV 尺寸: {df.shape}")
        
        # 2. 強制裁切與填補
        raw_grid = df.iloc[0:rows, 0:cols].fillna(0).values
        
        # 3. 建立最終網格 (預設 -1 牆壁)
        final_grid = np.full((rows, cols), -1.0)
        
        # 4. 填入數據
        r_in = min(raw_grid.shape[0], rows)
        c_in = min(raw_grid.shape[1], cols)
        final_grid[0:r_in, 0:c_in] = raw_grid[0:r_in, 0:c_in]
        
        return final_grid
    except Exception as e:
        print(f"❌ 地圖讀取失敗: {e}")
        return None

def load_shelf_coords():
    path = os.path.join(BASE_DIR, 'data', 'mapping', SHELF_COORD_FILE)
    coords = {}
    if not os.path.exists(path):
        print(f"❌ 找不到座標檔: {path}")
        return {}
    
    print(f"📖 讀取料架座標: {path}")
    df = pd.read_csv(path)
    for _, r in df.iterrows():
        # 注意：這裡使用修正後的 (y, x) 對應 (Row, Col)
        # 假設 csv 欄位是 x, y
        if r['floor'] == '2F':
            coords[str(r['shelf_id'])] = (int(r['y']), int(r['x']))
    return coords

def check_system():
    print("🚀 開始系統靜態體檢...\n")
    
    # 1. 檢查地圖
    ROWS, COLS = 32, 61
    grid = load_map_strict(MAP_FILE, ROWS, COLS)
    if grid is None: return

    print(f"✅ 地圖矩陣建立完成。形狀: {grid.shape}")
    print(f"   -> 期望: (32, 61)")
    print(f"   -> 實際: {grid.shape}")
    
    # 統計地圖元素
    unique, counts = np.unique(grid, return_counts=True)
    elements = dict(zip(unique, counts))
    print(f"   -> 地圖內容統計: {elements}")
    print("      (-1:牆壁, 0:走道, 1:料架區, 2:工作站)")

    # 2. 檢查料架位置
    print("\n🔍 檢查料架位置 (2F)...")
    shelves = load_shelf_coords()
    print(f"   -> 2F 總料架數: {len(shelves)}")
    
    valid_count = 0
    wall_count = 0
    out_of_bounds = 0
    
    for sid, pos in shelves.items():
        r, c = pos
        if 0 <= r < ROWS and 0 <= c < COLS:
            val = grid[r][c]
            if val == -1:
                wall_count += 1
                if wall_count <= 5: # 只印出前5個錯誤
                    print(f"      ❌ 料架 {sid} 在牆壁內! 座標 ({r}, {c})")
            else:
                valid_count += 1
        else:
            out_of_bounds += 1
            if out_of_bounds <= 5:
                print(f"      ❌ 料架 {sid} 超出地圖邊界! 座標 ({r}, {c})")

    print(f"   -> ✅ 正常料架: {valid_count}")
    print(f"   -> ❌ 牆壁內料架: {wall_count} (這應該要是 0)")
    print(f"   -> ❌ 界外料架: {out_of_bounds} (這應該要是 0)")

    if wall_count > 0:
        print("\n⚠️ 警告: 發現料架位於牆壁內！這表示地圖讀取偏移，或座標檔 X/Y 反了。")

    # 3. 檢查 AGV 初始位置
    print("\nTw 檢查 AGV 生成邏輯...")
    agv_positions = []
    # 模擬生成 20 台
    candidates = []
    for r in range(ROWS):
        for c in range(COLS):
            if grid[r][c] == 0: candidates.append((r,c)) # 優先通道
    
    if not candidates:
        print("   ⚠️ 警告: 沒有發現 '0' (通道)，嘗試使用 '1' (料架區)")
        for r in range(ROWS):
            for c in range(COLS):
                if grid[r][c] == 1: candidates.append((r,c))
    
    print(f"   -> 可用生成點數量: {len(candidates)}")
    
    if len(candidates) > 0:
        random.shuffle(candidates)
        agvs = candidates[:10]
        agv_wall_hits = 0
        for i, pos in enumerate(agvs):
            r, c = pos
            val = grid[r][c]
            status = "✅ OK" if val != -1 else "❌ WALL"
            if val == -1: agv_wall_hits += 1
            print(f"      AGV_{i+1} 生成於 ({r}, {c}) -> 地圖數值: {val} {status}")
            
        if agv_wall_hits == 0:
            print("   -> ✅ 所有測試 AGV 均生成在合法區域。")
        else:
            print("   -> ❌ AGV 生成位置有誤！")
    else:
        print("   -> ❌ 無法生成 AGV (無可用點)！")

if __name__ == "__main__":
    check_system()