import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

def load_map_matrix(filename):
    path = os.path.join(DATA_MAP_DIR, filename)
    if os.path.exists(path):
        try: return pd.read_excel(path, header=None).fillna(0).values
        except: pass
    csv_path = path.replace('.xlsx', '.csv')
    if os.path.exists(csv_path):
        try: return pd.read_csv(csv_path, header=None).fillna(0).values
        except: pass
    return None

def main():
    print("🔍 [地圖視覺化診斷] 電腦到底把哪裡當成路？")
    
    # 1. 讀取地圖
    grid_2f = load_map_matrix('2F_map.xlsx')
    if grid_2f is None:
        print("❌ 無法讀取 2F 地圖")
        return

    rows, cols = grid_2f.shape
    print(f"   -> 2F 地圖尺寸: {rows} 列 x {cols} 行")
    
    # 2. 統計實際可行走區域 (Value = 0)
    walkable_count = np.sum(grid_2f == 0)
    wall_count = np.sum(grid_2f == 1)
    station_count = np.sum(grid_2f == 2)
    
    print(f"   -> 可行走空地 (0): {walkable_count} 格")
    print(f"   -> 障礙物/牆壁 (1): {wall_count} 格")
    print(f"   -> 工作站 (2): {station_count} 格")
    
    # 3. 讀取 AGV 移動紀錄，看看它們都去哪
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    heatmap = np.zeros_like(grid_2f, dtype=int)
    
    if os.path.exists(evt_path):
        df = pd.read_csv(evt_path)
        # 只看 2F 的移動終點
        moves = df[(df['type'] == 'AGV_MOVE') & (df['floor'] == '2F')]
        
        for _, row in moves.iterrows():
            try:
                r, c = int(row['ey']), int(row['ex']) # 注意：Row=Y, Col=X
                if 0 <= r < rows and 0 <= c < cols:
                    heatmap[r, c] += 1
            except: pass
            
    # 4. 輸出文字版地圖 (簡化版)
    # 我們把地圖切成區塊，看看哪裡是 "0" (空地)
    print("\n🗺️ [地圖結構快照] ('.' = 空地/路, '#' = 牆, '@' = 工作站)")
    print("   注意看下方是否全是 '.' (空地)\n")
    
    # 為了避免洗版，我們每 2 列取樣一次，每 2 行取樣一次
    for r in range(0, rows, 1):
        line = f"{r:02d} | "
        for c in range(0, cols, 1):
            val = grid_2f[r][c]
            visits = heatmap[r][c]
            
            char = ' '
            if val == 1: char = '█' # Wall
            elif val == 2: char = '@' # Station
            elif val == 0: 
                # 如果是空地，且有 AGV 去過，標記為 '*'
                if visits > 50: char = 'X' # 熱點
                elif visits > 0: char = '.' # 有人走過
                else: char = '_' # 沒人走過的空地
            
            line += char
        print(line)
        
    print("\n圖例說明：")
    print("█ : 牆壁 (不可走)")
    print("@ : 工作站")
    print("X : AGV 塞車熱點 (路)")
    print(". : AGV 走過的路")
    print("_ : 沒人走的空地 (如果是這一大片在下方，代表那就是您看到的漂移區)")

    # 檢查是否有 "下半部全空" 的情況
    mid_row = rows // 2
    bottom_area = grid_2f[mid_row:, :]
    bottom_zeros = np.sum(bottom_area == 0)
    total_bottom = bottom_area.size
    
    if bottom_zeros / total_bottom > 0.8:
        print("\n⚠️ 警告：地圖下半部超過 80% 都是 '0' (空地)！")
        print("   這就是為什麼 AGV 會往下飄。因為上面擠滿了，演算法發現下面全是空位，就叫車子去那邊停。")
        print("   -> 解法：在 Excel 中，把非倉庫區域填滿 '1' (牆壁)。")

if __name__ == "__main__":
    main()