import pandas as pd
import numpy as np
import os
import collections

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')

def load_map_data(filename):
    print(f"   📂 搜尋路徑: {DATA_MAP_DIR}")
    
    # 列出所有候選檔名
    candidates = [
        filename,
        filename.replace('.xlsx', '.csv'),
        filename + " - Sheet1.csv",
        filename.replace('.xlsx', '') + " - Sheet1.csv"
    ]
    
    for fname in candidates:
        path = os.path.join(DATA_MAP_DIR, fname)
        if os.path.exists(path):
            print(f"   📖 嘗試讀取: {fname}")
            try:
                if fname.endswith('.xlsx') or fname.endswith('.xls'):
                    return pd.read_excel(path, header=None).fillna(0).values
                else:
                    return pd.read_csv(path, header=None).fillna(0).values
            except Exception as e:
                print(f"      ❌ 讀取失敗: {e}")
                pass
    
    # 如果都找不到，列出資料夾內所有檔案幫忙除錯
    print("   ❌ 找不到任何可用的地圖檔。資料夾內現有檔案：")
    try:
        files = os.listdir(DATA_MAP_DIR)
        for f in files:
            if 'map' in f: print(f"      - {f}")
    except:
        print("      (無法讀取資料夾)")
        
    return None

def visualize_area(grid, center_r, center_c, radius=5):
    """畫出工作站周圍的小地圖"""
    rows, cols = grid.shape
    r_start = max(0, center_r - radius)
    r_end = min(rows, center_r + radius + 1)
    c_start = max(0, center_c - radius)
    c_end = min(cols, center_c + radius + 1)
    
    print(f"\n   🗺️ [工作站 ({center_r}, {center_c}) 周圍環境]:")
    print("      " + "".join([str(i%10) for i in range(c_start, c_end)]))
    for r in range(r_start, r_end):
        line = f"   {r:02d} "
        for c in range(c_start, c_end):
            val = grid[r][c]
            if r == center_r and c == center_c: char = '★' # 中心
            elif val == 1: char = '█' # 牆/料架
            elif val == 2: char = '@' # 其他工作站
            else: char = '.' # 空位
            line += char
        print(line)
    print("      (圖例: ★=本站, █=障礙, @=他站, .=空位)")

def analyze_floor(floor_name, map_filename):
    print(f"\n{'='*40}")
    print(f"🔍 分析 {floor_name} 地圖空位與堵塞狀況...")
    print(f"{'='*40}")
    
    grid = load_map_data(map_filename)
    if grid is None:
        return

    rows, cols = grid.shape
    print(f"   -> 地圖尺寸: {rows}x{cols}")
    
    # 1. 找出所有工作站
    stations = []
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == 2:
                stations.append((r, c))
    
    print(f"   -> 發現 {len(stations)} 個工作站點位")
    if not stations: return

    # 2. 隨機選一個工作站來視覺化 (看看是不是被包圍)
    sample_st = stations[0]
    visualize_area(grid, sample_st[0], sample_st[1], radius=6)

    # 3. 模擬「找停車位」壓力測試 (BFS)
    # 假設所有 AGV 同時要找位子，我們看看第 36 台車要跑多遠
    
    queue = collections.deque(stations) # 從所有工作站同時出發
    visited = set(stations)
    found_slots = [] # 紀錄找到的空位距離
    
    # 距離圖
    distance_map = {} 
    for st in stations: distance_map[st] = 0
    
    # 開始擴散搜尋
    while queue and len(found_slots) < 200: # 找前 200 個位子
        curr = queue.popleft()
        r, c = curr
        dist = distance_map[curr]
        
        # 如果是空位 (0)，記錄下來
        if grid[r][c] == 0:
            found_slots.append(dist)
        
        # 往四面八方走
        for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
            nr, nc = r+dr, c+dc
            if 0 <= nr < rows and 0 <= nc < cols:
                if (nr, nc) not in visited:
                    val = grid[nr][nc]
                    # 只有 0(空地) 和 2(工作站) 可通行，1(障礙) 不可走
                    if val != 1: 
                        visited.add((nr, nc))
                        distance_map[(nr, nc)] = dist + 1
                        queue.append((nr, nc))

    # 4. 分析報告
    if not found_slots:
        print("\n   ❌ [嚴重] 工作站周圍完全被封死！找不到任何空位！")
        return

    print("\n   📊 [壓力測試報告] (假設 AGV 從工作站出發找位子)")
    
    # 第 1 台車 (最近的位子)
    print(f"      - 第 1 台車 (最佳位子): 需走 {found_slots[0]} 格")
    
    # 第 18 台車 (假設該樓層有一半的車回來)
    idx_18 = 17 if len(found_slots) > 17 else len(found_slots)-1
    print(f"      - 第 18 台車 (半數歸還): 需走 {found_slots[idx_18]} 格")
    
    # 第 36 台車 (假設全部車都擠回來)
    idx_36 = 35 if len(found_slots) > 35 else len(found_slots)-1
    dist_36 = found_slots[idx_36]
    print(f"      - 第 36 台車 (全滿歸還): 需走 {dist_36} 格")
    
    print("\n   💡 [診斷結論]")
    if dist_36 > 30:
        print("      ⚠️  **極度擁擠！**")
        print("      第 36 台車必須跑 30 格以上才能找到位子。")
        print("      這證實了為什麼車子會「往下飄」——因為近處都被前 35 台車停滿了，")
        print("      或者是地形本身就被料架 (█) 包圍，導致出不去。")
    elif dist_36 > 15:
        print("      ⚠️  **稍微擁擠**")
        print("      車子需要跑一段路才能停車，可能會導致工作站周圍小塞車。")
    else:
        print("      ✅ **空間充足**")
        print("      工作站周圍很空曠，如果還會亂跑，那就是程式邏輯 (Bug) 的問題，不是地形問題。")

if __name__ == "__main__":
    analyze_floor('2F', '2F_map.xlsx')
    analyze_floor('3F', '3F_map.xlsx')