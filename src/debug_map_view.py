import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')

def load_map_data(filename):
    candidates = [filename, filename.replace('.xlsx', '.csv')]
    for fname in candidates:
        path = os.path.join(DATA_MAP_DIR, fname)
        if os.path.exists(path):
            try:
                if fname.endswith('.xlsx'): return pd.read_excel(path, header=None).fillna(0).values
                else: return pd.read_csv(path, header=None).fillna(0).values
            except: pass
    return None

def main():
    print("🔍 [地圖透視鏡] 電腦眼中的世界...")
    
    # 1. 載入地圖
    grid = load_map_data('2F_map.xlsx')
    if grid is None: return
    
    # 2. 載入料架位置
    shelf_map = np.zeros_like(grid)
    map_file = os.path.join(MAPPING_DIR, 'shelf_coordinate_map.csv')
    if os.path.exists(map_file):
        df = pd.read_csv(map_file)
        df = df[df['floor'] == '2F']
        for _, r in df.iterrows():
            try: shelf_map[int(r['y']), int(r['x'])] = 1
            except: pass

    rows, cols = grid.shape
    print(f"   地圖尺寸: {rows}x{cols}")
    print("   圖例: [.]=空地/路  [#]=牆壁  [@]=工作站  [S]=料架位置")
    print("-" * 60)

    # 為了版面，每 2 行印一次 (若是大圖)
    step = 1 
    for r in range(0, rows, step):
        line = f"{r:02d} | "
        for c in range(0, cols, step):
            val = grid[r][c]
            is_shelf = shelf_map[r][c] == 1
            
            if is_shelf: char = 'S'
            elif val == 1: char = '#'
            elif val == 2: char = '@'
            else: char = '.' # 這裡就是 AGV 會去「漂移」的地方
            
            line += char
        print(line)
    print("-" * 60)
    print("💡 觀察重點：")
    print("   1. 如果下方有一大片 '.'，代表那是合法的路，AGV 當然會去。")
    print("   2. 如果下方有 'S'，代表有料架在那裡，AGV 必須去那裡工作。")

if __name__ == "__main__":
    main()