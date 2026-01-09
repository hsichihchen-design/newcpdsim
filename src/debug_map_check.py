import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')

def check_map(filename):
    print(f"🔍 正在檢查地圖: {filename} ...")
    path = os.path.join(DATA_MAP_DIR, filename)
    if not os.path.exists(path):
        print("   ❌ 檔案不存在！")
        return

    # 嘗試讀取
    df = pd.read_excel(path, header=None).fillna(0)
    grid = df.values
    
    # 強制轉型測試
    try:
        grid = grid.astype(int)
    except:
        print("   ⚠️ 警告: 地圖包含非數字字元，這可能導致判讀錯誤！")
    
    unique, counts = np.unique(grid, return_counts=True)
    stats = dict(zip(unique, counts))
    
    print(f"   -> 地圖大小: {grid.shape}")
    print(f"   -> 內容統計: {stats}")
    
    # 檢查關鍵物件
    ws_count = stats.get(2, 0) # 工作站
    shelf_count = stats.get(1, 0) # 料架
    
    if ws_count == 0:
        print("   ❌ 嚴重錯誤: 找不到任何工作站 (代號 2)！AGV 會因此卡在 (0,0)。")
        print("      請檢查 Excel 中工作站是否填寫正確，或是否被存為文字格式。")
    else:
        print(f"   ✅ 偵測到 {ws_count} 格工作站。")
        # 印出前幾個座標看看是否合理
        rows, cols = np.where(grid == 2)
        print(f"      範例座標 (Row, Col): {list(zip(rows[:3], cols[:3]))}")

    if shelf_count == 0:
        print("   ❌ 嚴重錯誤: 找不到任何料架 (代號 1)！")
    else:
        print(f"   ✅ 偵測到 {shelf_count} 格料架。")
    print("-" * 30)

if __name__ == "__main__":
    check_map('2F_map.xlsx')
    check_map('3F_map.xlsx')