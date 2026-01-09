import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')

def analyze_audit():
    print("🕵️‍♂️ [座標與視覺化法醫驗證] 啟動調查...\n")
    
    # 1. 檢查事件檔 (Step 4 的產出)
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path):
        print("❌ 找不到 simulation_events.csv")
        return
        
    df = pd.read_csv(evt_path)
    moves = df[df['type'] == 'AGV_MOVE']
    
    if moves.empty:
        print("❌ 沒有移動資料")
        return
        
    # 取得 AGV 實際跑到的最大/最小座標
    # 注意：在程式邏輯中，Col=X, Row=Y
    # 在 csv 中，我們寫入的是 sx, sy, ex, ey
    
    max_x = max(moves['ex'].max(), moves['sx'].max())
    max_y = max(moves['ey'].max(), moves['sy'].max())
    min_x = min(moves['ex'].min(), moves['sx'].min())
    min_y = min(moves['ey'].min(), moves['sy'].min())
    
    print(f"📊 [模擬數據事實]")
    print(f"   AGV 移動範圍 X (Column): {min_x} ~ {max_x}")
    print(f"   AGV 移動範圍 Y (Row)   : {min_y} ~ {max_y}")
    
    # 判斷 Step 4 是否成功限制邊界
    print("\n⚖️ [Step 4 判決]")
    if max_y >= 32:
        print(f"   ❌ 有罪！Step 4 依然產生了 Y={max_y} 的座標 (超過 31)。")
        print("      這代表 AGV 真的跑到了地圖下方的虛空區。")
    else:
        print(f"   ✅ 無罪！Step 4 產出的座標完全限制在 0~31 之間。")
        print("      如果畫面看起來 AGV 往下跑，那是「Step 5 視覺化」畫錯了。")

    # 2. 檢查地圖檔 (Step 5 讀取的來源)
    print("\n🗺️ [地圖檔案檢查]")
    map_file = '2F_map.xlsx'
    path = os.path.join(DATA_MAP_DIR, map_file)
    
    try:
        # 讀取原始 Excel (模擬 Step 5 的行為)
        df_map = pd.read_excel(path, header=None)
        raw_shape = df_map.shape
        
        # 嘗試讀取裁切後的 (模擬 value=0 的分佈)
        grid = df_map.fillna(0).values
        
        print(f"   原始 Excel 尺寸: {raw_shape[0]} 列 x {raw_shape[1]} 行")
        
        # 檢查下方是否有 '0' (Step 5 會把這些畫成白色的路)
        if raw_shape[0] > 32:
            bottom_area = grid[32:, :]
            zeros = np.sum(bottom_area == 0)
            if zeros > 0:
                print(f"   ⚠️ 警告：地圖第 32 列之後，還有 {zeros} 格被讀取為 '0' (空地)。")
                print("      Step 5 會把這些畫出來，導致畫面下半部有一大片白色區域。")
                
                if max_y < 32:
                    print("\n🎯 [關鍵結論]")
                    print("      AGV 座標正確 (都在上方)，但地圖畫太大 (下方有留白)。")
                    print("      視覺上 AGV 應該是集中在畫面上方，下方空蕩蕩。")
                    print("      -> 如果您看到 AGV 跑去下方，那可能是視覺化的 Y 軸翻轉或縮放錯誤。")
                else:
                    print("\n🎯 [關鍵結論]")
                    print("      AGV 座標錯誤 (跑出去了) + 地圖也有留白。")
                    print("      這是 Step 4 的 A* 演算法依然找到了通往地獄的道路。")
                    
    except Exception as e:
        print(f"   讀取地圖失敗: {e}")

if __name__ == "__main__":
    analyze_audit()