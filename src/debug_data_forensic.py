import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
MAP_DIR = os.path.join(BASE_DIR, 'data', 'mapping')

def forensic_analysis():
    print("🕵️‍♂️ 啟動資料鑑識 (Data Forensic)...")
    
    # 1. 檢查地圖座標檔 (Step 1 產出的)
    shelf_map_path = os.path.join(MAP_DIR, 'shelf_coordinate_map.csv')
    if os.path.exists(shelf_map_path):
        print(f"\n1. 檢查座標映射表 ({os.path.basename(shelf_map_path)}):")
        df_map = pd.read_csv(shelf_map_path)
        print(f"   -> 總料架數: {len(df_map)}")
        print(f"   -> 座標範例 (前 5 筆):")
        print(df_map[['shelf_id', 'floor', 'x', 'y']].head(5).to_string(index=False))
        
        # 統計座標分佈
        unique_x = df_map['x'].unique()
        unique_y = df_map['y'].unique()
        print(f"   -> X (Row?) 分佈範圍: {min(unique_x)} ~ {max(unique_x)} (共 {len(unique_x)} 種值)")
        print(f"   -> Y (Col?) 分佈範圍: {min(unique_y)} ~ {max(unique_y)} (共 {len(unique_y)} 種值)")
        
        if len(unique_x) < 5 or len(unique_y) < 5:
            print("   ⚠️ 警訊：座標值的變化太少！這代表所有料架可能都疊在一起。")
    else:
        print("   ❌ 找不到座標表，請重新執行 Step 1")

    # 2. 檢查事件 Log (Step 4 產出的)
    events_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if os.path.exists(events_path):
        print(f"\n2. 檢查移動事件 ({os.path.basename(events_path)}):")
        df_evt = pd.read_csv(events_path)
        moves = df_evt[df_evt['type'] == 'AGV_MOVE']
        
        if moves.empty:
            print("   ❌ 沒有任何移動事件！")
        else:
            print(f"   -> 總移動次數: {len(moves)}")
            print("   -> 移動範例 (前 5 筆):")
            print(moves[['floor', 'obj_id', 'sx', 'sy', 'ex', 'ey']].head(5).to_string(index=False))
            
            # 檢查是否真的有移動 (起點 != 終點)
            static_moves = moves[(moves['sx'] == moves['ex']) & (moves['sy'] == moves['ey'])]
            print(f"   -> 原地踏步的移動數: {len(static_moves)} (佔 {len(static_moves)/len(moves)*100:.1f}%)")
            
            if len(static_moves) > len(moves) * 0.9:
                print("   ⚠️ 警訊：90% 以上的移動都是原地踏步！難怪車子看起來不動。")
            else:
                print("   ✅ 資料顯示車子確實有改變座標，問題出在視覺化縮放。")
    else:
        print("   ❌ 找不到事件 Log，請重新執行 Step 4")

if __name__ == "__main__":
    forensic_analysis()