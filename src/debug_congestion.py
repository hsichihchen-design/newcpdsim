import pandas as pd
import numpy as np
import os
from collections import Counter

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')

def load_map_shape(filename):
    path = os.path.join(DATA_MAP_DIR, filename)
    if os.path.exists(path):
        try: return pd.read_excel(path, header=None).shape
        except: pass
    csv_path = path.replace('.xlsx', '.csv')
    if os.path.exists(csv_path):
        try: return pd.read_csv(csv_path, header=None).shape
        except: pass
    return (0,0)

def analyze_congestion():
    print("🔍 [塞車與路障分析] 啟動...")
    
    evt_path = os.path.join(LOG_DIR, 'simulation_events.csv')
    if not os.path.exists(evt_path):
        print("❌ 找不到事件檔")
        return

    df = pd.read_csv(evt_path)
    
    # 1. 找出瞬移事件 (Teleports)
    # 定義: 距離 > 2 但時間很短，或標記為 move 但跨度大
    teleports = []
    
    df['dist'] = abs(df['ex'] - df['sx']) + abs(df['ey'] - df['sy'])
    df['duration'] = (pd.to_datetime(df['end_time']) - pd.to_datetime(df['start_time'])).dt.total_seconds()
    
    # 正常走路 1格/秒。如果速度 > 2格/秒 且距離 > 5，判定為瞬移
    teleport_mask = (df['type'] == 'AGV_MOVE') & (df['dist'] > 5) & (df['dist'] / df['duration'] > 1.5)
    teleport_df = df[teleport_mask]
    
    print(f"   -> 偵測到 {len(teleport_df)} 次瞬移事件")
    
    if teleport_df.empty:
        print("   ✅ 沒有發現明顯的瞬移，可能是其他問題。")
        return

    # 2. 分析瞬移的目的地 (AGV 想去哪裡但去不了?)
    target_counts = Counter()
    for _, row in teleport_df.iterrows():
        target_counts[(row['floor'], row['ex'], row['ey'])] += 1
        
    print("\n   🚧 [Top 5 堵塞目的地] (AGV 想去這裡但失敗了):")
    for (floor, x, y), count in target_counts.most_common(5):
        print(f"      {floor} ({x}, {y}) - 失敗 {count} 次")

    # 3. 分析閒置車輛位置 (是誰擋路?)
    # 我們看所有任務結束後的 Dropoff 位置
    # 簡單起見，我們看所有 AGV_MOVE 的終點，如果是長時間停留的
    
    # 這裡我們換個策略：檢查瞬移發生最頻繁的區域周圍，是不是有很多車停著
    # 取第一名堵塞點
    if target_counts:
        top_blocked = target_counts.most_common(1)[0][0] # (floor, x, y)
        floor, tx, ty = top_blocked
        
        print(f"\n   🕵️‍♂️ [現場還原] 檢查熱點 {floor} ({tx}, {ty}) 周圍...")
        
        # 找出在這個樓層，且終點在這個區域附近的移動事件
        nearby_mask = (df['floor'] == floor) & \
                      (df['ex'].between(tx-5, tx+5)) & \
                      (df['ey'].between(ty-5, ty+5))
        
        nearby_events = df[nearby_mask]
        print(f"      -> 該區域共有 {len(nearby_events)} 次進出紀錄")
        
        # 檢查該區域是不是工作站
        stations = df[df['obj_id'].str.startswith('WS_')]
        st_nearby = stations[
            (stations['floor'] == floor) & 
            (stations['sx'].between(tx-2, tx+2)) & 
            (stations['sy'].between(ty-2, ty+2))
        ]
        
        if not st_nearby.empty:
            print(f"      🚨 確認：該區域附近有工作站！ ({st_nearby['obj_id'].unique()})")
            print("      👉 高機率是閒置 AGV 停在工作站門口，把路封死了。")
        else:
            print("      -> 附近沒有工作站，可能是窄巷道被堵死。")

    print("\n   💡 診斷結論：")
    print("      如果上述熱點是工作站，且瞬移次數極高，")
    print("      代表 AGV 做完事後「就地停車」，形成了路障。")
    print("      解法：強迫 AGV 歸還料架後，移動到「遠離工作站」的休息區。")

if __name__ == "__main__":
    analyze_congestion()