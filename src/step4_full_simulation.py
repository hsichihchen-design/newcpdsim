import pandas as pd
import numpy as np
import os
import time
import heapq
import collections
from datetime import datetime, timedelta

# 設定路徑
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class TimeAwareAStar:
    """
    具備時空感知能力的 A* 演算法
    """
    def __init__(self, grid, reservations):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations # 引用全域預約表 (Set: (x, y, time_sec))

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def find_path(self, start, goal, start_time_sec):
        # Open set: (f_score, current_pos, arrival_time)
        open_set = []
        heapq.heappush(open_set, (0, start, start_time_sec))
        
        came_from = {}
        g_score = {(start, start_time_sec): 0}
        
        # 為了效能，設定搜尋上限
        max_depth = 3000
        steps = 0
        
        # AGV 速度設定 (秒/格)
        # 假設 1.5 m/s, 格子 1m -> 0.67s/格。為了預約表方便，這裡取整數 1秒/格
        STEP_COST = 1 

        while open_set:
            steps += 1
            if steps > max_depth: return None, None

            _, current, current_time = heapq.heappop(open_set)

            if current == goal:
                # 回溯路徑 (包含時間資訊)
                path = []
                trace = (current, current_time)
                while trace in came_from:
                    pos, t = trace
                    path.append((pos, t))
                    trace = came_from[trace]
                path.append((start, start_time_sec))
                path.reverse()
                return path, current_time

            # 探索鄰居
            # 包含：上下左右移動 (Cost=1) + 原地等待 (Cost=1)
            # 這裡為了簡化與效能，暫不加入「主動等待」節點，只允許繞路
            neighbors = [(0, 1), (0, -1), (1, 0), (-1, 0)]
            
            for dr, dc in neighbors:
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + STEP_COST
                
                # 1. 邊界檢查
                if 0 <= nr < self.rows and 0 <= nc < self.cols:
                    # 2. 靜態障礙檢查 (1=Shelf, 2=Station)
                    # 允許終點是障礙物
                    val = self.grid[nr][nc]
                    if val in [1, 2] and (nr, nc) != goal and (nr, nc) != start:
                        continue
                        
                    # 3. 動態預約檢查 (核心邏輯!)
                    # 如果下一個時間點，該位置已被佔用，則視為牆壁
                    if (nr, nc, next_time) in self.reservations:
                        # 這裡發生了！因為被佔用，A* 會認為這條路不通，被迫去選別的鄰居(繞路)
                        continue
                        
                    # A* 標準更新
                    tentative_g = g_score[(current, current_time)] + STEP_COST
                    if ((nr, nc), next_time) not in g_score or tentative_g < g_score[((nr, nc), next_time)]:
                        g_score[((nr, nc), next_time)] = tentative_g
                        f = tentative_g + self.heuristic((nr, nc), goal)
                        heapq.heappush(open_set, (f, (nr, nc), next_time))
                        came_from[((nr, nc), next_time)] = (current, current_time)
                        
        return None, None

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (Reservation Table Mode)...")
        print(f"   -> 啟用動態路徑規劃：遇到佔用會自動繞路")
        print(f"   -> 模擬真實的交通堵塞與迴避行為")
        
        self.PICK_TIME = 20
        
        # 1. 載入地圖
        self.grid_2f = self._load_map('2F_map.xlsx')
        self.grid_3f = self._load_map('3F_map.xlsx')
        
        # 2. 全域預約表 (Spatial-Temporal Hash Map)
        # 格式: Set -> (x, y, time_sec)
        # 用於快速查詢某個時間點某個位置是否有人
        self.reservations_2f = set()
        self.reservations_3f = set()
        
        # 3. 載入資料
        self.shelf_coords = self._load_shelf_coords()
        self.inventory_map = self._load_inventory()
        self.orders = self._load_orders()
        
        # 4. 資源狀態
        self.agv_state = {
            '2F': {i: 0 for i in range(1, 9)}, # 儲存 "最早可用時間 (秒)"
            '3F': {i: 0 for i in range(101, 109)}
        }
        # 工作站座標與狀態
        self.stations = self._init_stations()

    def _load_map(self, filename):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        try: return pd.read_excel(path, header=None).fillna(0).values
        except: return np.zeros((32, 61))

    def _load_shelf_coords(self):
        path = os.path.join(BASE_DIR, 'data', 'mapping', 'shelf_coordinate_map.csv')
        coords = {}
        try:
            df = pd.read_csv(path)
            for _, r in df.iterrows():
                coords[str(r['shelf_id'])] = {'floor': r['floor'], 'pos': (int(r['x']), int(r['y']))}
        except: pass
        return coords
    
    def _load_inventory(self):
        path = os.path.join(BASE_DIR, 'data', 'master', 'item_inventory.csv')
        inv = {}
        try:
            df = pd.read_csv(path, dtype=str)
            part_col = next((c for c in df.columns if 'PART' in c), None)
            cell_col = next((c for c in df.columns if 'CELL' in c or 'LOC' in c), None)
            if part_col and cell_col:
                for _, r in df.iterrows():
                    inv.setdefault(str(r[part_col]).strip(), []).append(str(r[cell_col]).strip()[:7])
        except: pass
        return inv

    def _load_orders(self):
        path = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        try:
            df = pd.read_csv(path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            return df.sort_values('datetime').to_dict('records')
        except: return []

    def _init_stations(self):
        # 掃描地圖找工作站
        sts = {}
        count = 0
        for r in range(self.grid_2f.shape[0]):
            for c in range(self.grid_2f.shape[1]):
                if self.grid_2f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '2F', 'pos': (r,c), 'free_time': 0}
        
        start_3f = count
        for r in range(self.grid_3f.shape[0]):
            for c in range(self.grid_3f.shape[1]):
                if self.grid_3f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '3F', 'pos': (r,c), 'free_time': 0}
        
        # Fallback
        if not sts: sts[1] = {'floor': '2F', 'pos': (0,0), 'free_time': 0}
        return sts

    def get_target(self, order):
        part = str(order.get('PARTNO', '')).strip()
        cands = self.inventory_map.get(part, [])
        for sid in cands:
            if sid in self.shelf_coords: return self.shelf_coords[sid]
        
        if self.shelf_coords:
            import random
            sid = random.choice(list(self.shelf_coords.keys()))
            return self.shelf_coords[sid]
        return None

    def run(self):
        if not self.orders: return
        
        # 將真實時間轉換為 "秒數 (Integer)" 以便於預約表計算
        base_time = self.orders[0]['datetime']
        def to_sec(dt): return int((dt - base_time).total_seconds())
        def to_dt(sec): return base_time + timedelta(seconds=sec)
        
        print(f"🎬 開始模擬... (基準時間: {base_time})")
        start_real = time.time()
        
        # 初始化演算法
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f)
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f)
        
        kpi_list = []
        count = 0
        reroute_count = 0 # 統計繞路次數
        
        for order in self.orders:
            target = self.get_target(order)
            if not target: continue
            
            floor = target['floor']
            shelf_pos = target['pos']
            order_start_sec = to_sec(order['datetime'])
            
            # 1. 分配資源
            agv_pool = self.agv_state[floor]
            best_agv = min(agv_pool, key=agv_pool.get)
            agv_ready_sec = agv_pool[best_agv]
            
            # 簡單分配同樓層工作站
            valid_st = [sid for sid, info in self.stations.items() if info['floor'] == floor]
            if not valid_st: valid_st = list(self.stations.keys())
            
            st_pool = {sid: self.stations[sid]['free_time'] for sid in valid_st}
            best_st = min(st_pool, key=st_pool.get)
            st_ready_sec = self.stations[best_st]['free_time']
            st_pos = self.stations[best_st]['pos']
            
            # 任務開始時間
            start_sec = max(order_start_sec, agv_ready_sec, st_ready_sec)
            if start_sec < 0: start_sec = 0
            
            # 2. 規劃路徑 (Station -> Shelf)
            # 這裡我們只規劃 "去程"，回程假設是對稱的 (為了效能)
            # 或者您可以再跑一次回程規劃
            astar = astar_2f if floor == '2F' else astar_3f
            res_table = self.reservations_2f if floor == '2F' else self.reservations_3f
            
            path, arrival_sec = astar.find_path(st_pos, shelf_pos, start_sec)
            
            if not path:
                # 無法到達 (可能被完全堵死) -> 延遲重試
                # 這裡簡單處理：強制延遲 60 秒再出發 (Penalty)
                total_dur = 300 
                finish_sec = start_sec + total_dur
                is_rerouted = False
                is_fail = True
            else:
                is_fail = False
                # 計算理論最短距離 (曼哈頓)
                manhattan_dist = abs(st_pos[0]-shelf_pos[0]) + abs(st_pos[1]-shelf_pos[1])
                actual_dist = len(path)
                
                # 如果 實際距離 > 理論距離 + 2，代表發生了繞路
                is_rerouted = actual_dist > (manhattan_dist + 2)
                if is_rerouted: reroute_count += 1
                
                # 3. 預約路徑 (佔用時空網格)
                # 簡單模型：去程 + 揀貨停留 + 回程 (回程沿用去程路徑但時間往後推)
                # 佔用去程
                for pos, t in path:
                    res_table.add((pos[0], pos[1], t))
                
                # 佔用回程 (假設原路返回)
                pick_end_sec = arrival_sec + self.PICK_TIME
                return_start_sec = pick_end_sec
                for i, (pos, t) in enumerate(reversed(path)):
                    # t 是去程時間，這裡我們要算出回程時間
                    # 回程時間 = 開始回程時間 + 第 i 步
                    return_t = return_start_sec + i
                    res_table.add((pos[0], pos[1], return_t))
                
                finish_sec = return_start_sec + len(path)
                total_dur = finish_sec - start_sec

            # 4. 更新狀態
            self.agv_state[floor][best_agv] = finish_sec
            self.stations[best_st]['free_time'] = finish_sec
            
            # 5. KPI
            kpi_list.append({
                'task_id': count,
                'wave_id': order.get('WAVE_ID', 'N/A'),
                'floor': floor,
                'agv': best_agv,
                'station': best_st,
                'rerouted': is_rerouted,
                'fail': is_fail,
                'start_time': to_dt(start_sec),
                'finish_time': to_dt(finish_sec),
                'duration_sec': total_dur
            })
            
            count += 1
            if count % 1000 == 0:
                print(f"\r🚀 進度: {count}/{len(self.orders)} | 繞路發生數: {reroute_count} ({(reroute_count/count)*100:.1f}%)", end='')
                
                # [記憶體管理] 定期清理太舊的預約，防止記憶體爆炸
                # 清除目前時間 3600 秒以前的預約 (假設 AGV 不會卡那麼久)
                limit_t = start_sec - 3600
                if floor == '2F':
                    self.reservations_2f = {r for r in self.reservations_2f if r[2] > limit_t}
                    astar_2f.reservations = self.reservations_2f
                else:
                    self.reservations_3f = {r for r in self.reservations_3f if r[2] > limit_t}
                    astar_3f.reservations = self.reservations_3f

        print(f"\n✅ 模擬完成！耗時 {time.time() - start_real:.2f} 秒")
        print(f"🔍 總繞路/堵塞迴避次數: {reroute_count}")
        
        pd.DataFrame(kpi_list).to_csv(os.path.join(LOG_DIR, 'simulation_kpi.csv'), index=False)
        print("💾 KPI 已存檔")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()