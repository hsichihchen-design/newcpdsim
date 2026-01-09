import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
from datetime import datetime, timedelta

# 設定路徑
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class TimeAwareAStar:
    def __init__(self, grid, reservations):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations 

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def find_path(self, start, goal, start_time_sec):
        # 簡單 A* (含預約表檢查)
        open_set = []
        heapq.heappush(open_set, (0, start, start_time_sec))
        came_from = {}
        g_score = {(start, start_time_sec): 0}
        
        max_depth = 3000 # 限制深度，避免死鎖
        steps = 0
        STEP_COST = 1 

        while open_set:
            steps += 1
            if steps > max_depth: return None, None

            _, current, current_time = heapq.heappop(open_set)

            if current == goal:
                path = []
                trace = (current, current_time)
                while trace in came_from:
                    pos, t = trace
                    path.append((pos, t))
                    trace = came_from[trace]
                path.append((start, start_time_sec))
                path.reverse()
                return path, current_time

            neighbors = [(0, 1), (0, -1), (1, 0), (-1, 0)]
            for dr, dc in neighbors:
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + STEP_COST
                
                if 0 <= nr < self.rows and 0 <= nc < self.cols:
                    val = self.grid[nr][nc]
                    # 允許終點是障礙物(工作站/料架)，但路徑中間不行
                    if val in [1, 2] and (nr, nc) != goal and (nr, nc) != start:
                        continue
                        
                    if (nr, nc, next_time) in self.reservations:
                        continue
                        
                    tentative_g = g_score[(current, current_time)] + STEP_COST
                    if ((nr, nc), next_time) not in g_score or tentative_g < g_score[((nr, nc), next_time)]:
                        g_score[((nr, nc), next_time)] = tentative_g
                        f = tentative_g + self.heuristic((nr, nc), goal)
                        heapq.heappush(open_set, (f, (nr, nc), next_time))
                        came_from[((nr, nc), next_time)] = (current, current_time)
        return None, None

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (Fix: Deadline Timestamp)...")
        
        self.PICK_TIME = 20
        # 強制讀取地圖，若失敗則報錯
        self.grid_2f = self._load_map('2F_map.xlsx')
        self.grid_3f = self._load_map('3F_map.xlsx')
        print(f"   -> 2F Map: {self.grid_2f.shape}, 3F Map: {self.grid_3f.shape}")
        
        self.reservations_2f = set()
        self.reservations_3f = set()
        self.shelf_coords = self._load_shelf_coords()
        self.inventory_map = self._load_inventory()
        self.orders = self._load_orders()
        self.agv_state = {
            '2F': {i: 0 for i in range(1, 9)},
            '3F': {i: 0 for i in range(101, 109)}
        }
        self.stations = self._init_stations()

    def _load_map(self, filename):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        if not os.path.exists(path):
            print(f"❌ 嚴重錯誤: 找不到地圖檔 {path}")
            return np.zeros((10,10)) 
        try: return pd.read_excel(path, header=None).fillna(0).values
        except: return np.zeros((10,10))

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
            # [Fix] 確保 datetime 欄位正確轉型
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # [Fix] 關鍵修正：將 WAVE_DEADLINE 也強制轉型為 datetime
            if 'WAVE_DEADLINE' in df.columns:
                df['WAVE_DEADLINE'] = pd.to_datetime(df['WAVE_DEADLINE'], errors='coerce')
                
            return df.sort_values('datetime').to_dict('records')
        except Exception as e:
            print(f"❌ 訂單讀取失敗: {e}")
            return []

    def _init_stations(self):
        sts = {}
        count = 0
        # 掃描 2F
        for r in range(self.grid_2f.shape[0]):
            for c in range(self.grid_2f.shape[1]):
                if self.grid_2f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '2F', 'pos': (r,c), 'free_time': 0}
        
        # 掃描 3F (ID 接續)
        start_3f = count
        for r in range(self.grid_3f.shape[0]):
            for c in range(self.grid_3f.shape[1]):
                if self.grid_3f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '3F', 'pos': (r,c), 'free_time': 0}
        
        if not sts:
            print("⚠️ 警告: 地圖中未發現工作站 (數值=2)，使用預設座標 (5,5)")
            sts[1] = {'floor': '2F', 'pos': (5,5), 'free_time': 0}
        else:
            print(f"   -> 已識別 {len(sts)} 個工作站")
        return sts

    def get_target(self, order):
        part = str(order.get('PARTNO', '')).strip()
        cands = self.inventory_map.get(part, [])
        for sid in cands:
            if sid in self.shelf_coords: return self.shelf_coords[sid]
        # 隨機 fallback
        if self.shelf_coords:
            import random
            sid = random.choice(list(self.shelf_coords.keys()))
            return self.shelf_coords[sid]
        return None

    def run(self):
        if not self.orders: 
            print("⚠️ 無訂單資料，模擬結束")
            return
        
        base_time = self.orders[0]['datetime']
        def to_sec(dt): return int((dt - base_time).total_seconds())
        def to_dt(sec): return base_time + timedelta(seconds=sec)
        
        print(f"🎬 開始模擬... (基準時間: {base_time})")
        start_real = time.time()
        
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f)
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f)
        
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])

        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        w_kpi = csv.writer(f_kpi)
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation'])

        count = 0
        reroute_count = 0 
        
        total_orders = len(self.orders)
        
        for order in self.orders:
            target = self.get_target(order)
            if not target: continue
            
            floor = target['floor']
            shelf_pos = target['pos']
            order_start_sec = to_sec(order['datetime'])
            
            # 分配資源
            agv_pool = self.agv_state[floor]
            best_agv = min(agv_pool, key=agv_pool.get)
            agv_ready_sec = agv_pool[best_agv]
            
            valid_st = [sid for sid, info in self.stations.items() if info['floor'] == floor]
            if not valid_st: valid_st = list(self.stations.keys())
            st_pool = {sid: self.stations[sid]['free_time'] for sid in valid_st}
            best_st = min(st_pool, key=st_pool.get)
            st_ready_sec = self.stations[best_st]['free_time']
            st_pos = self.stations[best_st]['pos']
            
            start_sec = max(order_start_sec, agv_ready_sec, st_ready_sec)
            
            # 規劃去程
            astar = astar_2f if floor == '2F' else astar_3f
            res_table = self.reservations_2f if floor == '2F' else self.reservations_3f
            
            path, arrival_sec = astar.find_path(st_pos, shelf_pos, start_sec)
            
            if not path:
                total_dur = 300 
                finish_sec = start_sec + total_dur
                arrival_sec = start_sec + 150
            else:
                manhattan = abs(st_pos[0]-shelf_pos[0]) + abs(st_pos[1]-shelf_pos[1])
                if len(path) > manhattan + 2: reroute_count += 1
                
                # [Path Compression] 合併直線移動
                if len(path) > 1:
                    seg_start = path[0] 
                    for i in range(len(path) - 1):
                        curr_pos, curr_t = path[i]
                        next_pos, next_t = path[i+1]
                        
                        # 預約佔用
                        res_table.add((curr_pos[0], curr_pos[1], curr_t))
                        
                        # 轉彎檢測
                        if i < len(path) - 2:
                            nn_pos, _ = path[i+2]
                            v1 = (next_pos[0]-curr_pos[0], next_pos[1]-curr_pos[1])
                            v2 = (nn_pos[0]-next_pos[0], nn_pos[1]-next_pos[1])
                            
                            if v1 != v2:
                                # 發生轉彎，寫入一段
                                w_evt.writerow([
                                    to_dt(seg_start[1]), to_dt(next_t), floor, f"AGV_{best_agv}",
                                    seg_start[0][1], seg_start[0][0], next_pos[1], next_pos[0],
                                    'AGV_MOVE', ''
                                ])
                                seg_start = path[i+1]
                        else:
                            # 最後一段
                            w_evt.writerow([
                                to_dt(seg_start[1]), to_dt(next_t), floor, f"AGV_{best_agv}",
                                seg_start[0][1], seg_start[0][0], next_pos[1], next_pos[0],
                                'AGV_MOVE', ''
                            ])

                # 揀貨
                pick_end_sec = arrival_sec + self.PICK_TIME
                w_evt.writerow([
                    to_dt(arrival_sec), to_dt(pick_end_sec), floor, f"AGV_{best_agv}",
                    shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0],
                    'PICKING', f"Order_{count}"
                ])
                
                # 回程
                return_start_sec = pick_end_sec
                finish_sec = return_start_sec + len(path)
                
                for i in range(len(path)):
                    pos, _ = path[len(path)-1-i]
                    t = return_start_sec + i
                    res_table.add((pos[0], pos[1], t))
                
                # 回程視覺化 (一段到底)
                w_evt.writerow([
                    to_dt(return_start_sec), to_dt(finish_sec), floor, f"AGV_{best_agv}",
                    shelf_pos[1], shelf_pos[0], st_pos[1], st_pos[0],
                    'AGV_MOVE', ''
                ])

            self.agv_state[floor][best_agv] = finish_sec
            self.stations[best_st]['free_time'] = finish_sec
            
            # [Fix] 安全的比較日期
            is_delayed = 'N'
            deadline = order.get('WAVE_DEADLINE')
            # 確保 deadline 是有效時間物件，finish_sec 轉出的時間也是有效物件
            if pd.notna(deadline):
                # 如果 deadline 還是字串，這層保險會再轉一次，但理論上 _load_orders 已轉好
                if isinstance(deadline, str):
                    try: deadline = pd.to_datetime(deadline)
                    except: pass
                
                if isinstance(deadline, (pd.Timestamp, datetime)):
                    if to_dt(finish_sec) > deadline:
                        is_delayed = 'Y'
                
            w_kpi.writerow([
                to_dt(finish_sec), 'PICKING', order.get('WAVE_ID', 'N/A'),
                is_delayed, to_dt(finish_sec).date(), f"WS_{best_st}"
            ])
            
            count += 1
            if count % 2000 == 0:
                print(f"\r🚀 進度: {count}/{total_orders} | 繞路: {reroute_count}", end='')
                
                limit_t = start_sec - 3600
                if floor == '2F':
                    self.reservations_2f = {r for r in self.reservations_2f if r[2] > limit_t}
                    astar_2f.reservations = self.reservations_2f
                else:
                    self.reservations_3f = {r for r in self.reservations_3f if r[2] > limit_t}
                    astar_3f.reservations = self.reservations_3f

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！耗時 {time.time() - start_real:.2f} 秒")
        print("💾 資料已輸出 (已壓縮): simulation_kpi.csv, simulation_events.csv")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()