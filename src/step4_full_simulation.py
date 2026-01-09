import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
from datetime import datetime, timedelta

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
        if start == goal:
            return [(start, start_time_sec)], start_time_sec

        # 邊界檢查
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None
        if not (0 <= goal[0] < self.rows and 0 <= goal[1] < self.cols): return None, None

        open_set = []
        heapq.heappush(open_set, (0, start, start_time_sec))
        came_from = {}
        g_score = {(start, start_time_sec): 0}
        
        max_depth = 5000 
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
                    # 障礙物邏輯：1=牆/料架, 2=工作站
                    # 允許進出起點與終點，但不允許穿過牆壁(1)
                    if val == 1 and (nr, nc) != goal and (nr, nc) != start:
                        continue
                    # 工作站(2)通常也是障礙，除非它是目標
                    if val == 2 and (nr, nc) != goal and (nr, nc) != start:
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
        print(f"🚀 [Step 4] 啟動進階模擬 (Fix: Map Values -1 to 1)...")
        
        self.PICK_TIME = 20
        self.grid_2f = self._load_map('2F_map.xlsx')
        self.grid_3f = self._load_map('3F_map.xlsx')
        print(f"   -> 2F Map: {self.grid_2f.shape}, 3F Map: {self.grid_3f.shape}")
        
        self.reservations_2f = set()
        self.reservations_3f = set()
        self.shelf_coords = self._load_shelf_coords()
        self.inventory_map = self._load_inventory()
        self.orders = self._load_orders()
        
        # AGV 初始化: 隨機分佈在空地 (Value=0)
        self.agv_state = {
            '2F': {i: {'time': 0, 'pos': self._find_random_empty_spot(self.grid_2f)} for i in range(1, 9)},
            '3F': {i: {'time': 0, 'pos': self._find_random_empty_spot(self.grid_3f)} for i in range(101, 109)}
        }
        self.stations = self._init_stations()

    def _find_random_empty_spot(self, grid):
        rows, cols = grid.shape
        # 嘗試 100 次找空位
        for _ in range(100):
            r, c = np.random.randint(0, rows), np.random.randint(0, cols)
            if grid[r][c] == 0: return (r, c)
        # 找不到就回傳 (0,0) 但可能會卡住
        return (0, 0)

    def _load_map(self, filename):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        if not os.path.exists(path):
            path_csv = path.replace('.xlsx', '.csv')
            if os.path.exists(path_csv):
                 df = pd.read_csv(path_csv, header=None)
            else:
                print(f"❌ 找不到地圖檔: {filename}")
                return np.zeros((10,10))
        else:
            try: df = pd.read_excel(path, header=None)
            except: return np.zeros((10,10))
            
        grid = df.fillna(0).values
        
        # [核心修復] 標準化地圖數值
        # 將 -1 (邊界) 轉換為 1 (牆壁)
        # 將其他非 0, 2, 3 的數值也視為牆壁
        grid[grid == -1] = 1
        
        return grid

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
            cols = df.columns
            part_col = next((c for c in cols if 'PART' in c), None)
            cell_col = next((c for c in cols if 'CELL' in c or 'LOC' in c), None)
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
            if 'WAVE_DEADLINE' in df.columns:
                df['WAVE_DEADLINE'] = pd.to_datetime(df['WAVE_DEADLINE'], errors='coerce')
            return df.sort_values('datetime').to_dict('records')
        except: return []

    def _init_stations(self):
        sts = {}
        count = 0
        # 2F
        for r in range(self.grid_2f.shape[0]):
            for c in range(self.grid_2f.shape[1]):
                if self.grid_2f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '2F', 'pos': (r,c), 'free_time': 0}
        # 3F
        start_3f = count
        for r in range(self.grid_3f.shape[0]):
            for c in range(self.grid_3f.shape[1]):
                if self.grid_3f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '3F', 'pos': (r,c), 'free_time': 0}
        
        if not sts: sts[1] = {'floor': '2F', 'pos': (5,5), 'free_time': 0}
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

    def write_move_events(self, writer, path, floor, agv_id, res_table):
        if not path or len(path) < 2: return
        
        seg_start = path[0] 
        for i in range(len(path) - 1):
            curr_pos, curr_t = path[i]
            next_pos, next_t = path[i+1]
            
            res_table.add((curr_pos[0], curr_pos[1], curr_t))
            
            is_turn = False
            if i < len(path) - 2:
                nn_pos, _ = path[i+2]
                v1 = (next_pos[0]-curr_pos[0], next_pos[1]-curr_pos[1])
                v2 = (nn_pos[0]-next_pos[0], nn_pos[1]-next_pos[1])
                if v1 != v2: is_turn = True
            else:
                is_turn = True 
            
            if is_turn:
                # 寫入 Event (Visualizer: sx=Col, sy=Row)
                writer.writerow([
                    self.to_dt(seg_start[1]), self.to_dt(next_t), floor, f"AGV_{agv_id}",
                    seg_start[0][1], seg_start[0][0], next_pos[1], next_pos[0],
                    'AGV_MOVE', ''
                ])
                seg_start = path[i+1]

    def run(self):
        if not self.orders: return
        
        self.base_time = self.orders[0]['datetime']
        self.to_dt = lambda sec: self.base_time + timedelta(seconds=sec)
        def to_sec(dt): return int((dt - self.base_time).total_seconds())
        
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f)
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f)
        
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])

        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        w_kpi = csv.writer(f_kpi)
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation'])

        count = 0
        
        for order in self.orders:
            target = self.get_target(order)
            if not target: continue
            
            floor = target['floor']
            shelf_pos = target['pos']
            order_start_sec = to_sec(order['datetime'])
            
            # 資源分配
            agv_pool = self.agv_state[floor]
            best_agv = min(agv_pool, key=lambda k: agv_pool[k]['time'])
            agv_ready_sec = agv_pool[best_agv]['time']
            agv_curr_pos = agv_pool[best_agv]['pos']
            
            valid_st = [sid for sid, info in self.stations.items() if info['floor'] == floor]
            if not valid_st: valid_st = list(self.stations.keys())
            st_pool = {sid: self.stations[sid]['free_time'] for sid in valid_st}
            best_st = min(st_pool, key=st_pool.get)
            st_ready_sec = self.stations[best_st]['free_time']
            st_pos = self.stations[best_st]['pos']
            
            start_sec = max(order_start_sec, agv_ready_sec, st_ready_sec)
            
            astar = astar_2f if floor == '2F' else astar_3f
            res_table = self.reservations_2f if floor == '2F' else self.reservations_3f
            
            # 1. 移車
            path_to_station, arrive_st_sec = astar.find_path(agv_curr_pos, st_pos, start_sec)
            if not path_to_station:
                arrive_st_sec = start_sec + 60
                path_to_station = [(agv_curr_pos, start_sec), (st_pos, arrive_st_sec)] # 強制移動
            
            self.write_move_events(w_evt, path_to_station, floor, best_agv, res_table)

            # 2. 去程
            path_to_shelf, arrive_shelf_sec = astar.find_path(st_pos, shelf_pos, arrive_st_sec)
            
            if not path_to_shelf:
                total_dur = 300 
                finish_sec = arrive_st_sec + total_dur
                pick_end_sec = finish_sec - 100
            else:
                self.write_move_events(w_evt, path_to_shelf, floor, best_agv, res_table)
                
                pick_end_sec = arrive_shelf_sec + self.PICK_TIME
                w_evt.writerow([
                    self.to_dt(arrive_shelf_sec), self.to_dt(pick_end_sec), floor, f"AGV_{best_agv}",
                    shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0],
                    'PICKING', f"Order_{count}"
                ])
                
                # 3. 回程
                path_return, finish_sec = astar.find_path(shelf_pos, st_pos, pick_end_sec)
                if path_return:
                    self.write_move_events(w_evt, path_return, floor, best_agv, res_table)
                else:
                    finish_sec = pick_end_sec + 60

            # 4. 工作站狀態
            task_type = 'OUTBOUND'
            wave_id = str(order.get('WAVE_ID', ''))
            if 'RECEIVING' in wave_id or 'REC' in wave_id: task_type = 'INBOUND'
            elif 'REPLENISH' in wave_id: task_type = 'REPLENISH'
            
            status_color = 'BLUE' 
            if task_type == 'INBOUND': status_color = 'GREEN'
            if task_type == 'REPLENISH': status_color = 'ORANGE'
            
            w_evt.writerow([
                self.to_dt(arrive_st_sec), self.to_dt(finish_sec), floor, f"WS_{best_st}",
                st_pos[1], st_pos[0], st_pos[1], st_pos[0],
                'STATION_STATUS', status_color
            ])

            self.agv_state[floor][best_agv]['time'] = finish_sec
            self.agv_state[floor][best_agv]['pos'] = st_pos
            self.stations[best_st]['free_time'] = finish_sec
            
            is_delayed = 'N'
            deadline = order.get('WAVE_DEADLINE')
            if pd.notna(deadline) and isinstance(deadline, (pd.Timestamp, datetime)):
                 if self.to_dt(finish_sec) > deadline: is_delayed = 'Y'

            w_kpi.writerow([
                self.to_dt(finish_sec), 'PICKING', wave_id,
                is_delayed, self.to_dt(finish_sec).date(), f"WS_{best_st}"
            ])
            
            count += 1
            if count % 1000 == 0:
                print(f"\r🚀 進度: {count}/{len(self.orders)}", end='')
                limit_t = start_sec - 3600
                if floor == '2F':
                    self.reservations_2f = {r for r in self.reservations_2f if r[2] > limit_t}
                    astar_2f.reservations = self.reservations_2f
                else:
                    self.reservations_3f = {r for r in self.reservations_3f if r[2] > limit_t}
                    astar_3f.reservations = self.reservations_3f

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！耗時 {time.time() - self.base_time.timestamp() if hasattr(self, 'base_time') else 0:.2f} 秒")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()