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
        open_set = []
        heapq.heappush(open_set, (0, start, start_time_sec))
        came_from = {}
        g_score = {(start, start_time_sec): 0}
        
        max_depth = 5000 # 增加搜尋深度以應對繞路
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
        print(f"🚀 [Step 4] 啟動進階模擬 (Visualization Ready)...")
        
        self.PICK_TIME = 20
        self.grid_2f = self._load_map('2F_map.xlsx')
        self.grid_3f = self._load_map('3F_map.xlsx')
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
        sts = {}
        count = 0
        for r in range(self.grid_2f.shape[0]):
            for c in range(self.grid_2f.shape[1]):
                if self.grid_2f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '2F', 'pos': (r,c), 'free_time': 0}
        for r in range(self.grid_3f.shape[0]):
            for c in range(self.grid_3f.shape[1]):
                if self.grid_3f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '3F', 'pos': (r,c), 'free_time': 0}
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
        
        base_time = self.orders[0]['datetime']
        def to_sec(dt): return int((dt - base_time).total_seconds())
        def to_dt(sec): return base_time + timedelta(seconds=sec)
        
        print(f"🎬 開始模擬... (基準時間: {base_time})")
        start_real = time.time()
        
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f)
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f)
        
        # [NEW] 開啟 Event Log
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])

        kpi_list = []
        count = 0
        reroute_count = 0 
        
        for order in self.orders:
            target = self.get_target(order)
            if not target: continue
            
            floor = target['floor']
            shelf_pos = target['pos']
            order_start_sec = to_sec(order['datetime'])
            
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
            if start_sec < 0: start_sec = 0
            
            astar = astar_2f if floor == '2F' else astar_3f
            res_table = self.reservations_2f if floor == '2F' else self.reservations_3f
            
            path, arrival_sec = astar.find_path(st_pos, shelf_pos, start_sec)
            
            if not path:
                total_dur = 300 
                finish_sec = start_sec + total_dur
                is_rerouted = False
                is_fail = True
            else:
                is_fail = False
                manhattan_dist = abs(st_pos[0]-shelf_pos[0]) + abs(st_pos[1]-shelf_pos[1])
                actual_dist = len(path)
                is_rerouted = actual_dist > (manhattan_dist + 2)
                if is_rerouted: reroute_count += 1
                
                # [VISUALIZATION] 寫入去程路徑
                for i in range(len(path) - 1):
                    curr_pos, curr_t = path[i]
                    next_pos, next_t = path[i+1]
                    res_table.add((curr_pos[0], curr_pos[1], curr_t)) # 預約
                    
                    w_evt.writerow([
                        to_dt(curr_t), to_dt(next_t), floor, f"AGV_{best_agv}",
                        curr_pos[1], curr_pos[0], next_pos[1], next_pos[0], # 注意 X,Y 交換
                        'AGV_MOVE', ''
                    ])

                # 寫入回程 (簡化: 原路返回，時間往後推)
                # 到達料架時間
                pick_end_sec = arrival_sec + self.PICK_TIME
                
                # 寫入揀貨事件
                w_evt.writerow([
                    to_dt(arrival_sec), to_dt(pick_end_sec), floor, f"AGV_{best_agv}",
                    shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0],
                    'PICKING', f"Order_{count}"
                ])

                return_start_sec = pick_end_sec
                # 回程路徑 (path 是 st -> shelf，所以回程是 reversed path)
                rev_path = path[::-1] # Shelf -> Station
                
                for i in range(len(rev_path) - 1):
                    # 原始座標
                    p1_pos, _ = rev_path[i]
                    p2_pos, _ = rev_path[i+1]
                    
                    # 計算回程時間
                    t1 = return_start_sec + i
                    t2 = return_start_sec + i + 1
                    
                    res_table.add((p1_pos[0], p1_pos[1], t1)) # 預約
                    
                    w_evt.writerow([
                        to_dt(t1), to_dt(t2), floor, f"AGV_{best_agv}",
                        p1_pos[1], p1_pos[0], p2_pos[1], p2_pos[0],
                        'AGV_MOVE', ''
                    ])

                finish_sec = return_start_sec + len(path)
                total_dur = finish_sec - start_sec

            self.agv_state[floor][best_agv] = finish_sec
            self.stations[best_st]['free_time'] = finish_sec
            
            kpi_list.append({
                'task_id': count,
                'wave_id': order.get('WAVE_ID', 'N/A'),
                'floor': floor,
                'agv': best_agv,
                'station': best_st,
                'rerouted': is_rerouted,
                'start_time': to_dt(start_sec),
                'finish_time': to_dt(finish_sec),
                'duration_sec': total_dur
            })
            
            count += 1
            if count % 1000 == 0:
                print(f"\r🚀 進度: {count}/{len(self.orders)} | 繞路: {reroute_count}", end='')
                # 記憶體清理
                limit_t = start_sec - 3600
                if floor == '2F':
                    self.reservations_2f = {r for r in self.reservations_2f if r[2] > limit_t}
                    astar_2f.reservations = self.reservations_2f
                else:
                    self.reservations_3f = {r for r in self.reservations_3f if r[2] > limit_t}
                    astar_3f.reservations = self.reservations_3f

        f_evt.close()
        print(f"\n✅ 模擬完成！耗時 {time.time() - start_real:.2f} 秒")
        pd.DataFrame(kpi_list).to_csv(os.path.join(LOG_DIR, 'simulation_kpi.csv'), index=False)
        print("💾 資料已輸出: simulation_kpi.csv, simulation_events.csv")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()