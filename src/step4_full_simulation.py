import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class TimeAwareAStar:
    def __init__(self, grid, reservations):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations 
        self.moves = [(0, 1), (0, -1), (1, 0), (-1, 0)]

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def find_path(self, start, goal, start_time_sec):
        if start == goal: return [(start, start_time_sec)], start_time_sec
        # 邊界檢查
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None
        if not (0 <= goal[0] < self.rows and 0 <= goal[1] < self.cols): return None, None

        open_set = []
        heapq.heappush(open_set, (0, 0, start, start_time_sec))
        came_from = {}
        g_score = {(start, start_time_sec): 0}
        
        max_steps = 3000 
        steps = 0
        NORMAL_COST = 1      
        RELOCATION_COST = 45 
        HEURISTIC_WEIGHT = 1.5 

        while open_set:
            steps += 1
            if steps > max_steps: return None, None
            _, _, current, current_time = heapq.heappop(open_set)

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

            for dr, dc in self.moves:
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + NORMAL_COST 
                
                if 0 <= nr < self.rows and 0 <= nc < self.cols:
                    val = self.grid[nr][nc]
                    step_cost = NORMAL_COST
                    if (val == 1 or val == 2):
                        if (nr, nc) != goal and (nr, nc) != start: step_cost = RELOCATION_COST

                    if (nr, nc, next_time) in self.reservations: continue
                    new_g = g_score[(current, current_time)] + step_cost
                    next_node_key = ((nr, nc), next_time)
                    if next_node_key not in g_score or new_g < g_score[next_node_key]:
                        g_score[next_node_key] = new_g
                        h = self.heuristic((nr, nc), goal)
                        f = new_g + (h * HEURISTIC_WEIGHT)
                        heapq.heappush(open_set, (f, h, (nr, nc), next_time))
                        came_from[next_node_key] = (current, current_time)
        return None, None

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (True Random Spread)...")
        
        self.PICK_TIME = 20
        self.grid_2f = self._load_map('2F_map.xlsx')
        self.grid_3f = self._load_map('3F_map.xlsx')
        
        self.reservations_2f = set()
        self.reservations_3f = set()
        self.shelf_coords = self._load_shelf_coords()
        self.inventory_map = self._load_inventory()
        self.orders = self._load_orders()
        
        # AGV 初始化: 使用全圖隨機撒點，解決集中左上角問題
        print("   -> 初始化車隊: 2F(18台), 3F(18台)")
        self.agv_state = {
            '2F': {i: {'time': 0, 'pos': self._get_truly_random_spot(self.grid_2f)} for i in range(1, 19)},
            '3F': {i: {'time': 0, 'pos': self._get_truly_random_spot(self.grid_3f)} for i in range(101, 119)}
        }
        self.stations = self._init_stations()
        
        self.wave_totals = {}
        for o in self.orders:
            wid = o.get('WAVE_ID', 'UNKNOWN')
            self.wave_totals[wid] = self.wave_totals.get(wid, 0) + 1

    def _get_truly_random_spot(self, grid):
        """找出所有空地 (0)，然後隨機選一個，確保分散"""
        rows, cols = grid.shape
        candidates = []
        # 掃描全圖 (這只在初始化跑一次，不影響效能)
        for r in range(rows):
            for c in range(cols):
                if grid[r][c] == 0:
                    candidates.append((r, c))
        
        if candidates:
            return random.choice(candidates)
        return (0, 0) # Fallback

    def _find_nearest_empty_spot(self, grid, start_pos):
        rows, cols = grid.shape
        queue = [start_pos]
        visited = set([start_pos])
        while queue:
            curr = queue.pop(0)
            if grid[curr[0]][curr[1]] == 0: return curr
            for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
                nr, nc = curr[0] + dr, curr[1] + dc
                if 0 <= nr < rows and 0 <= nc < cols and (nr, nc) not in visited:
                    if grid[nr][nc] != 2:
                        visited.add((nr, nc))
                        queue.append((nr, nc))
        return start_pos

    def _load_map(self, filename):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        df = None
        if os.path.exists(path):
            try: df = pd.read_excel(path, header=None)
            except: pass
        if df is None:
            csv_path = path.replace('.xlsx', '.csv')
            if os.path.exists(csv_path):
                try: df = pd.read_csv(csv_path, header=None)
                except: pass
        if df is not None:
            grid = df.fillna(0).values
            grid[grid == -1] = 1 # 牆壁標準化
            return grid
        return np.zeros((10,10))

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
        for r in range(self.grid_2f.shape[0]):
            for c in range(self.grid_2f.shape[1]):
                if self.grid_2f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '2F', 'pos': (r,c), 'free_time': 0}
        start_3f = count
        for r in range(self.grid_3f.shape[0]):
            for c in range(self.grid_3f.shape[1]):
                if self.grid_3f[r][c] == 2:
                    count += 1; sts[count] = {'floor': '3F', 'pos': (r,c), 'free_time': 0}
        if not sts: sts[1] = {'floor': '2F', 'pos': (5,5), 'free_time': 0}
        return sts

    def get_best_target(self, order, st_ref_2f, st_ref_3f):
        part = str(order.get('PARTNO', '')).strip()
        candidates = self.inventory_map.get(part, [])
        valid_targets = []
        for sid in candidates:
            if sid in self.shelf_coords:
                valid_targets.append(self.shelf_coords[sid])

        if not valid_targets and self.shelf_coords:
            sid = random.choice(list(self.shelf_coords.keys()))
            valid_targets.append(self.shelf_coords[sid])

        if not valid_targets: return None

        best_tgt = None
        min_dist = float('inf')
        for tgt in valid_targets:
            f = tgt['floor']
            st_pos = st_ref_2f if f == '2F' else st_ref_3f
            dist = abs(tgt['pos'][0] - st_pos[0]) + abs(tgt['pos'][1] - st_pos[1])
            if dist < min_dist:
                min_dist = dist
                best_tgt = tgt
        return best_tgt

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
                writer.writerow([
                    self.to_dt(seg_start[1]), self.to_dt(next_t), floor, f"AGV_{agv_id}",
                    seg_start[0][1], seg_start[0][0], next_pos[1], next_pos[0], 'AGV_MOVE', ''
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
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave'])

        count = 0
        total_orders = len(self.orders)
        print(f"🎬 開始模擬 {total_orders} 筆訂單...")
        start_real = time.time()
        
        def find_first_st(floor):
            for sid, info in self.stations.items():
                if info['floor'] == floor: return info['pos']
            return (0,0)
            
        st_ref_2f = find_first_st('2F')
        st_ref_3f = find_first_st('3F')

        for order in self.orders:
            order_start_sec = to_sec(order['datetime'])
            target = self.get_best_target(order, st_ref_2f, st_ref_3f)
            
            if not target:
                count += 1
                continue
            
            floor = target['floor']
            shelf_pos = target['pos']
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
            
            path_to_station, arrive_st_sec = astar.find_path(agv_curr_pos, st_pos, start_sec)
            if not path_to_station:
                arrive_st_sec = start_sec + 60
                path_to_station = [(agv_curr_pos, start_sec), (st_pos, arrive_st_sec)]
            self.write_move_events(w_evt, path_to_station, floor, best_agv, res_table)

            path_to_shelf, arrive_shelf_sec = astar.find_path(st_pos, shelf_pos, arrive_st_sec)
            if not path_to_shelf:
                total_dur = 300 
                finish_sec = arrive_st_sec + total_dur
                pick_end_sec = finish_sec - 100
                drop_pos = shelf_pos
            else:
                self.write_move_events(w_evt, path_to_shelf, floor, best_agv, res_table)
                pick_end_sec = arrive_shelf_sec + self.PICK_TIME
                w_evt.writerow([
                    self.to_dt(arrive_shelf_sec), self.to_dt(pick_end_sec), floor, f"AGV_{best_agv}",
                    shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0], 'PICKING', f"Order_{count}"
                ])
                path_return, finish_sec = astar.find_path(shelf_pos, st_pos, pick_end_sec)
                if path_return:
                    self.write_move_events(w_evt, path_return, floor, best_agv, res_table)
                else:
                    finish_sec = pick_end_sec + 60
                    
                grid_obj = self.grid_2f if floor == '2F' else self.grid_3f
                drop_pos = self._find_nearest_empty_spot(grid_obj, st_pos)
                path_drop, drop_sec = astar.find_path(st_pos, drop_pos, finish_sec)
                if path_drop:
                     self.write_move_events(w_evt, path_drop, floor, best_agv, res_table)
                     finish_sec = drop_sec

            task_type = 'OUTBOUND'
            wave_id = str(order.get('WAVE_ID', 'UNKNOWN'))
            if 'RECEIVING' in wave_id: task_type = 'INBOUND'
            elif 'REPLENISH' in wave_id: task_type = 'REPLENISH'
            
            status_color = 'BLUE' 
            if task_type == 'INBOUND': status_color = 'GREEN'
            if task_type == 'REPLENISH': status_color = 'ORANGE'
            
            is_delayed = 'N'
            deadline = order.get('WAVE_DEADLINE')
            if pd.notna(deadline) and isinstance(deadline, (pd.Timestamp, datetime)):
                 if self.to_dt(finish_sec) > deadline: is_delayed = 'Y'
            
            status_text = f"{status_color}|{wave_id}|{is_delayed}"
            w_evt.writerow([
                self.to_dt(arrive_st_sec), self.to_dt(finish_sec), floor, f"WS_{best_st}",
                st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'STATION_STATUS', status_text
            ])

            self.agv_state[floor][best_agv]['time'] = finish_sec
            self.agv_state[floor][best_agv]['pos'] = drop_pos
            self.stations[best_st]['free_time'] = finish_sec
            
            total_in_wave = self.wave_totals.get(wave_id, 0)
            w_kpi.writerow([
                self.to_dt(finish_sec), 'PICKING', wave_id,
                is_delayed, self.to_dt(finish_sec).date(), f"WS_{best_st}", total_in_wave
            ])
            
            count += 1
            if count % 500 == 0:
                print(f"\r🚀 進度: {count}/{total_orders} (Time: {time.time()-start_real:.1f}s)", end='')
                limit_t = start_sec - 1800 
                if floor == '2F':
                    self.reservations_2f = {r for r in self.reservations_2f if r[2] > limit_t}
                    astar_2f.reservations = self.reservations_2f
                else:
                    self.reservations_3f = {r for r in self.reservations_3f if r[2] > limit_t}
                    astar_3f.reservations = self.reservations_3f

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！總耗時 {time.time() - start_real:.2f} 秒")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()