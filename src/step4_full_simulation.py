import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
from collections import defaultdict, deque
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class TimeAwareAStar:
    def __init__(self, grid, reservations_dict, valid_storage_spots):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations_dict
        self.valid_storage_spots = valid_storage_spots 
        self.moves = [(0, 1), (0, -1), (1, 0), (-1, 0), (0, 0)]

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    # [Fix] 統一參數名稱為 is_loaded
    def find_path(self, start, goal, start_time_sec, static_blockers=None, is_loaded=False):
        if start == goal: return [(start, start_time_sec)], start_time_sec
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None
        
        if static_blockers is None: static_blockers = set()

        open_set = []
        h_start = self.heuristic(start, goal)
        heapq.heappush(open_set, (h_start, h_start, start_time_sec, start, (0,0)))
        
        came_from = {}
        g_score = {(start, start_time_sec, (0,0)): 0}
        
        max_steps = 8000 
        steps = 0
        
        NORMAL_COST = 1      
        OBSTACLE_COST = 9999 
        TURNING_COST = 2.0   
        WAIT_COST = 1.5      
        HEURISTIC_WEIGHT = 2.0 
        HORIZON_LIMIT = 30 

        best_node = None
        min_h = float('inf')

        while open_set:
            steps += 1
            if steps > max_steps: break 
            
            f, h, current_time, current, last_move = heapq.heappop(open_set)

            if h < min_h:
                min_h = h
                best_node = (current, current_time, last_move)

            if current == goal:
                return self._reconstruct_path(came_from, (current, current_time, last_move), start, start_time_sec)

            for dr, dc in self.moves:
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + 1 
                
                if 0 <= nr < self.rows and 0 <= nc < self.cols:
                    if (nr, nc) in static_blockers and (nr, nc) != goal and (nr, nc) != start:
                        continue

                    val = self.grid[nr][nc]
                    step_cost = NORMAL_COST
                    
                    if val == -1: 
                        step_cost = OBSTACLE_COST 
                    elif val == 1:
                        if (nr, nc) in self.valid_storage_spots: 
                            if is_loaded:
                                # 載貨時：不能穿過其他料架
                                if (nr, nc) != goal and (nr, nc) != start:
                                    step_cost = OBSTACLE_COST
                            else:
                                # 空車時：可以穿過
                                step_cost = NORMAL_COST
                        else:
                            if (nr, nc) != goal and (nr, nc) != start: step_cost = OBSTACLE_COST
                    
                    if step_cost >= OBSTACLE_COST: continue

                    if (next_time - start_time_sec) < HORIZON_LIMIT:
                        if next_time in self.reservations and (nr, nc) in self.reservations[next_time]:
                            continue
                    
                    if dr == 0 and dc == 0: step_cost += WAIT_COST
                    elif (dr, dc) != last_move and last_move != (0,0): step_cost += TURNING_COST

                    new_g = g_score[(current, current_time, last_move)] + step_cost
                    new_move = (dr, dc)
                    state_key = ((nr, nc), next_time, new_move)
                    
                    if state_key not in g_score or new_g < g_score[state_key]:
                        g_score[state_key] = new_g
                        h = self.heuristic((nr, nc), goal)
                        f = new_g + (h * HEURISTIC_WEIGHT)
                        heapq.heappush(open_set, (f, h, next_time, (nr, nc), new_move))
                        came_from[state_key] = (current, current_time, last_move)
                        
        if best_node:
            return self._reconstruct_path(came_from, best_node, start, start_time_sec)
        return None, None

    def _reconstruct_path(self, came_from, current_node, start_pos, start_time):
        path = []
        curr = current_node
        while curr in came_from:
            pos, t, move = curr
            path.append((pos, t))
            curr = came_from[curr]
        path.append((start_pos, start_time))
        path.reverse()
        return path, path[-1][1]

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (V35: Fix Param Name is_loaded)...")
        
        self.PICK_TIME = 20
        self.grid_2f = self._load_map_correct('2F_map.xlsx', 32, 61)
        self.grid_3f = self._load_map_correct('3F_map.xlsx', 32, 61)
        
        self.reservations_2f = defaultdict(set)
        self.reservations_3f = defaultdict(set)
        
        self.shelf_coords = self._load_shelf_coords()
        self.shelf_occupancy = {'2F': set(), '3F': set()}
        self.valid_storage_spots = {'2F': set(), '3F': set()}
        
        for sid, info in self.shelf_coords.items():
            f = info['floor']
            p = info['pos']
            if 0 <= p[0] < 32 and 0 <= p[1] < 61:
                self.shelf_occupancy[f].add(p)
                self.valid_storage_spots[f].add(p)
            
        print(f"   -> 2F 有效儲位: {len(self.valid_storage_spots['2F'])}")
        print(f"   -> 3F 有效儲位: {len(self.valid_storage_spots['3F'])}")
            
        self.inventory_map = self._load_inventory()
        self.orders = self._load_all_tasks()
        self.stations = self._init_stations()
        
        print("   -> 初始化車隊...")
        self.used_spots_2f = set()
        self.used_spots_3f = set()
        
        self.agv_state = {
            '2F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_2f, self.used_spots_2f, '2F')} for i in range(1, 19)},
            '3F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_3f, self.used_spots_3f, '3F')} for i in range(101, 119)}
        }
        
        self.wave_totals = {}
        self.recv_totals = {}
        for o in self.orders:
            wid = str(o.get('WAVE_ID', 'UNKNOWN')) 
            d_str = o['datetime'].strftime('%Y-%m-%d')
            if 'RECEIVING' in wid: self.recv_totals[d_str] = self.recv_totals.get(d_str, 0) + 1
            else: self.wave_totals[wid] = self.wave_totals.get(wid, 0) + 1

    def _get_strict_spawn_spot(self, grid, used_spots, floor):
        rows, cols = grid.shape
        candidates = []
        for r in range(rows):
            for c in range(cols):
                if grid[r][c] == 0: candidates.append((r,c))
        if not candidates: candidates = list(self.valid_storage_spots[floor])
        random.shuffle(candidates)
        for cand in candidates:
            if cand not in used_spots:
                used_spots.add(cand)
                return cand
        return (0, 0) 

    def _find_nearest_valid_storage(self, start_pos, valid_spots, occupied_spots, shelf_occupied_spots, limit=5):
        available = []
        for spot in valid_spots:
            if spot not in shelf_occupied_spots and spot not in occupied_spots:
                dist = abs(spot[0]-start_pos[0]) + abs(spot[1]-start_pos[1])
                available.append((dist, spot))
        available.sort(key=lambda x: x[0])
        return [x[1] for x in available[:limit]]

    def _load_map_correct(self, filename, rows, cols):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        if not os.path.exists(path): path = path.replace('.xlsx', '.csv')
        try:
            if filename.endswith('.xlsx'): df = pd.read_excel(path, header=None)
            else: df = pd.read_csv(path, header=None)
        except: return np.full((rows, cols), 0)
        df_crop = df.iloc[0:rows, 0:cols]
        raw_grid = df_crop.fillna(0).values 
        final_grid = np.full((rows, cols), -1.0) 
        r_in = min(raw_grid.shape[0], rows)
        c_in = min(raw_grid.shape[1], cols)
        final_grid[0:r_in, 0:c_in] = raw_grid[0:r_in, 0:c_in]
        return final_grid

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

    def _load_all_tasks(self):
        tasks = []
        path_out = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        try:
            df_out = pd.read_csv(path_out)
            df_out['datetime'] = pd.to_datetime(df_out['datetime'])
            df_out = df_out.dropna(subset=['datetime'])
            if 'WAVE_DEADLINE' in df_out.columns:
                df_out['WAVE_DEADLINE'] = pd.to_datetime(df_out['WAVE_DEADLINE'], errors='coerce')
            tasks.extend(df_out.to_dict('records'))
        except: pass
        
        path_in = os.path.join(BASE_DIR, 'data', 'transaction', 'historical_receiving_ex.csv')
        try:
            df_in = pd.read_csv(path_in)
            cols = df_in.columns
            date_col = next((c for c in cols if 'DATE' in c), None)
            part_col = next((c for c in cols if 'ITEM' in c or 'PART' in c), None)
            if date_col and part_col:
                df_in['datetime'] = pd.to_datetime(df_in[date_col])
                df_in = df_in.dropna(subset=['datetime'])
                df_in['PARTNO'] = df_in[part_col]
                df_in['WAVE_ID'] = 'RECEIVING_' + df_in['datetime'].dt.strftime('%Y%m%d')
                df_in['WAVE_DEADLINE'] = pd.NaT 
                tasks.extend(df_in.to_dict('records'))
        except: pass
        tasks.sort(key=lambda x: x['datetime'])
        return tasks

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
                pos = self.shelf_coords[sid]['pos']
                if 0 <= pos[0] < 32 and 0 <= pos[1] < 61:
                    valid_targets.append((sid, self.shelf_coords[sid]))
        if not valid_targets and self.shelf_coords:
            all_sids = list(self.shelf_coords.keys())
            random.shuffle(all_sids)
            for sid in all_sids:
                pos = self.shelf_coords[sid]['pos']
                if 0 <= pos[0] < 32 and 0 <= pos[1] < 61:
                    valid_targets.append((sid, self.shelf_coords[sid]))
                    break
        if not valid_targets: return None, None
        best_tgt = None
        best_sid = None
        min_dist = float('inf')
        for sid, tgt in valid_targets:
            f = tgt['floor']
            st_pos = st_ref_2f if f == '2F' else st_ref_3f
            dist = abs(tgt['pos'][0] - st_pos[0]) + abs(tgt['pos'][1] - st_pos[1])
            if dist < min_dist: min_dist = dist; best_tgt = tgt; best_sid = sid
        return best_sid, best_tgt

    def write_move_events(self, writer, path, floor, agv_id, res_table):
        if not path or len(path) < 2: return
        seg_start = path[0] 
        for i in range(len(path) - 1):
            curr_pos, curr_t = path[i]
            next_pos, next_t = path[i+1]
            res_table[curr_t].add((curr_pos[0], curr_pos[1]))
            
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

    def _cleanup_reservations(self, res_table, limit_time):
        cutoff = limit_time - 60
        to_del = [t for t in res_table if t < cutoff]
        for t in to_del:
            del res_table[t]

    def _handle_blocking_shelves(self, path, floor, agv_id, start_time, w_evt, res_table, astar):
        final_time = start_time
        if not path: return start_time
        
        path_coords = [p[0] for p in path]
        target_pos = path_coords[-1]
        start_pos = path_coords[0]
        
        obstacles = []
        for pos in path_coords:
            if pos == start_pos or pos == target_pos: continue
            if pos in self.shelf_occupancy[floor]:
                obstacles.append(pos)
        
        if not obstacles:
            self.write_move_events(w_evt, path, floor, agv_id, res_table)
            return path[-1][1]

        current_t = start_time
        grid = self.grid_2f if floor == '2F' else self.grid_3f
        
        for obs_pos in obstacles:
            buffer_pos = self._find_accessible_buffer(obs_pos, grid, self.shelf_occupancy[floor], astar, start_time)
            if not buffer_pos: return start_time + 60

            # 1. AGV -> Obstacle (Empty)
            path_to_obs, _ = astar.find_path(self.agv_state[floor][int(agv_id)]['pos'], obs_pos, current_t, is_loaded=False)
            if path_to_obs:
                self.write_move_events(w_evt, path_to_obs, floor, agv_id, res_table)
                current_t = path_to_obs[-1][1]
                self.agv_state[floor][int(agv_id)]['pos'] = obs_pos
            else:
                current_t += 30 
            
            # 2. Lift
            w_evt.writerow([
                self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}",
                obs_pos[1], obs_pos[0], obs_pos[1], obs_pos[0], 'SHELF_LOAD', ''
            ])
            current_t += 5
            self.shelf_occupancy[floor].remove(obs_pos)
            
            # 3. Obstacle -> Buffer (Loaded)
            path_to_buffer, _ = astar.find_path(obs_pos, buffer_pos, current_t, is_loaded=True)
            if path_to_buffer:
                self.write_move_events(w_evt, path_to_buffer, floor, agv_id, res_table)
                current_t = path_to_buffer[-1][1]
                self.agv_state[floor][int(agv_id)]['pos'] = buffer_pos
            else:
                w_evt.writerow([
                    self.to_dt(current_t), self.to_dt(current_t+60), floor, f"AGV_{agv_id}",
                    obs_pos[1], obs_pos[0], buffer_pos[1], buffer_pos[0], 'AGV_MOVE', 'Force Move'
                ])
                current_t += 60
            
            # 4. Drop
            w_evt.writerow([
                self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}",
                buffer_pos[1], buffer_pos[0], buffer_pos[1], buffer_pos[0], 'SHELF_UNLOAD', ''
            ])
            current_t += 5
            self.shelf_occupancy[floor].add(buffer_pos)
            
            found_sid = None
            for sid, info in self.shelf_coords.items():
                if info['floor'] == floor and info['pos'] == obs_pos:
                    found_sid = sid
                    break
            if found_sid:
                self.shelf_coords[found_sid]['pos'] = buffer_pos
            
        new_path = []
        for pos, t in path:
            new_path.append((pos, t - path[0][1] + current_t))
            
        self.write_move_events(w_evt, new_path, floor, agv_id, res_table)
        return new_path[-1][1]

    def _find_accessible_buffer(self, start_pos, grid, occupied_spots, astar, start_time):
        rows, cols = grid.shape
        q = deque([(start_pos, 0)])
        visited = {start_pos}
        
        while q:
            curr, dist = q.popleft()
            if dist > 15: break
            r, c = curr
            is_valid_spot = (grid[r][c] == 0) or (grid[r][c] == 1 and curr not in occupied_spots)
            
            if is_valid_spot and curr != start_pos:
                path, _ = astar.find_path(start_pos, curr, start_time, is_loaded=True)
                if path: return curr
            
            for dr, dc in [(0,1),(0,-1),(1,0),(-1,0)]:
                nr, nc = r+dr, c+dc
                if 0<=nr<rows and 0<=nc<cols and (nr,nc) not in visited:
                    visited.add((nr,nc))
                    q.append(((nr,nc), dist+1))
        return None

    def run(self):
        if not self.orders: return
        self.base_time = self.orders[0]['datetime']
        self.to_dt = lambda sec: self.base_time + timedelta(seconds=sec)
        def to_sec(dt): return int((dt - self.base_time).total_seconds())
        
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f, self.valid_storage_spots['2F'])
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f, self.valid_storage_spots['3F'])
        
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])
        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        w_kpi = csv.writer(f_kpi)
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave', 'deadline_ts'])

        count = 0
        total_orders = len(self.orders)
        print(f"🎬 開始模擬 {total_orders} 筆訂單...")
        start_real = time.time()
        
        for floor in ['2F', '3F']:
            for agv_id, state in self.agv_state[floor].items():
                pos = state['pos']
                w_evt.writerow([
                    self.to_dt(0), self.to_dt(1), floor, f"AGV_{agv_id}",
                    pos[1], pos[0], pos[1], pos[0], 'AGV_MOVE', 'INIT'
                ])

        def find_first_st(floor):
            for sid, info in self.stations.items():
                if info['floor'] == floor: return info['pos']
            return (0,0)
        st_ref_2f = find_first_st('2F')
        st_ref_3f = find_first_st('3F')

        for order in self.orders:
            if pd.isna(order.get('datetime')): continue
            order_start_sec = to_sec(order['datetime'])
            
            target_id, target = self.get_best_target(order, st_ref_2f, st_ref_3f)
            if not target: count += 1; continue
            
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
            
            static_blockers = set()
            for agv_id, state in agv_pool.items():
                if agv_id != best_agv and state['time'] <= start_sec:
                    static_blockers.add(state['pos'])
            
            astar = astar_2f if floor == '2F' else astar_3f
            res_table = self.reservations_2f if floor == '2F' else self.reservations_3f
            
            # --- 1. 取貨 (Empty) ---
            path_to_shelf, _ = astar.find_path(agv_curr_pos, shelf_pos, start_sec, static_blockers, is_loaded=False)
            
            current_t = start_sec
            if path_to_shelf:
                self.write_move_events(w_evt, path_to_shelf, floor, best_agv, res_table)
                current_t = path_to_shelf[-1][1]
            else:
                current_t += 300
                w_evt.writerow([
                    self.to_dt(start_sec), self.to_dt(current_t), floor, f"AGV_{best_agv}",
                    agv_curr_pos[1], agv_curr_pos[0], shelf_pos[1], shelf_pos[0], 'AGV_MOVE', 'Force'
                ])
            
            self.agv_state[floor][best_agv]['pos'] = shelf_pos

            # Pick Shelf
            w_evt.writerow([
                self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}",
                shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0], 'SHELF_LOAD', ''
            ])
            current_t += 5
            
            if shelf_pos in self.shelf_occupancy[floor]:
                self.shelf_occupancy[floor].remove(shelf_pos)

            # --- 2. 搬運 (Loaded) ---
            path_to_st, _ = astar.find_path(shelf_pos, st_pos, current_t, is_loaded=True)
            
            if not path_to_st:
                # 找不到合法路徑，代表被包圍，觸發移車邏輯
                path_to_st, _ = astar.find_path(shelf_pos, st_pos, current_t, is_loaded=False) # 暫用 Empty 找路徑原型
                arrive_st_sec = self._handle_blocking_shelves(path_to_st, floor, best_agv, current_t, w_evt, res_table, astar)
            else:
                self.write_move_events(w_evt, path_to_st, floor, best_agv, res_table)
                arrive_st_sec = path_to_st[-1][1]
                
            # --- 3. 作業 ---
            leave_st_sec = arrive_st_sec + self.PICK_TIME
            for t in range(arrive_st_sec, leave_st_sec):
                res_table[t].add((st_pos[0], st_pos[1]))
            
            task_type = 'OUTBOUND'
            wave_id = str(order.get('WAVE_ID', 'UNKNOWN'))
            if 'RECEIVING' in wave_id: task_type = 'INBOUND'
            elif 'REPLENISH' in wave_id: task_type = 'REPLENISH'
            
            status_color = 'BLUE' 
            if task_type == 'INBOUND': status_color = 'GREEN'
            if task_type == 'REPLENISH': status_color = 'ORANGE'
            
            is_delayed = 'N'
            deadline = order.get('WAVE_DEADLINE')
            deadline_ts = 0
            if pd.notna(deadline) and isinstance(deadline, (pd.Timestamp, datetime)):
                 deadline_ts = int(deadline.timestamp())
            
            status_text = f"{status_color}|{wave_id}|{is_delayed}"
            w_evt.writerow([
                self.to_dt(arrive_st_sec), self.to_dt(leave_st_sec), floor, f"WS_{best_st}",
                st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'STATION_STATUS', status_text
            ])
            w_evt.writerow([
                self.to_dt(arrive_st_sec), self.to_dt(leave_st_sec), floor, f"AGV_{best_agv}",
                st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'PICKING', f"Order_{count}"
            ])

            # --- 4. 歸還 (Loaded) ---
            candidate_spots = self._find_nearest_valid_storage(
                st_pos, 
                self.valid_storage_spots[floor], 
                {s['pos'] for k, s in agv_pool.items() if k != best_agv}, 
                self.shelf_occupancy[floor], 
                limit=5
            )
            
            path_drop = None
            drop_sec = 0
            drop_pos = st_pos
            
            for drop_try in candidate_spots:
                path_drop, _ = astar.find_path(st_pos, drop_try, leave_st_sec, static_blockers, is_loaded=True)
                if path_drop:
                    self.write_move_events(w_evt, path_drop, floor, best_agv, res_table)
                    drop_sec = path_drop[-1][1]
                    drop_pos = drop_try
                    break
            
            if not path_drop:
                for drop_try in candidate_spots:
                    path_drop, _ = astar.find_path(st_pos, drop_try, leave_st_sec, static_blockers, is_loaded=False) # 找假路徑
                    if path_drop:
                        drop_sec = self._handle_blocking_shelves(path_drop, floor, best_agv, leave_st_sec, w_evt, res_table, astar)
                        drop_pos = drop_try
                        break

            finish_sec = drop_sec if drop_sec > 0 else leave_st_sec + 60
            
            w_evt.writerow([
                self.to_dt(finish_sec), self.to_dt(finish_sec+5), floor, f"AGV_{agv_id}",
                drop_pos[1], drop_pos[0], drop_pos[1], drop_pos[0], 'SHELF_UNLOAD', ''
            ])
            finish_sec += 5

            self.shelf_occupancy[floor].add(drop_pos)
            if target_id:
                self.shelf_coords[target_id]['pos'] = drop_pos

            self.agv_state[floor][best_agv]['time'] = finish_sec
            self.agv_state[floor][best_agv]['pos'] = drop_pos
            self.stations[best_st]['free_time'] = leave_st_sec
            
            if deadline_ts > 0 and self.to_dt(finish_sec) > deadline:
                is_delayed = 'Y'
            
            total_in_wave = 0
            if task_type == 'INBOUND':
                d_str = order['datetime'].strftime('%Y-%m-%d')
                total_in_wave = self.recv_totals.get(d_str, 0)
            else:
                total_in_wave = self.wave_totals.get(wave_id, 0)
            
            w_kpi.writerow([
                self.to_dt(finish_sec), 'PICKING' if task_type=='OUTBOUND' else 'RECEIVING', wave_id,
                is_delayed, self.to_dt(finish_sec).date(), f"WS_{best_st}", total_in_wave, deadline_ts
            ])
            
            count += 1
            if count % 20 == 0:
                print(f"\r🚀 進度: {count}/{total_orders} (Time: {time.time()-start_real:.1f}s)", end='')
            
            if count % 200 == 0:
                self._cleanup_reservations(self.reservations_2f, start_sec)
                self._cleanup_reservations(self.reservations_3f, start_sec)

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！總耗時 {time.time() - start_real:.2f} 秒")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()