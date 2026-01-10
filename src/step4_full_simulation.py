import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
import math
from collections import defaultdict, deque, Counter
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class TimeAwareAStar:
    def __init__(self, grid, reservations_dict, shelf_occupancy_set):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations_dict
        self.shelf_occupancy = shelf_occupancy_set 
        self.moves = [(0, 1), (0, -1), (1, 0), (-1, 0), (0, 0)]
        self.max_steps = 2500 

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def find_path(self, start, goal, start_time_sec, static_blockers=None, is_loaded=False, ignore_dynamic=False, allow_tunneling=False):
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None
        if not (0 <= goal[0] < self.rows and 0 <= goal[1] < self.cols): return None, None
        if self.grid[start[0]][start[1]] == -1: return None, None
        if self.grid[goal[0]][goal[1]] == -1: return None, None

        if start == goal: return [(start, start_time_sec)], start_time_sec
        
        dist = self.heuristic(start, goal)
        dynamic_max_steps = max(2000, dist * 20) 

        open_set = []
        h_start = self.heuristic(start, goal)
        heapq.heappush(open_set, (h_start, h_start, start_time_sec, start, (0,0)))
        
        came_from = {}
        g_score = {(start, start_time_sec, (0,0)): 0}
        
        steps = 0
        NORMAL_COST = 1      
        TURNING_COST = 1.5   
        WAIT_COST = 1.0       
        HEURISTIC_WEIGHT = 1.2 
        TUNNEL_COST = 20.0 

        while open_set:
            steps += 1
            if steps > dynamic_max_steps: break 
            
            f, h, current_time, current, last_move = heapq.heappop(open_set)

            if current == goal:
                return self._reconstruct_path(came_from, (current, current_time, last_move), start, start_time_sec)

            for dr, dc in self.moves:
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + 1 
                
                if 0 <= nr < self.rows and 0 <= nc < self.cols:
                    val = self.grid[nr][nc]
                    if val == -1: continue 
                    
                    if not ignore_dynamic:
                        if (next_time - start_time_sec) < 45: 
                            if next_time in self.reservations and (nr, nc) in self.reservations[next_time]:
                                continue
                    
                    is_spot_occupied = ((nr, nc) in self.shelf_occupancy)
                    step_cost = NORMAL_COST
                    
                    if is_loaded and is_spot_occupied:
                        if (nr, nc) != goal and (nr, nc) != start:
                            if allow_tunneling:
                                step_cost = TUNNEL_COST
                            else:
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

class TrafficController:
    def __init__(self, grid, agv_state_pool, reservations):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.agv_pool = agv_state_pool 
        self.reservations = reservations

    def clear_path_obstacles(self, start_pos, goal_pos, current_time, w_evt, floor, my_agv_name):
        blocker_id = None
        blocker_pos = None
        
        curr = list(start_pos)
        target = list(goal_pos)
        steps_checked = 0
        while curr != target and steps_checked < 5:
            if curr[0] < target[0]: curr[0] += 1
            elif curr[0] > target[0]: curr[0] -= 1
            elif curr[1] < target[1]: curr[1] += 1
            elif curr[1] > target[1]: curr[1] -= 1
            check_pos = tuple(curr)
            for agv_id, state in self.agv_pool.items():
                if f"AGV_{agv_id}" == my_agv_name: continue
                if state['pos'] == check_pos:
                    blocker_id = agv_id
                    blocker_pos = check_pos
                    break
            if blocker_id: break
            steps_checked += 1
            
        if not blocker_id: return False, 0

        sanctuary = self._find_sanctuary(blocker_pos, current_time)
        if sanctuary:
            self.agv_pool[blocker_id]['pos'] = sanctuary
            dist = abs(blocker_pos[0]-sanctuary[0]) + abs(blocker_pos[1]-sanctuary[1])
            cost = dist * 2.0 
            w_evt.writerow([
                datetime.fromtimestamp(current_time), datetime.fromtimestamp(current_time+int(cost)), 
                floor, f"AGV_{blocker_id}", blocker_pos[1], blocker_pos[0], sanctuary[1], sanctuary[0], 'YIELD', f'Evicted by {my_agv_name}'
            ])
            for t in range(int(cost) + 5):
                self.reservations[current_time + t].add(sanctuary)
            return True, cost
        return False, 0

    def _find_sanctuary(self, start_pos, current_time):
        q = deque([start_pos])
        visited = {start_pos}
        max_search = 500 
        count = 0
        while q and count < max_search:
            curr = q.popleft()
            count += 1
            if curr != start_pos and self.grid[curr[0]][curr[1]] != -1:
                is_reserved = False
                for t in range(5):
                    if curr in self.reservations[current_time + t]:
                        is_reserved = True; break
                if not is_reserved:
                    is_occupied = False
                    for state in self.agv_pool.values():
                        if state['pos'] == curr:
                            is_occupied = True; break
                    if not is_occupied:
                        return curr 
            for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
                nr, nc = curr[0]+dr, curr[1]+dc
                if 0<=nr<self.rows and 0<=nc<self.cols:
                    if (nr, nc) not in visited:
                        visited.add((nr, nc))
                        q.append((nr, nc))
        return None

class ShuffleManager:
    def __init__(self, grid, shelf_occupancy, pos_to_shelf_id_map, shelf_coords):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.occupancy = shelf_occupancy
        self.pos_to_id = pos_to_shelf_id_map
        self.shelf_coords = shelf_coords

    def try_make_space(self, target_pos, w_evt, current_time, floor, agv_name):
        blockers = []
        for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
            nr, nc = target_pos[0]+dr, target_pos[1]+dc
            if (nr, nc) in self.occupancy:
                blockers.append((nr, nc))
        if not blockers: return False, 0

        total_penalty = 0
        moved_count = 0
        for blk_pos in blockers:
            buffer_pos = self._find_nearest_empty(blk_pos)
            if buffer_pos:
                self._execute_logical_move(blk_pos, buffer_pos, w_evt, current_time, floor)
                dist_shuffle = abs(blk_pos[0]-buffer_pos[0]) + abs(blk_pos[1]-buffer_pos[1])
                cost = 10.0 + (dist_shuffle * 2.0) + 15.0 
                total_penalty += cost
                moved_count += 1
                if moved_count >= 1: break 
        if moved_count > 0: return True, total_penalty
        return False, 0

    def _find_nearest_empty(self, start_pos):
        q = deque([start_pos])
        visited = {start_pos}
        max_dist = 10 
        while q:
            curr = q.popleft()
            if self.grid[curr[0]][curr[1]] != -1 and curr not in self.occupancy:
                return curr
            if abs(curr[0]-start_pos[0]) + abs(curr[1]-start_pos[1]) > max_dist: continue
            for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
                nr, nc = curr[0]+dr, curr[1]+dc
                if 0<=nr<self.rows and 0<=nc<self.cols:
                    if (nr, nc) not in visited:
                        visited.add((nr, nc))
                        q.append((nr, nc))
        return None

    def _execute_logical_move(self, start, end, w_evt, t, floor):
        sid = self.pos_to_id.get(start)
        if not sid: return
        self.occupancy.remove(start)
        self.occupancy.add(end)
        self.shelf_coords[sid]['pos'] = end
        del self.pos_to_id[start]
        self.pos_to_id[end] = sid
        w_evt.writerow([
            datetime.fromtimestamp(t), datetime.fromtimestamp(t+1), 
            floor, f"Shelf_{sid}", start[1], start[0], end[1], end[0], 'SHUFFLE', 'Moved aside'
        ])

class ParkingManager:
    def __init__(self, grid, shelf_occupancy):
        self.grid = grid
        self.shelf_occupancy = shelf_occupancy
    
    def find_parking_spot(self, current_pos, active_shelves_pos):
        q = deque([current_pos])
        visited = {current_pos}
        while q:
            curr = q.popleft()
            if curr in self.shelf_occupancy and curr not in active_shelves_pos:
                return curr
            if abs(curr[0]-current_pos[0]) + abs(curr[1]-current_pos[1]) > 50:
                continue
            for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
                nr, nc = curr[0]+dr, curr[1]+dc
                if 0<=nr<self.grid.shape[0] and 0<=nc<self.grid.shape[1]:
                    if (nr, nc) not in visited:
                        visited.add((nr, nc))
                        q.append((nr, nc))
        return None

class OrderProcessor:
    def __init__(self, stations_2f, stations_3f):
        self.stations = {'2F': list(stations_2f.keys()), '3F': list(stations_3f.keys())}
        self.cust_station_map = {} 
    def process_wave(self, wave_orders, floor):
        wave_custs = wave_orders['PARTCUSTID'].unique()
        available_stations = self.stations.get(floor, [])
        if not available_stations: return []
        for i, cust_id in enumerate(wave_custs):
            if cust_id not in self.cust_station_map:
                st_idx = i % len(available_stations)
                self.cust_station_map[cust_id] = available_stations[st_idx]
        shelf_tasks = defaultdict(list)
        for _, row in wave_orders.iterrows():
            loc = str(row.get('LOC', '')).strip()
            if len(loc) < 9: continue 
            shelf_id = loc[:9]
            face = loc[10] if len(loc) > 10 else 'A'
            cust_id = row.get('PARTCUSTID')
            target_st = self.cust_station_map.get(cust_id)
            if not target_st: target_st = random.choice(available_stations)
            shelf_tasks[shelf_id].append({
                'face': face, 'station': target_st,
                'sku': f"{row.get('FRCD','')}_{row.get('PARTNO','')}",
                'qty': row.get('QTY', 1), 'order_row': row
            })
        final_tasks = []
        for shelf_id, orders in shelf_tasks.items():
            orders.sort(key=lambda x: (x['station'], x['face']))
            stops = []
            current_st = None
            current_face = None
            current_sku_group = defaultdict(int) 
            for o in orders:
                st = o['station']
                face = o['face']
                sku = o['sku']
                if (st != current_st or face != current_face) and current_st is not None:
                    proc_time = self._calc_time(current_sku_group)
                    stops.append({'station': current_st, 'face': current_face, 'time': proc_time})
                    current_sku_group = defaultdict(int)
                current_st = st
                current_face = face
                current_sku_group[sku] += 1
            if current_st is not None:
                proc_time = self._calc_time(current_sku_group)
                stops.append({'station': current_st, 'face': current_face, 'time': proc_time})
            final_tasks.append({
                'shelf_id': shelf_id, 'stops': stops,
                'wave_id': orders[0]['order_row'].get('WAVE_ID'),
                'raw_orders': [o['order_row'] for o in orders]
            })
        return final_tasks
    def _calc_time(self, sku_group):
        total_time = 0
        for sku, count in sku_group.items(): total_time += 15 + (count * 5)
        return total_time

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (V88: Consolidation Fix)...")
        
        self.grid_2f = self._load_map_correct('2F_map.xlsx', 32, 61)
        self.grid_3f = self._load_map_correct('3F_map.xlsx', 32, 61)
        
        self.reservations_2f = defaultdict(set)
        self.reservations_3f = defaultdict(set)
        self.shelf_coords = self._load_shelf_coords()
        self.shelf_occupancy = {'2F': set(), '3F': set()}
        self.valid_storage_spots = {'2F': set(), '3F': set()}
        self.pos_to_sid_2f = {}
        self.pos_to_sid_3f = {}
        
        r2, c2 = self.grid_2f.shape
        for r in range(r2):
            for c in range(c2):
                if self.grid_2f[r][c] == 1: self.valid_storage_spots['2F'].add((r, c))

        r3, c3 = self.grid_3f.shape
        for r in range(r3):
            for c in range(c3):
                if self.grid_3f[r][c] == 1: self.valid_storage_spots['3F'].add((r, c))

        for sid, info in self.shelf_coords.items():
            f = info['floor']
            p = info['pos']
            if f == '2F' and p in self.valid_storage_spots['2F']: 
                self.shelf_occupancy['2F'].add(p)
                self.pos_to_sid_2f[p] = sid
            elif f == '3F' and p in self.valid_storage_spots['3F']: 
                self.shelf_occupancy['3F'].add(p)
                self.pos_to_sid_3f[p] = sid
            
        self.inventory_map = self._load_inventory() 
        self.all_tasks_raw = self._load_all_tasks()
        
        # [V88] 使用強制聚合邏輯
        self._assign_locations_smartly(self.all_tasks_raw)
        
        self.stations = self._init_stations()
        st_2f = {k:v for k,v in self.stations.items() if v['floor']=='2F'}
        st_3f = {k:v for k,v in self.stations.items() if v['floor']=='3F'}
        
        self.processor = OrderProcessor(st_2f, st_3f)
        
        self.used_spots_2f = set()
        self.used_spots_3f = set()
        self.agv_state = {
            '2F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_2f, self.used_spots_2f, '2F')} for i in range(1, 19)},
            '3F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_3f, self.used_spots_3f, '3F')} for i in range(101, 119)}
        }
        
        self.shuffler_2f = ShuffleManager(self.grid_2f, self.shelf_occupancy['2F'], self.pos_to_sid_2f, self.shelf_coords)
        self.shuffler_3f = ShuffleManager(self.grid_3f, self.shelf_occupancy['3F'], self.pos_to_sid_3f, self.shelf_coords)
        self.traffic_2f = TrafficController(self.grid_2f, self.agv_state['2F'], self.reservations_2f)
        self.traffic_3f = TrafficController(self.grid_3f, self.agv_state['3F'], self.reservations_3f)
        self.parking_2f = ParkingManager(self.grid_2f, self.shelf_occupancy['2F'])
        self.parking_3f = ParkingManager(self.grid_3f, self.shelf_occupancy['3F'])
        
        self.wave_totals = {}
        self.recv_totals = {}
        for o in self.all_tasks_raw:
            wid = str(o.get('WAVE_ID', 'UNKNOWN')) 
            d_str = o['datetime'].strftime('%Y-%m-%d')
            if 'RECEIVING' in wid: self.recv_totals[d_str] = self.recv_totals.get(d_str, 0) + 1
            else: self.wave_totals[wid] = self.wave_totals.get(wid, 0) + 1
            
        self.teleport_heatmap = Counter()

    def _load_inventory(self):
        path = os.path.join(BASE_DIR, 'data', 'master', 'item_inventory.csv')
        inv = defaultdict(list)
        try:
            df = pd.read_csv(path, dtype=str)
            cols = [c.upper() for c in df.columns]
            part_col = next((c for c in cols if 'PART' in c), None)
            cell_col = next((c for c in cols if 'CELL' in c or 'LOC' in c), None)
            if part_col and cell_col:
                for _, r in df.iterrows():
                    inv[str(r[part_col]).strip()].append(str(r[cell_col]).strip())
        except: pass
        return inv

    def _load_all_tasks(self):
        tasks = []
        path_out = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        try:
            df_out = pd.read_csv(path_out)
            df_out.columns = [c.upper() for c in df_out.columns]
            date_col = next((c for c in df_out.columns if 'DATETIME' == c), None)
            if not date_col: date_col = next((c for c in df_out.columns if 'DATE' in c or 'TIME' in c), None)
            if date_col:
                df_out['datetime'] = pd.to_datetime(df_out[date_col])
                df_out = df_out.dropna(subset=['datetime'])
                if 'LOC' not in df_out.columns: df_out['LOC'] = ''
                tasks.extend(df_out.to_dict('records'))
        except: pass
        path_in = os.path.join(BASE_DIR, 'data', 'transaction', 'historical_receiving_ex.csv')
        try:
            df_in = pd.read_csv(path_in)
            df_in.columns = [c.upper() for c in df_in.columns]
            cols = df_in.columns
            date_col = next((c for c in cols if 'DATE' in c), None)
            part_col = next((c for c in cols if 'ITEM' in c or 'PART' in c), None)
            if date_col and part_col:
                df_in['datetime'] = pd.to_datetime(df_in[date_col])
                df_in = df_in.dropna(subset=['datetime'])
                df_in['PARTNO'] = df_in[part_col]
                df_in['WAVE_ID'] = 'RECEIVING_' + df_in['datetime'].dt.strftime('%Y%m%d')
                df_in['PARTCUSTID'] = 'REC_VENDOR'
                if 'LOC' not in df_in.columns: df_in['LOC'] = ''
                tasks.extend(df_in.to_dict('records'))
        except: pass
        tasks.sort(key=lambda x: x['datetime'])
        return tasks 

    # [V88] Stickiness Logic to enforce merging
    def _assign_locations_smartly(self, tasks):
        print("   -> Running Inventory Consolidation (Stickiness Mode)...")
        part_shelf_map = {}
        valid_shelves = list(self.shelf_coords.keys())
        
        # 1. 學習已有的位置 (Learn Fixed LOCs)
        for t in tasks:
            part = str(t.get('PARTNO', '')).strip()
            loc = str(t.get('LOC', '')).strip()
            if len(loc) >= 9:
                if part not in part_shelf_map:
                    part_shelf_map[part] = loc # 記住這個料號對應的位置

        # 2. 分配缺失的位置 (Assign Missing)
        for t in tasks:
            loc = str(t.get('LOC', '')).strip()
            if len(loc) >= 9: continue 
            
            part = str(t.get('PARTNO', '')).strip()
            
            # Stickiness Check: 如果這料號之前已經有位置，就用一樣的 (最大化合併)
            if part in part_shelf_map:
                t['LOC'] = part_shelf_map[part]
            else:
                # 新料號，選擇庫存清單的第一個 (保持一致性)
                cands = self.inventory_map.get(part, [])
                if cands:
                    chosen = cands[0] 
                    t['LOC'] = chosen
                    part_shelf_map[part] = chosen
                elif valid_shelves:
                    chosen = f"{random.choice(valid_shelves)}-A-A01"
                    t['LOC'] = chosen
                    part_shelf_map[part] = chosen

    def _get_strict_spawn_spot(self, grid, used_spots, floor):
        rows, cols = grid.shape
        candidates = []
        for r in range(rows):
            for c in range(cols):
                if grid[r][c] == 0: candidates.append((r,c))
        if not candidates:
             for r in range(rows):
                for c in range(cols):
                    if grid[r][c] == 1: candidates.append((r,c))
        random.shuffle(candidates)
        for cand in candidates:
            return cand
        return (0, 0)

    def _find_smart_storage_spot(self, start_pos, valid_spots, occupied_spots, shelf_occupied_spots, agv_pool, grid, limit=50):
        if start_pos is None: return list(valid_spots)[:5]
        candidates = []
        agv_positions = [s['pos'] for s in agv_pool.values()]
        
        for spot in valid_spots:
            if spot not in shelf_occupied_spots and spot not in occupied_spots:
                dist = abs(spot[0]-start_pos[0]) + abs(spot[1]-start_pos[1])
                crowd_penalty = 0
                for apos in agv_positions:
                    d = abs(spot[0]-apos[0]) + abs(spot[1]-apos[1])
                    if d < 5: crowd_penalty += (10 - d) * 5 
                
                obstacle_count = 0
                for dr, dc in [(0,1),(0,-1),(1,0),(-1,0)]:
                    nr, nc = spot[0]+dr, spot[1]+dc
                    if not (0<=nr<grid.shape[0] and 0<=nc<grid.shape[1]): obstacle_count+=1
                    elif grid[nr][nc] == -1: obstacle_count+=1
                    elif (nr, nc) in shelf_occupied_spots: obstacle_count+=1
                
                island_penalty = 0
                if obstacle_count >= 3: island_penalty = 1000 
                
                total_score = dist + crowd_penalty + island_penalty + random.uniform(0, 10)
                candidates.append((total_score, spot))
        
        candidates.sort(key=lambda x: x[0])
        top_candidates = [x[1] for x in candidates[:limit]]
        if top_candidates:
            return [random.choice(top_candidates)]
        return []

    def _load_map_correct(self, filename, rows, cols):
        path = os.path.join(BASE_DIR, 'data', 'master', filename)
        if not os.path.exists(path): path = path.replace('.xlsx', '.csv')
        try:
            if filename.endswith('.xlsx'): df = pd.read_excel(path, header=None)
            else: df = pd.read_csv(path, header=None)
        except: return np.full((rows, cols), 0)
        raw_grid = df.iloc[0:rows, 0:cols].fillna(0).values 
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
                coords[str(r['shelf_id'])] = {'floor': r['floor'], 'pos': (int(r['y']), int(r['x']))}
        except: pass
        return coords

    def _init_stations(self):
        sts = {}
        def find_stations(grid):
            candidates = []
            rows, cols = grid.shape
            for r in range(rows):
                for c in range(cols):
                    if grid[r][c] == 2: candidates.append((r, c))
            candidates.sort() 
            return candidates
        cands_2f = find_stations(self.grid_2f)
        for i, pos in enumerate(cands_2f):
            sid = i + 1 
            sts[sid] = {'floor': '2F', 'pos': pos, 'free_time': 0}
        cands_3f = find_stations(self.grid_3f)
        for i, pos in enumerate(cands_3f):
            sid = 101 + i 
            sts[sid] = {'floor': '3F', 'pos': pos, 'free_time': 0}
        return sts

    def _is_physically_connected(self, grid, start, end):
        if grid[start[0]][start[1]] == -1 or grid[end[0]][end[1]] == -1: return False
        q = deque([start])
        visited = {start}
        while q:
            curr = q.popleft()
            if curr == end: return True
            for dr, dc in [(0,1),(0,-1),(1,0),(-1,0)]:
                nr, nc = curr[0]+dr, curr[1]+dc
                if 0<=nr<grid.shape[0] and 0<=nc<grid.shape[1] and grid[nr][nc] != -1:
                    if (nr, nc) not in visited:
                        visited.add((nr, nc))
                        q.append((nr, nc))
        return False

    def write_move_events(self, writer, path, floor, agv_id, res_table):
        if not path or len(path) < 2: return
        for i in range(len(path) - 1):
            curr_pos, curr_t = path[i]
            next_pos, next_t = path[i+1]
            res_table[curr_t].add(curr_pos) 
            writer.writerow([
                self.to_dt(curr_t), self.to_dt(next_t), floor, f"AGV_{agv_id}",
                curr_pos[1], curr_pos[0], next_pos[1], next_pos[0], 'AGV_MOVE', ''
            ])
        if path:
            res_table[path[-1][1]].add(path[-1][0])

    def _cleanup_reservations(self, res_table, limit_time):
        cutoff = limit_time - 60
        to_del = [t for t in res_table if t < cutoff]
        for t in to_del: del res_table[t]

    def _move_agv_segment(self, start_p, end_p, start_t, loaded, agv_name, floor, astar, shuffler, traffic_ctrl, w_evt, res_table, grid, is_returning=False, agv_pool=None):
        curr = start_p
        target = end_p
        t = start_t
        start_wait = t
        TIMEOUT_LIMIT = 60
        
        if not self._is_physically_connected(grid, curr, target):
             t += 120
             w_evt.writerow([self.to_dt(t-120), self.to_dt(t), floor, agv_name, curr[1], curr[0], target[1], target[0], 'AGV_MOVE', 'TELEPORT'])
             self.teleport_heatmap[curr] += 1
             return target, t, True

        retry_count = 0 

        while curr != target:
            if t - start_wait > TIMEOUT_LIMIT:
                t += 60 
                w_evt.writerow([self.to_dt(t-60), self.to_dt(t), floor, agv_name, curr[1], curr[0], target[1], target[0], 'AGV_MOVE', 'FORCE_TELE'])
                self.teleport_heatmap[curr] += 1 
                return target, t, True

            path, _ = astar.find_path(curr, target, t, is_loaded=loaded, ignore_dynamic=False)
            
            # [V88] Faster Rerouting (2 retries instead of 3)
            if not path and is_returning and retry_count > 2:
                new_candidates = self._find_smart_storage_spot(
                    curr, self.valid_storage_spots[floor], 
                    {s['pos'] for k,s in agv_pool.items()}, self.shelf_occupancy[floor], agv_pool, grid, limit=20
                )
                if new_candidates:
                    target = new_candidates[0]
                    retry_count = 0 
                    continue

            if not path and (t - start_wait > 5):
                success, penalty = traffic_ctrl.clear_path_obstacles(curr, target, t, w_evt, floor, agv_name)
                if success:
                    t += int(penalty)
                    continue

            if not path and (t - start_wait > 10): 
                success, penalty = shuffler.try_make_space(target, w_evt, t, floor, agv_name)
                if success:
                    t += int(penalty)
                    continue 
            
            if not path and (t - start_wait > 30):
                 path, _ = astar.find_path(curr, target, t, is_loaded=loaded, ignore_dynamic=True, allow_tunneling=True)
                 if path: t += 30 

            if path:
                self.write_move_events(w_evt, path, floor, agv_name, res_table)
                t = path[-1][1]
                curr = target
            else:
                backoff_time = min(2 ** retry_count, 5) 
                for k in range(backoff_time):
                    res_table[t + k].add(curr)
                t += backoff_time
                retry_count += 1
                    
        return curr, t, False

    def run(self):
        if not self.all_tasks_raw: return
        self.base_time = self.all_tasks_raw[0]['datetime']
        self.to_dt = lambda sec: self.base_time + timedelta(seconds=sec)
        
        astar_2f = TimeAwareAStar(self.grid_2f, self.reservations_2f, self.shelf_occupancy['2F'])
        astar_3f = TimeAwareAStar(self.grid_3f, self.reservations_3f, self.shelf_occupancy['3F'])
        
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])
        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        w_kpi = csv.writer(f_kpi)
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave', 'deadline_ts'])

        df_tasks = pd.DataFrame(self.all_tasks_raw)
        grouped_waves = df_tasks.groupby('WAVE_ID')
        
        task_queue_2f = deque()
        task_queue_3f = deque()
        
        for wave_id, wave_df in grouped_waves:
            wave_2f = wave_df[wave_df['LOC'].str.startswith('2')].copy()
            wave_3f = wave_df[wave_df['LOC'].str.startswith('3')].copy()
            task_queue_2f.extend(self.processor.process_wave(wave_2f, '2F'))
            task_queue_3f.extend(self.processor.process_wave(wave_3f, '3F'))
            
        total_tasks = len(task_queue_2f) + len(task_queue_3f)
        
        print(f"🎬 開始模擬... (V88: Inventory Consolidation Fix)")
        print(f"   -> 原始訂單: {len(self.all_tasks_raw)} | AGV任務: {total_tasks}")
        
        # Init outputs
        for floor in ['2F', '3F']:
            for sid, info in self.stations.items():
                if info['floor'] == floor:
                    w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"WS_{sid}", info['pos'][1], info['pos'][0], info['pos'][1], info['pos'][0], 'STATION_STATUS', 'WHITE|IDLE|N'])
            for agv_id, state in self.agv_state[floor].items():
                pos = state['pos']
                w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"AGV_{agv_id}", pos[1], pos[0], pos[1], pos[0], 'AGV_MOVE', 'INIT'])

        done_count = 0
        stats = {'Load': 0, 'Visit': 0, 'Return': 0, 'Park': 0}
        
        queues = {'2F': task_queue_2f, '3F': task_queue_3f}
        astars = {'2F': astar_2f, '3F': astar_3f}
        
        import sys 
        start_real = time.time()

        for floor in ['2F', '3F']:
            queue = queues[floor]
            astar = astars[floor]
            agv_pool = self.agv_state[floor]
            res_table = self.reservations_2f if floor=='2F' else self.reservations_3f
            grid = self.grid_2f if floor=='2F' else self.grid_3f
            shuffler = self.shuffler_2f if floor=='2F' else self.shuffler_3f
            traffic = self.traffic_2f if floor=='2F' else self.traffic_3f
            parking = self.parking_2f if floor=='2F' else self.parking_2f
            
            while queue:
                task = queue.popleft()
                best_agv = min(agv_pool, key=lambda k: agv_pool[k]['time'])
                agv_ready_time = agv_pool[best_agv]['time']
                
                shelf_id = task['shelf_id']
                if shelf_id not in self.shelf_coords: 
                    done_count += 1; continue
                    
                shelf_pos = self.shelf_coords[shelf_id]['pos']
                if grid[shelf_pos[0]][shelf_pos[1]] == -1:
                    shelf_pos = self._get_strict_spawn_spot(grid, set(), floor)

                current_t = agv_ready_time
                agv_pos = agv_pool[best_agv]['pos']
                if grid[agv_pos[0]][agv_pos[1]] == -1:
                    agv_pos = self._get_strict_spawn_spot(grid, set(), floor)

                # 1. Load
                agv_pos, current_t, tele_1 = self._move_agv_segment(
                    agv_pos, shelf_pos, current_t, False, f"AGV_{best_agv}", 
                    floor, astar, shuffler, traffic, w_evt, res_table, grid,
                    is_returning=False, agv_pool=agv_pool
                )
                if tele_1: stats['Load'] += 1
                
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0], 'SHELF_LOAD', f"Task_{done_count}"])
                current_t += 5
                
                if shelf_pos in self.shelf_occupancy[floor]: self.shelf_occupancy[floor].remove(shelf_pos)
                current_shelf_pos = shelf_pos
                
                # 2. Visit
                for stop in task['stops']:
                    target_st = stop['station']
                    if target_st not in self.stations: continue
                    st_pos = self.stations[target_st]['pos']
                    
                    nearby_agvs = 0
                    for s in agv_pool.values():
                        if abs(s['pos'][0]-st_pos[0]) + abs(s['pos'][1]-st_pos[1]) < 5:
                            nearby_agvs += 1
                    
                    if nearby_agvs > 3: 
                        park_spot = parking.find_parking_spot(current_shelf_pos, set())
                        if park_spot:
                            current_shelf_pos, current_t, _ = self._move_agv_segment(
                                current_shelf_pos, park_spot, current_t, True, f"AGV_{best_agv}",
                                floor, astar, shuffler, traffic, w_evt, res_table, grid, is_returning=False, agv_pool=agv_pool
                            )
                            current_t += 20 
                    
                    current_shelf_pos, current_t, tele_2 = self._move_agv_segment(
                        current_shelf_pos, st_pos, current_t, True, f"AGV_{best_agv}",
                        floor, astar, shuffler, traffic, w_evt, res_table, grid,
                        is_returning=False, agv_pool=agv_pool
                    )
                    if tele_2: stats['Visit'] += 1
                    
                    leave_t = current_t + stop['time']
                    w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"WS_{target_st}", st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'STATION_STATUS', 'BLUE|BUSY|N'])
                    w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"AGV_{best_agv}", st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'PICKING', f"Processing"])
                    for t in range(current_t, int(leave_t)): res_table[t].add(st_pos)
                    current_t = int(leave_t)
                
                # 3. Return (Smart & Dispersed & Island-Aware)
                candidates = self._find_smart_storage_spot(
                    current_shelf_pos, self.valid_storage_spots[floor], 
                    {s['pos'] for k,s in agv_pool.items()}, self.shelf_occupancy[floor], agv_pool, grid, limit=50
                )
                if not candidates: candidates = [shelf_pos]
                drop_pos = candidates[0]
                
                if grid[drop_pos[0]][drop_pos[1]] == -1:
                     drop_pos = self._get_strict_spawn_spot(grid, set(), floor)

                current_shelf_pos, current_t, tele_3 = self._move_agv_segment(
                    current_shelf_pos, drop_pos, current_t, True, f"AGV_{best_agv}",
                    floor, astar, shuffler, traffic, w_evt, res_table, grid,
                    is_returning=True, agv_pool=agv_pool
                )
                if tele_3: stats['Return'] += 1

                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", drop_pos[1], drop_pos[0], drop_pos[1], drop_pos[0], 'SHELF_UNLOAD', 'Done'])
                current_t += 5
                
                self.shelf_occupancy[floor].add(drop_pos)
                self.shelf_coords[shelf_id]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['time'] = current_t
                
                # 4. Aggressive Parking
                nearest_st_dist = float('inf')
                for st_info in self.stations.values():
                    if st_info['floor'] == floor:
                        d = abs(drop_pos[0]-st_info['pos'][0]) + abs(drop_pos[1]-st_info['pos'][1])
                        if d < nearest_st_dist: nearest_st_dist = d
                
                if nearest_st_dist < 10:
                    park_spot = parking.find_parking_spot(drop_pos, set())
                    if park_spot:
                        current_shelf_pos, current_t, tele_4 = self._move_agv_segment(
                            drop_pos, park_spot, current_t, False, f"AGV_{best_agv}",
                            floor, astar, shuffler, traffic, w_evt, res_table, grid, is_returning=False, agv_pool=agv_pool
                        )
                        if not tele_4:
                            self.agv_state[floor][best_agv]['pos'] = park_spot
                            self.agv_state[floor][best_agv]['time'] = current_t
                            w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+1), floor, f"AGV_{best_agv}", park_spot[1], park_spot[0], park_spot[1], park_spot[0], 'PARKING', 'Hidden'])
                            stats['Park'] += 1

                for raw_o in task['raw_orders']:
                     wid = raw_o.get('WAVE_ID', 'UNK')
                     ttype = 'INBOUND' if 'RECEIVING' in wid else 'OUTBOUND'
                     w_kpi.writerow([self.to_dt(current_t), ttype, wid, 'N', self.to_dt(current_t).date(), f"WS_{task['stops'][-1]['station']}", 0, 0])

                done_count += 1
                
                if done_count % 10 == 0 or done_count == total_tasks: 
                     elapsed = time.time()-start_real
                     print(f"\r🚀 進度:{done_count} | ⏱️ {elapsed:.0f}s | ⚡ Load:{stats['Load']} | Visit:{stats['Visit']} | Return:{stats['Return']} | 🅿️ Park:{stats['Park']}", end='')
                     self._cleanup_reservations(res_table, current_t)

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！ Total Teleports: {sum(stats.values())}")
        print("\n🔥 [Top 3 Death Spots (Teleport Hotspots)]")
        for pos, count in self.teleport_heatmap.most_common(3):
            print(f"   📍 座標 {pos}: {count} 次 (建議檢查該區域是否為死路)")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()