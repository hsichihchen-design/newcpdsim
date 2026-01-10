import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
from collections import defaultdict, deque, Counter
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

    def find_path(self, start, goal, start_time_sec, static_blockers=None, is_loaded=False, ignore_dynamic=False):
        if start == goal: return [(start, start_time_sec)], start_time_sec
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None
        
        if static_blockers is None: static_blockers = set()

        open_set = []
        h_start = self.heuristic(start, goal)
        heapq.heappush(open_set, (h_start, h_start, start_time_sec, start, (0,0)))
        
        came_from = {}
        g_score = {(start, start_time_sec, (0,0)): 0}
        
        max_steps = 6000 # Increased limit
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
                    # 1. Static Blockers (Other AGVs fixed pos)
                    if not ignore_dynamic:
                        if (nr, nc) in static_blockers and (nr, nc) != goal and (nr, nc) != start:
                            continue

                    val = self.grid[nr][nc]
                    step_cost = NORMAL_COST
                    
                    # 2. Map Obstacles
                    if val == -1: 
                        step_cost = OBSTACLE_COST # Wall is always obstacle
                    elif val == 1:
                        # Shelf area
                        if (nr, nc) in self.valid_storage_spots: 
                            if is_loaded:
                                # Loaded: treat other shelves as obstacles unless ignore_dynamic=True (Soft Fallback)
                                if not ignore_dynamic and (nr, nc) != goal and (nr, nc) != start:
                                    step_cost = OBSTACLE_COST
                                else:
                                    step_cost = NORMAL_COST * 2 # Penalty for moving through shelves
                            else:
                                step_cost = NORMAL_COST
                        else:
                            # Non-valid spot but marked as 1? Treat as obstacle
                            if (nr, nc) != goal and (nr, nc) != start: step_cost = OBSTACLE_COST
                    
                    if step_cost >= OBSTACLE_COST: continue

                    # 3. Dynamic Reservations
                    if not ignore_dynamic:
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
                        
        if best_node and ignore_dynamic:
             # In fallback mode, return partial path if goal not reached
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
        print(f"🚀 [Step 4] 啟動進階模擬 (V50: Smart Fallback & Strict Stations)...")
        
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
            
        self.inventory_map = self._load_inventory() 
        self.all_tasks_raw = self._load_all_tasks()
        self._assign_locations_smartly(self.all_tasks_raw)
        
        # [V50 Fix] Stations Init Logic
        self.stations = self._init_stations()
        st_2f = {k:v for k,v in self.stations.items() if v['floor']=='2F'}
        st_3f = {k:v for k,v in self.stations.items() if v['floor']=='3F'}
        self.processor = OrderProcessor(st_2f, st_3f)
        
        print("   -> 初始化車隊...")
        self.used_spots_2f = set()
        self.used_spots_3f = set()
        self.agv_state = {
            '2F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_2f, self.used_spots_2f, '2F')} for i in range(1, 19)},
            '3F': {i: {'time': 0, 'pos': self._get_strict_spawn_spot(self.grid_3f, self.used_spots_3f, '3F')} for i in range(101, 119)}
        }
        
        self.wave_totals = {}
        self.recv_totals = {}
        for o in self.all_tasks_raw:
            wid = str(o.get('WAVE_ID', 'UNKNOWN')) 
            d_str = o['datetime'].strftime('%Y-%m-%d')
            if 'RECEIVING' in wid: self.recv_totals[d_str] = self.recv_totals.get(d_str, 0) + 1
            else: self.wave_totals[wid] = self.wave_totals.get(wid, 0) + 1

    # --- Helper Functions ---
    def _load_inventory(self):
        path = os.path.join(BASE_DIR, 'data', 'master', 'item_inventory.csv')
        inv = defaultdict(list)
        try:
            df = pd.read_csv(path, dtype=str)
            cols = [c.upper() for c in df.columns]
            df.columns = cols 
            part_col = next((c for c in cols if 'PART' in c), None)
            cell_col = next((c for c in cols if 'CELL' in c or 'LOC' in c), None)
            if part_col and cell_col:
                for _, r in df.iterrows():
                    part = str(r[part_col]).strip()
                    loc = str(r[cell_col]).strip()
                    if loc: inv[part].append(loc)
            print(f"   -> Inventory Loaded: {len(inv)} items")
        except Exception as e: print(f"⚠️ Inventory Load Error: {e}")
        return inv

    def _load_all_tasks(self):
        tasks = []
        path_out = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        valid_shelves_list = list(self.shelf_coords.keys())
        
        def resolve_loc(row):
            if 'LOC' in row and pd.notna(row['LOC']) and len(str(row['LOC'])) > 9: return str(row['LOC']).strip()
            part = str(row.get('PARTNO', '')).strip()
            candidates = self.inventory_map.get(part, [])
            if candidates:
                c = candidates[0]
                if len(c) >= 11: return c
                if len(c) == 9: return f"{c}-A-A01"
            if valid_shelves_list: return f"{random.choice(valid_shelves_list)}-A-A01"
            return ""

        try:
            df_out = pd.read_csv(path_out)
            df_out.columns = [c.upper() for c in df_out.columns]
            date_col = next((c for c in df_out.columns if 'DATETIME' == c), None)
            if not date_col: date_col = next((c for c in df_out.columns if 'DATE' in c or 'TIME' in c), None)
            
            if date_col:
                df_out['datetime'] = pd.to_datetime(df_out[date_col])
                df_out = df_out.dropna(subset=['datetime'])
                if 'LOC' not in df_out.columns: df_out['LOC'] = ''
                df_out['LOC'] = df_out.apply(resolve_loc, axis=1)
                df_out = df_out[df_out['LOC'].str.len() >= 9]
                tasks.extend(df_out.to_dict('records'))
        except Exception as e: print(f"⚠️ Load Orders Error: {e}")
        
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
                df_in['LOC'] = df_in.apply(resolve_loc, axis=1)
                df_in = df_in[df_in['LOC'].str.len() >= 9]
                tasks.extend(df_in.to_dict('records'))
        except: pass
        
        tasks.sort(key=lambda x: x['datetime'])
        return tasks

    def _assign_locations_smartly(self, tasks):
        print("   -> Running Smart Shelf Assignment...")
        shelf_demand = Counter()
        for t in tasks:
            if t['LOC'] and len(str(t['LOC'])) >= 9:
                sid = str(t['LOC'])[:9]
                shelf_demand[sid] += 1
        
        for t in tasks:
            if t['LOC'] and len(str(t['LOC'])) >= 9: continue
            part = str(t.get('PARTNO', '')).strip()
            candidates = self.inventory_map.get(part, [])
            if not candidates: continue
            best_loc = None
            max_score = -1
            for loc in candidates:
                if len(loc) < 9: continue
                sid = loc[:9]
                score = shelf_demand[sid]
                if score > max_score: max_score = score; best_loc = loc
            if best_loc:
                if len(best_loc) == 9: best_loc += "-A-A01"
                t['LOC'] = best_loc
                shelf_demand[best_loc[:9]] += 1

    def _get_strict_spawn_spot(self, grid, used_spots, floor):
        rows, cols = grid.shape
        candidates = []
        for r in range(rows):
            for c in range(cols):
                if grid[r][c] == 0: candidates.append((r,c))
        if not candidates: candidates = list(self.valid_storage_spots[floor])
        random.shuffle(candidates)
        for cand in candidates:
            if cand not in used_spots: used_spots.add(cand); return cand
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

    def _init_stations(self):
        sts = {}
        # [V50 Fix] Strict 8 stations per floor
        def find_stations(grid):
            candidates = []
            rows, cols = grid.shape
            for r in range(rows):
                for c in range(cols):
                    if grid[r][c] == 2: candidates.append((r, c))
            candidates.sort() # Sort by coordinate
            return candidates

        cands_2f = find_stations(self.grid_2f)
        for i in range(8):
            sid = i + 1
            pos = cands_2f[i] if i < len(cands_2f) else (cands_2f[-1] if cands_2f else (0,0))
            sts[sid] = {'floor': '2F', 'pos': pos, 'free_time': 0}

        cands_3f = find_stations(self.grid_3f)
        for i in range(8):
            sid = 101 + i
            pos = cands_3f[i] if i < len(cands_3f) else (cands_3f[-1] if cands_3f else (0,0))
            sts[sid] = {'floor': '3F', 'pos': pos, 'free_time': 0}
            
        return sts

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

    def _cleanup_reservations(self, res_table, limit_time):
        cutoff = limit_time - 60
        to_del = [t for t in res_table if t < cutoff]
        for t in to_del: del res_table[t]

    def _generate_fallback_path(self, start, end, start_time):
        path = []
        curr = list(start)
        t = start_time
        path.append((tuple(curr), t))
        while curr[0] != end[0]:
            curr[0] += 1 if end[0] > curr[0] else -1
            t += 1
            path.append((tuple(curr), t))
        while curr[1] != end[1]:
            curr[1] += 1 if end[1] > curr[1] else -1
            t += 1
            path.append((tuple(curr), t))
        return path

    def smart_move(self, agv_id, start_pos, target_pos, floor, start_time, w_evt, res_table, astar, is_loaded_mode):
        # 1. Normal A*
        path, _ = astar.find_path(start_pos, target_pos, start_time, is_loaded=is_loaded_mode)
        if path:
            self.write_move_events(w_evt, path, floor, agv_id, res_table)
            return path[-1][1]
        
        # 2. Try Clearing Obstacles (if loaded)
        if is_loaded_mode:
            # First try path with ignore_dynamic=True to see if wall-safe path exists
            path_soft, _ = astar.find_path(start_pos, target_pos, start_time, is_loaded=is_loaded_mode, ignore_dynamic=True)
            
            if path_soft:
                # If a soft path exists, it means we are blocked by dynamic obstacles (AGVs/Shelves).
                # Trigger clearing logic.
                new_time = self._execute_obstacle_clearing(start_pos, floor, agv_id, start_time, w_evt, res_table, astar, is_carrying_main_shelf=True)
                path, _ = astar.find_path(start_pos, target_pos, new_time, is_loaded=True)
                if path:
                    self.write_move_events(w_evt, path, floor, agv_id, res_table)
                    return path[-1][1]
            
            # Fallback 1: Use the soft path (ignores reservations, might overlap but won't hit walls)
            if path_soft:
                self.write_move_events(w_evt, path_soft, floor, agv_id, res_table)
                return path_soft[-1][1]
            
            # Fallback 2: Manhattan (Last resort, might hit walls)
            fb_path = self._generate_fallback_path(start_pos, target_pos, start_time)
            self.write_move_events(w_evt, fb_path, floor, agv_id, res_table)
            return fb_path[-1][1]
            
        else:
            # Empty AGV blocked? Try soft path first
            path_soft, _ = astar.find_path(start_pos, target_pos, start_time, is_loaded=False, ignore_dynamic=True)
            if path_soft:
                self.write_move_events(w_evt, path_soft, floor, agv_id, res_table)
                return path_soft[-1][1]
                
            fb_path = self._generate_fallback_path(start_pos, target_pos, start_time)
            self.write_move_events(w_evt, fb_path, floor, agv_id, res_table)
            return fb_path[-1][1]

    def _execute_obstacle_clearing(self, target_pos, floor, agv_id, start_time, w_evt, res_table, astar, is_carrying_main_shelf):
        grid = self.grid_2f if floor == '2F' else self.grid_3f
        current_t = start_time
        
        if is_carrying_main_shelf:
            w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", target_pos[1], target_pos[0], target_pos[1], target_pos[0], 'SHELF_UNLOAD', 'Temp Drop'])
            current_t += 5
            self.shelf_occupancy[floor].add(target_pos)
        
        blocking_neighbors = []
        for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
            nr, nc = target_pos[0]+dr, target_pos[1]+dc
            if 0<=nr<grid.shape[0] and 0<=nc<grid.shape[1]:
                if (nr,nc) in self.shelf_occupancy[floor]: blocking_neighbors.append((nr,nc))
        
        if not blocking_neighbors:
            if is_carrying_main_shelf:
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", target_pos[1], target_pos[0], target_pos[1], target_pos[0], 'SHELF_LOAD', 'Reload'])
                current_t += 5
                self.shelf_occupancy[floor].remove(target_pos)
            return current_t

        obstacle_to_move = blocking_neighbors[0]
        buffer_pos = self._find_accessible_buffer(obstacle_to_move, grid, self.shelf_occupancy[floor], astar, current_t)
        
        if not buffer_pos: 
            if is_carrying_main_shelf:
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", target_pos[1], target_pos[0], target_pos[1], target_pos[0], 'SHELF_LOAD', 'Deadlock'])
                current_t += 5
                self.shelf_occupancy[floor].remove(target_pos)
            return current_t + 60

        current_t = self.smart_move(agv_id, target_pos, obstacle_to_move, floor, current_t, w_evt, res_table, astar, is_loaded_mode=False)
        w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", obstacle_to_move[1], obstacle_to_move[0], obstacle_to_move[1], obstacle_to_move[0], 'SHELF_LOAD', 'Clear Obs'])
        current_t += 5
        self.shelf_occupancy[floor].remove(obstacle_to_move)
        
        current_t = self.smart_move(agv_id, obstacle_to_move, buffer_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=True)
        w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", buffer_pos[1], buffer_pos[0], buffer_pos[1], buffer_pos[0], 'SHELF_UNLOAD', ''])
        current_t += 5
        self.shelf_occupancy[floor].add(buffer_pos)
        
        for sid, info in self.shelf_coords.items():
            if info['floor'] == floor and info['pos'] == obstacle_to_move: self.shelf_coords[sid]['pos'] = buffer_pos; break
        
        current_t = self.smart_move(agv_id, buffer_pos, target_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=False)
        
        if is_carrying_main_shelf:
            w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{agv_id}", target_pos[1], target_pos[0], target_pos[1], target_pos[0], 'SHELF_LOAD', 'Reload Main'])
            current_t += 5
            self.shelf_occupancy[floor].remove(target_pos)
            
        self.agv_state[floor][int(agv_id)]['pos'] = target_pos
        return current_t

    def _find_accessible_buffer(self, start_pos, grid, occupied_spots, astar, start_time):
        rows, cols = grid.shape
        q = deque([(start_pos, 0)])
        visited = {start_pos}
        while q:
            curr, dist = q.popleft()
            if dist > 10: break
            r, c = curr
            is_valid_spot = (grid[r][c] == 0) or (grid[r][c] == 1 and curr not in occupied_spots)
            if is_valid_spot and curr != start_pos:
                path, _ = astar.find_path(start_pos, curr, start_time, is_loaded=True)
                if path: return curr
            for dr, dc in [(0,1),(0,-1),(1,0),(-1,0)]:
                nr, nc = r+dr, c+dc
                if 0<=nr<rows and 0<=nc<cols and (nr,nc) not in visited: visited.add((nr,nc)); q.append(((nr,nc), dist+1))
        return None

    def run(self):
        if not self.all_tasks_raw: return
        self.base_time = self.all_tasks_raw[0]['datetime']
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

        print(f"🎬 開始模擬... (Task Chaining Activated)")
        print(f"   -> Effective Orders: {len(self.all_tasks_raw)}")
        start_real = time.time()
        
        # [V50 Fix] Force output initial status for ALL stations (1-8, 101-108)
        for floor in ['2F', '3F']:
            for sid, info in self.stations.items():
                if info['floor'] == floor:
                    w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"WS_{sid}", info['pos'][1], info['pos'][0], info['pos'][1], info['pos'][0], 'STATION_STATUS', 'WHITE|IDLE|N'])

        for floor in ['2F', '3F']:
            for agv_id, state in self.agv_state[floor].items():
                pos = state['pos']
                w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"AGV_{agv_id}", pos[1], pos[0], pos[1], pos[0], 'AGV_MOVE', 'INIT'])

        df_tasks = pd.DataFrame(self.all_tasks_raw)
        grouped_waves = df_tasks.groupby('WAVE_ID')
        
        task_queue_2f = deque()
        task_queue_3f = deque()
        
        print("   -> Generating Task Chains...")
        for wave_id, wave_df in grouped_waves:
            wave_2f = wave_df[wave_df['LOC'].str.startswith('2')].copy()
            wave_3f = wave_df[wave_df['LOC'].str.startswith('3')].copy()
            
            tasks_2f = self.processor.process_wave(wave_2f, '2F')
            tasks_3f = self.processor.process_wave(wave_3f, '3F')
            
            task_queue_2f.extend(tasks_2f)
            task_queue_3f.extend(tasks_3f)
            
        print(f"   -> Tasks: 2F={len(task_queue_2f)}, 3F={len(task_queue_3f)}")
        
        total_tasks = len(task_queue_2f) + len(task_queue_3f)
        done_count = 0
        
        queues = {'2F': task_queue_2f, '3F': task_queue_3f}
        astars = {'2F': astar_2f, '3F': astar_3f}
        
        for floor in ['2F', '3F']:
            queue = queues[floor]
            astar = astars[floor]
            agv_pool = self.agv_state[floor]
            res_table = self.reservations_2f if floor=='2F' else self.reservations_3f
            
            while queue:
                task = queue.popleft()
                best_agv = min(agv_pool, key=lambda k: agv_pool[k]['time'])
                agv_ready_time = agv_pool[best_agv]['time']
                
                shelf_id = task['shelf_id']
                if shelf_id not in self.shelf_coords: 
                    done_count += 1; continue
                    
                shelf_pos = self.shelf_coords[shelf_id]['pos']
                
                current_t = agv_ready_time
                agv_pos = agv_pool[best_agv]['pos']
                
                current_t = self.smart_move(best_agv, agv_pos, shelf_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=False)
                
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0], 'SHELF_LOAD', f"Task_{done_count}"])
                current_t += 5
                if shelf_pos in self.shelf_occupancy[floor]: self.shelf_occupancy[floor].remove(shelf_pos)
                
                current_shelf_pos = shelf_pos
                
                for stop in task['stops']:
                    target_st = stop['station']
                    st_pos = self.stations[target_st]['pos']
                    proc_time = stop['time']
                    
                    if current_shelf_pos == st_pos:
                        buffer_pos = self._find_accessible_buffer(st_pos, self.grid_2f if floor=='2F' else self.grid_3f, self.shelf_occupancy[floor], astar, current_t)
                        if not buffer_pos: buffer_pos = (st_pos[0]+1, st_pos[1])
                        
                        current_t = self.smart_move(best_agv, st_pos, buffer_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=True)
                        current_t = self.smart_move(best_agv, buffer_pos, st_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=True)
                    else:
                        current_t = self.smart_move(best_agv, current_shelf_pos, st_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=True)
                    
                    current_shelf_pos = st_pos
                    
                    leave_t = current_t + proc_time
                    w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"WS_{target_st}", st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'STATION_STATUS', 'BLUE|BUSY|N'])
                    w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"AGV_{best_agv}", st_pos[1], st_pos[0], st_pos[1], st_pos[0], 'PICKING', f"Processing"])
                    
                    for t in range(current_t, int(leave_t)): res_table[t].add(st_pos)
                    current_t = int(leave_t)
                
                candidates = self._find_nearest_valid_storage(current_shelf_pos, self.valid_storage_spots[floor], {s['pos'] for k,s in agv_pool.items()}, self.shelf_occupancy[floor])
                if not candidates: candidates = [shelf_pos]
                drop_pos = candidates[0]
                
                current_t = self.smart_move(best_agv, current_shelf_pos, drop_pos, floor, current_t, w_evt, res_table, astar, is_loaded_mode=True)
                
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", drop_pos[1], drop_pos[0], drop_pos[1], drop_pos[0], 'SHELF_UNLOAD', 'Done'])
                current_t += 5
                
                self.shelf_occupancy[floor].add(drop_pos)
                self.shelf_coords[shelf_id]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['time'] = current_t
                
                for raw_o in task['raw_orders']:
                    wid = raw_o.get('WAVE_ID', 'UNK')
                    ttype = 'INBOUND' if 'RECEIVING' in wid else 'OUTBOUND'
                    w_kpi.writerow([self.to_dt(current_t), ttype, wid, 'N', self.to_dt(current_t).date(), f"WS_{task['stops'][-1]['station']}", 0, 0])

                done_count += 1
                if done_count % 50 == 0:
                    print(f"\r🚀 進度: {done_count}/{total_tasks} Tasks (Time: {time.time()-start_real:.1f}s)", end='')
                    self._cleanup_reservations(res_table, current_t)

        f_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！總耗時 {time.time() - start_real:.2f} 秒")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()