import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
import re
import math
from collections import defaultdict, deque, Counter
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
# ----------------------------------------

class BatchWriter:
    def __init__(self, filepath, header, chunk_size=20000):
        self.f = open(filepath, 'w', newline='', encoding='utf-8')
        self.writer = csv.writer(self.f)
        self.writer.writerow(header)
        self.buffer = []
        self.chunk_size = chunk_size
    
    def writerow(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size:
            self.flush()
            
    def flush(self):
        if self.buffer:
            self.writer.writerows(self.buffer)
            self.buffer = []
            
    def close(self):
        self.flush()
        self.f.close()

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
        dynamic_max_steps = max(500, dist * 15) 
        HEURISTIC_WEIGHT = 2.0 

        open_set = []
        h_start = self.heuristic(start, goal)
        heapq.heappush(open_set, (h_start, h_start, start_time_sec, start, (0,0)))
        
        g_score = {(start, start_time_sec, (0,0)): 0}
        came_from = {}
        
        steps = 0
        NORMAL_COST = 1      
        TURNING_COST = 1.0   
        WAIT_COST = 1.0       
        TUNNEL_COST = 50.0 

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
                    if self.grid[nr][nc] == -1: continue 
                    
                    step_cost = NORMAL_COST

                    if not ignore_dynamic:
                        if (next_time - start_time_sec) < 60: 
                            if next_time in self.reservations:
                                if (nr, nc) in self.reservations[next_time]: continue
                                if current_time in self.reservations:
                                    if ((nr, nc) in self.reservations[current_time]) and \
                                       (current in self.reservations[next_time]):
                                        continue

                    is_spot_occupied = ((nr, nc) in self.shelf_occupancy)
                    
                    if is_loaded and is_spot_occupied:
                        if (nr, nc) != goal and (nr, nc) != start:
                            if allow_tunneling: step_cost += TUNNEL_COST 
                            else: continue 
                    elif not is_loaded and is_spot_occupied:
                        step_cost += NORMAL_COST * 3

                    if dr == 0 and dc == 0: step_cost += WAIT_COST
                    elif (dr, dc) != last_move and last_move != (0,0): step_cost += TURNING_COST

                    new_g = g_score.get((current, current_time, last_move), float('inf')) + step_cost
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
        
        while curr != target and steps_checked < 6:
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
                floor, f"AGV_{blocker_id}", blocker_pos[1], blocker_pos[0], sanctuary[1], sanctuary[0], 'YIELD', f'Yield for {my_agv_name}'
            ])
            for t in range(int(cost) + 5):
                self.reservations[current_time + t].add(sanctuary)
            return True, cost
        return False, 0

    def _find_sanctuary(self, start_pos, current_time):
        q = deque([start_pos])
        visited = {start_pos}
        max_search = 100 
        count = 0
        while q and count < max_search:
            curr = q.popleft()
            count += 1
            if curr != start_pos and self.grid[curr[0]][curr[1]] != -1:
                is_reserved = False
                for t in range(3): 
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

    def attempt_backtrack(self, current_pos, goal_pos, current_time, w_evt, floor, agv_name):
        best_retreat = None
        max_dist = -1
        for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
            nr, nc = current_pos[0]+dr, current_pos[1]+dc
            if 0 <= nr < self.rows and 0 <= nc < self.cols:
                if self.grid[nr][nc] != -1:
                    is_occupied = False
                    for state in self.agv_pool.values():
                        if state['pos'] == (nr, nc): is_occupied = True; break
                    
                    if not is_occupied:
                        dist_to_goal = abs(nr - goal_pos[0]) + abs(nc - goal_pos[1])
                        if dist_to_goal > max_dist:
                            max_dist = dist_to_goal
                            best_retreat = (nr, nc)
        
        if best_retreat:
            w_evt.writerow([
                datetime.fromtimestamp(current_time), datetime.fromtimestamp(current_time+5), 
                floor, agv_name, current_pos[1], current_pos[0], best_retreat[1], best_retreat[0], 'YIELD', 'Backtracking'
            ])
            for t in range(current_time, current_time+10):
                self.reservations[t].add(best_retreat)
            return True, best_retreat, 5
        return False, current_pos, 0

class CleanupManager:
    def __init__(self):
        self.pending_tasks = deque()
    
    def add_task(self, buffer_pos, original_pos, shelf_id):
        self.pending_tasks.append((buffer_pos, original_pos, shelf_id))
    
    def get_nearest_task(self, agv_pos):
        if not self.pending_tasks: return None
        best_idx = -1
        min_dist = float('inf')
        for i, task in enumerate(self.pending_tasks):
            buf_pos = task[0]
            dist = abs(buf_pos[0]-agv_pos[0]) + abs(buf_pos[1]-agv_pos[1])
            if dist < min_dist:
                min_dist = dist
                best_idx = i
        if best_idx != -1:
            task = self.pending_tasks[best_idx]
            del self.pending_tasks[best_idx]
            return task 
        return None

class ShuffleManager:
    def __init__(self, grid, shelf_occupancy, pos_to_shelf_id_map, shelf_coords, cleanup_mgr):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.occupancy = shelf_occupancy
        self.pos_to_id = pos_to_shelf_id_map
        self.shelf_coords = shelf_coords
        self.cleanup_mgr = cleanup_mgr

    def execute_shuffle_and_leave(self, agv_pos, target_pos, w_evt, current_time, floor, agv_name, astar, res_table, write_move_fn, base_dt):
        blockers = []
        for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
            nr, nc = target_pos[0]+dr, target_pos[1]+dc
            if (nr, nc) in self.occupancy:
                blockers.append((nr, nc))
        
        if not blockers: return False, current_time, agv_pos

        blk_pos = blockers[0]
        sid_blk = self.pos_to_id.get(blk_pos, "Unknown")
        
        buffer_pos = self._find_smart_buffer(blk_pos, exclude={target_pos})
        if not buffer_pos: return False, current_time, agv_pos

        t = current_time
        curr = agv_pos
        start_t = t
        
        # [V63] Visible Shuffle: Increased duration to 10s per step
        t, curr = self._run_move(curr, blk_pos, t, False, astar, w_evt, floor, agv_name, res_table, write_move_fn, base_dt)
        if not t: return False, start_t, agv_pos
        
        self._log_event(w_evt, base_dt, t, floor, agv_name, blk_pos, 'SHUFFLE_LOAD', f"Mov Blk {sid_blk}")
        if blk_pos in self.occupancy: self.occupancy.remove(blk_pos)
        t += 10 # More time to see it

        t_buf, curr_buf = self._run_move(curr, buffer_pos, t, True, astar, w_evt, floor, agv_name, res_table, write_move_fn, base_dt)
        if not t_buf: 
            self.occupancy.add(blk_pos)
            return False, start_t, agv_pos 
            
        t = t_buf
        curr = curr_buf
        self._log_event(w_evt, base_dt, t, floor, agv_name, buffer_pos, 'SHUFFLE_UNLOAD', f"Drop Aside {sid_blk}")
        self.occupancy.add(buffer_pos)
        
        if sid_blk != "Unknown" and sid_blk in self.shelf_coords: 
            self.shelf_coords[sid_blk]['pos'] = buffer_pos
        if blk_pos in self.pos_to_id: del self.pos_to_id[blk_pos]
        self.pos_to_id[buffer_pos] = sid_blk
        
        t += 10 # More time to see it
        self.cleanup_mgr.add_task(buffer_pos, blk_pos, sid_blk)
        
        return True, t, curr

    def _run_move(self, start, end, t, loaded, astar, w_evt, floor, agv_name, res_table, write_move_fn, base_dt):
        path, end_t = astar.find_path(start, end, t, is_loaded=loaded, ignore_dynamic=True, allow_tunneling=True)
        if not path: return None, None
        write_move_fn(w_evt, path, floor, agv_name.replace("AGV_", ""), res_table)
        return end_t, end

    def _log_event(self, w_evt, base_dt, t, floor, agv, pos, type_, text):
        w_evt.writerow([
            base_dt + timedelta(seconds=t), base_dt + timedelta(seconds=t+10), 
            floor, agv, pos[1], pos[0], pos[1], pos[0], type_, text
        ])

    def _find_smart_buffer(self, start_pos, exclude, limit_radius=10):
        q = deque([start_pos])
        visited = {start_pos}
        candidates = []
        while q:
            curr = q.popleft()
            if self.grid[curr[0]][curr[1]] != -1 and curr not in self.occupancy and curr not in exclude:
                prio = 0 if self.grid[curr[0]][curr[1]] == 1 else 1 
                dist = abs(curr[0]-start_pos[0]) + abs(curr[1]-start_pos[1])
                candidates.append((prio, dist, curr))
            if abs(curr[0]-start_pos[0]) + abs(curr[1]-start_pos[1]) > limit_radius: continue
            for dr, dc in [(0,1), (0,-1), (1,0), (-1,0)]:
                nr, nc = curr[0]+dr, curr[1]+dc
                if 0<=nr<self.rows and 0<=nc<self.cols:
                    if (nr, nc) not in visited:
                        visited.add((nr, nc))
                        q.append((nr, nc))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            return candidates[0][2]
        return None
    
    def _find_nearest_empty(self, start_pos, exclude, limit_radius=10):
        return self._find_smart_buffer(start_pos, exclude, limit_radius)

class ParkingManager:
    def __init__(self, grid, valid_storage_spots, shelf_occupancy):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.shelf_occupancy = shelf_occupancy
        # [V63] Efficient Parking Set
        self.valid_spots_list = list(valid_storage_spots)
        self.all_spots_set = set(valid_storage_spots)
    
    def get_fast_parking_spot(self, agv_pool):
        # O(1) Approach: Filter available spots
        # This is expensive to do every time, so we sample smarter.
        # Logic: Randomly pick from valid_spots, check occupancy.
        # But if fail often, we scan.
        occupied_by_agvs = {s['pos'] for s in agv_pool.values()}
        
        # Fast Path: 50 attempts
        for _ in range(50):
            spot = random.choice(self.valid_spots_list)
            if spot not in self.shelf_occupancy and spot not in occupied_by_agvs:
                return spot
        
        # Slow Path: Linear Scan (Fallback to avoid NoSpot)
        for spot in self.valid_spots_list:
            if spot not in self.shelf_occupancy and spot not in occupied_by_agvs:
                return spot
                
        return None

class PhysicalQueueManager:
    def __init__(self, stations_info):
        self.station_queues = {} 
        for sid, info in stations_info.items():
            r, c = info['pos']
            q_slots = []
            for col in range(2, 7): # Col 2-6
                q_slots.append((r, col))
            
            exits = [(r-1, 1), (r+1, 1)]
            
            self.station_queues[sid] = {
                'slots': q_slots,
                'exits': exits,
                'occupants': [None] * len(q_slots),
                'processing': None 
            }

    def get_target_for_agv(self, sid, agv_id):
        q_data = self.station_queues.get(sid)
        if not q_data: return None, False 
        
        if q_data['processing'] == agv_id:
            return None, True 
            
        if agv_id in q_data['occupants']:
            idx = q_data['occupants'].index(agv_id)
            if idx == 0:
                if q_data['processing'] is None:
                    return (q_data['slots'][0][0], 1), True 
                else:
                    return q_data['slots'][0], False 
            else:
                next_idx = idx - 1
                if q_data['occupants'][next_idx] is None:
                    return q_data['slots'][next_idx], False
                else:
                    return q_data['slots'][idx], False 
                    
        for i in range(len(q_data['slots'])-1, -1, -1):
            if q_data['occupants'][i] is None:
                last_slot_idx = len(q_data['slots']) - 1
                if q_data['occupants'][last_slot_idx] is None:
                    return q_data['slots'][last_slot_idx], False
                else:
                    return None, False 
                    
        return None, False

    def update_position(self, sid, agv_id, current_pos):
        q_data = self.station_queues.get(sid)
        if not q_data: return
        
        proc_pos = (q_data['slots'][0][0], 1)
        if current_pos == proc_pos:
            q_data['processing'] = agv_id
            if agv_id in q_data['occupants']:
                idx = q_data['occupants'].index(agv_id)
                q_data['occupants'][idx] = None
            return

        if current_pos in q_data['slots']:
            idx = q_data['slots'].index(current_pos)
            if agv_id in q_data['occupants']:
                old_idx = q_data['occupants'].index(agv_id)
                if old_idx != idx: q_data['occupants'][old_idx] = None
            q_data['occupants'][idx] = agv_id

    def release_station(self, sid, agv_id):
        q_data = self.station_queues.get(sid)
        if q_data and q_data['processing'] == agv_id:
            q_data['processing'] = None
            
    def get_exit_spot(self, sid):
        q_data = self.station_queues.get(sid)
        if q_data:
            return q_data['exits'][0] 
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

class LiveMonitor:
    def __init__(self):
        self.stats = {'Load':0, 'Visit':0, 'Return':0, 'Park':0}
        self.teleports = Counter()
        self.start_time = time.time()
    
    def log_success(self, category):
        self.stats[category] += 1
        
    def log_teleport(self, category, reason):
        self.teleports[f"{category}:{reason}"] += 1
        
    def print_status(self, done_count, total_tasks, agv_pool, cleaners):
        elapsed = time.time() - self.start_time
        active_agvs = sum(1 for s in agv_pool.values() if s['time'] > 0)
        trash_count = len(cleaners.pending_tasks)
        top_errors = self.teleports.most_common(3)
        err_str = " | ".join([f"{k}:{v}" for k,v in top_errors])
        print(f"\n[{elapsed:.0f}s] {done_count}/{total_tasks} | 🚗 Act:{active_agvs} Trash:{trash_count}")
        print(f"   📊 S:{self.stats['Load']}/{self.stats['Visit']}/{self.stats['Return']}/{self.stats['Park']}")
        print(f"   ⚠️ Err: {err_str}")

class AdvancedSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動進階模擬 (V63: The Omni-Fix)...")
        
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
        
        self.cleaner_2f = CleanupManager()
        self.cleaner_3f = CleanupManager()
        
        self.shuffler_2f = ShuffleManager(self.grid_2f, self.shelf_occupancy['2F'], self.pos_to_sid_2f, self.shelf_coords, self.cleaner_2f)
        self.shuffler_3f = ShuffleManager(self.grid_3f, self.shelf_occupancy['3F'], self.pos_to_sid_3f, self.shelf_coords, self.cleaner_3f)
        self.traffic_2f = TrafficController(self.grid_2f, self.agv_state['2F'], self.reservations_2f)
        self.traffic_3f = TrafficController(self.grid_3f, self.agv_state['3F'], self.reservations_3f)
        
        self.parking_2f = ParkingManager(self.grid_2f, self.valid_storage_spots['2F'], self.shelf_occupancy['2F'])
        self.parking_3f = ParkingManager(self.grid_3f, self.valid_storage_spots['3F'], self.shelf_occupancy['3F'])
        
        self.qm_2f = PhysicalQueueManager(st_2f)
        self.qm_3f = PhysicalQueueManager(st_3f)
        
        self.wave_totals = {}
        self.recv_totals = {}
        for o in self.all_tasks_raw:
            wid = str(o.get('WAVE_ID', 'UNKNOWN')) 
            d_str = o['datetime'].strftime('%Y-%m-%d')
            if 'RECEIVING' in wid: self.recv_totals[d_str] = self.recv_totals.get(d_str, 0) + 1
            else: self.wave_totals[wid] = self.wave_totals.get(wid, 0) + 1
            
        self.monitor = LiveMonitor()

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

    def _assign_locations_smartly(self, tasks):
        print("   -> Running Inventory Consolidation (Stickiness Mode)...")
        part_shelf_map = {}
        valid_shelves = list(self.shelf_coords.keys())
        for t in tasks:
            part = str(t.get('PARTNO', '')).strip()
            loc = str(t.get('LOC', '')).strip()
            if len(loc) >= 9:
                if part not in part_shelf_map:
                    part_shelf_map[part] = loc 
        for t in tasks:
            loc = str(t.get('LOC', '')).strip()
            if len(loc) >= 9: continue 
            part = str(t.get('PARTNO', '')).strip()
            if part in part_shelf_map:
                t['LOC'] = part_shelf_map[part]
            else:
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
        sample_spots = random.sample(list(valid_spots), min(limit*2, len(valid_spots)))
        for spot in sample_spots:
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
        if top_candidates: return [random.choice(top_candidates)]
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
            sid = f"2F_{i + 1}" 
            sts[sid] = {'floor': '2F', 'pos': pos, 'free_time': 0}
        cands_3f = find_stations(self.grid_3f)
        for i, pos in enumerate(cands_3f):
            sid = f"3F_{i + 1}"
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

    def _move_agv_segment(self, start_p, end_p, start_t, loaded, agv_name, floor, astar, shuffler, traffic_ctrl, w_evt, res_table, grid, is_returning=False, agv_pool=None, reason_label="GENERIC"):
        curr = start_p
        target = end_p
        t = start_t
        start_wait = t
        TIMEOUT_LIMIT = 60
        
        if not self._is_physically_connected(grid, curr, target):
             t += 120
             w_evt.writerow([self.to_dt(t-120), self.to_dt(t), floor, agv_name, curr[1], curr[0], target[1], target[0], 'AGV_MOVE', 'TELE_UNREACHABLE'])
             self.monitor.log_teleport(reason_label, 'Unreach')
             return target, t, True

        retry_count = 0 

        while curr != target:
            if t - start_wait > TIMEOUT_LIMIT:
                success, retreat_pos, retreat_time = traffic_ctrl.attempt_backtrack(curr, target, t, w_evt, floor, agv_name)
                if success:
                    curr = retreat_pos
                    t += retreat_time
                    start_wait = t 
                    continue
                
                t += 60 
                w_evt.writerow([self.to_dt(t-60), self.to_dt(t), floor, agv_name, curr[1], curr[0], target[1], target[0], 'AGV_MOVE', 'TELE_DEADLOCK'])
                self.monitor.log_teleport(reason_label, 'Stuck')
                return target, t, True

            path, _ = astar.find_path(curr, target, t, is_loaded=loaded, ignore_dynamic=False)
            
            if not path and is_returning and retry_count > 1:
                 new_candidates = self._find_smart_storage_spot(
                    curr, self.valid_storage_spots[floor], 
                    {s['pos'] for k,s in agv_pool.items()}, self.shelf_occupancy[floor], agv_pool, grid, limit=30
                 )
                 if new_candidates:
                    target = new_candidates[0]
                    retry_count = 0 
                    continue

            if not path and (t - start_wait > 3): 
                success, penalty = traffic_ctrl.clear_path_obstacles(curr, target, t, w_evt, floor, agv_name)
                if success:
                    t += int(penalty)
                    continue

            if not path and (t - start_wait > 5): 
                success, new_t, new_pos = shuffler.execute_shuffle_and_leave(
                    curr, target, w_evt, t, floor, agv_name, astar, res_table, self.write_move_events, self.base_time
                )
                if success:
                    t = new_t
                    curr = new_pos 
                    continue 
            
            if not path and (t - start_wait > 20):
                 path, _ = astar.find_path(curr, target, t, is_loaded=loaded, ignore_dynamic=False, allow_tunneling=True)
                 if path: t += 30 

            if not path and (t - start_wait > 45):
                 path, _ = astar.find_path(curr, target, t, is_loaded=loaded, ignore_dynamic=True, allow_tunneling=True)
                 if path: t += 30 
                 if not path:
                     t += 60
                     w_evt.writerow([self.to_dt(t-60), self.to_dt(t), floor, agv_name, curr[1], curr[0], target[1], target[0], 'AGV_MOVE', 'TELE_NO_PATH'])
                     self.monitor.log_teleport(reason_label, 'NoPath')
                     return target, t, True

            if path:
                self.write_move_events(w_evt, path, floor, agv_name.replace("AGV_", ""), res_table)
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
        
        w_evt = BatchWriter(os.path.join(LOG_DIR, 'simulation_events.csv'), ['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])
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
        
        print(f"🎬 開始模擬... (V63: The Omni-Fix)...")
        print(f"   -> 原始訂單: {len(self.all_tasks_raw)} | AGV任務: {total_tasks}")
        
        for floor in ['2F', '3F']:
            for sid, info in self.stations.items():
                if info['floor'] == floor:
                    display_id = sid.split('_')[1] 
                    w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"WS_{sid}", info['pos'][1], info['pos'][0], info['pos'][1], info['pos'][0], 'STATION_STATUS', f'WHITE|IDLE|Waiting'])
            for agv_id, state in self.agv_state[floor].items():
                pos = state['pos']
                w_evt.writerow([self.to_dt(0), self.to_dt(1), floor, f"AGV_{agv_id}", pos[1], pos[0], pos[1], pos[0], 'AGV_MOVE', 'INIT'])

        done_count = 0
        stats = {'Load': 0, 'Visit': 0, 'Return': 0, 'Park': 0}
        
        queues = {'2F': task_queue_2f, '3F': task_queue_3f}
        astars = {'2F': astar_2f, '3F': astar_3f}
        q_mgrs = {'2F': self.qm_2f, '3F': self.qm_3f}
        cleaners = {'2F': self.cleaner_2f, '3F': self.cleaner_3f}
        
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
            q_mgr = q_mgrs[floor]
            cleaner = cleaners[floor]
            
            while queue or cleaner.pending_tasks:
                best_agv = min(agv_pool, key=lambda k: agv_pool[k]['time'])
                agv_ready_time = agv_pool[best_agv]['time']
                agv_pos = agv_pool[best_agv]['pos']
                if grid[agv_pos[0]][agv_pos[1]] == -1: agv_pos = self._get_strict_spawn_spot(grid, set(), floor)
                current_t = agv_ready_time
                
                # Cleanup Priority
                cleanup_task = cleaner.get_nearest_task(agv_pos)
                if cleanup_task:
                    buf_pos, orig_pos, sid = cleanup_task
                    path, end_t = astar.find_path(agv_pos, buf_pos, current_t, False, ignore_dynamic=True)
                    if path:
                        self.write_move_events(w_evt, path, floor, f"{best_agv}", res_table)
                        w_evt.writerow([self.to_dt(end_t), self.to_dt(end_t+10), floor, f"AGV_{best_agv}", buf_pos[1], buf_pos[0], buf_pos[1], buf_pos[0], 'SHUFFLE_LOAD', f"Restore {sid}"])
                        if buf_pos in self.shelf_occupancy: self.shelf_occupancy.remove(buf_pos)
                        t2 = end_t + 10
                        
                        path2, end_t2 = astar.find_path(buf_pos, orig_pos, t2, True, ignore_dynamic=True, allow_tunneling=True)
                        if path2:
                            self.write_move_events(w_evt, path2, floor, f"{best_agv}", res_table)
                            w_evt.writerow([self.to_dt(end_t2), self.to_dt(end_t2+10), floor, f"AGV_{best_agv}", orig_pos[1], orig_pos[0], orig_pos[1], orig_pos[0], 'SHUFFLE_UNLOAD', f"Restored {sid}"])
                            self.shelf_occupancy[floor].add(orig_pos)
                            if sid != "Unknown" and sid in self.shelf_coords: self.shelf_coords[sid]['pos'] = orig_pos
                            if sid != "Unknown":
                                if floor == '2F': self.pos_to_sid_2f[orig_pos] = sid
                                else: self.pos_to_sid_3f[orig_pos] = sid
                            self.agv_state[floor][best_agv]['pos'] = orig_pos
                            self.agv_state[floor][best_agv]['time'] = end_t2 + 10
                            continue 
                
                if not queue: break
                
                # Peek Task & Station
                task = queue[0] 
                target_st = task['stops'][-1]['station']
                
                # [V63] Physical Queue Check
                target_pos, is_ready_to_work = q_mgr.get_target_for_agv(target_st, best_agv)
                
                if not target_pos:
                    self.agv_state[floor][best_agv]['time'] += 5
                    continue
                
                task = queue.popleft() # Take it
                
                shelf_id = task['shelf_id']
                if shelf_id not in self.shelf_coords: 
                    done_count += 1; continue
                    
                shelf_pos = self.shelf_coords[shelf_id]['pos']
                if grid[shelf_pos[0]][shelf_pos[1]] == -1: shelf_pos = self._get_strict_spawn_spot(grid, set(), floor)

                # 1. Load Shelf
                agv_pos, current_t, tele_1 = self._move_agv_segment(
                    agv_pos, shelf_pos, current_t, False, f"AGV_{best_agv}", 
                    floor, astar, shuffler, traffic, w_evt, res_table, grid, reason_label="LOAD"
                )
                if tele_1: stats['Load'] += 1
                self.monitor.log_success('Load')
                
                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0], 'SHELF_LOAD', f"Task_{done_count}"])
                current_t += 5
                if shelf_pos in self.shelf_occupancy[floor]: self.shelf_occupancy[floor].remove(shelf_pos)
                current_shelf_pos = shelf_pos
                
                # 2. Visit Station (with Queuing)
                # We need to reach 'Processing' state
                while True:
                    next_q_pos, is_processing = q_mgr.get_target_for_agv(target_st, best_agv)
                    
                    if not next_q_pos:
                        current_t += 5; continue
                        
                    # Move to assigned slot
                    current_shelf_pos, current_t, tele_2 = self._move_agv_segment(
                        current_shelf_pos, next_q_pos, current_t, True, f"AGV_{best_agv}",
                        floor, astar, shuffler, traffic, w_evt, res_table, grid, reason_label="QUEUE"
                    )
                    
                    # Update Manager
                    q_mgr.update_position(target_st, best_agv, next_q_pos)
                    
                    if is_processing:
                        break # Reached (r, 1), start work
                    else:
                        # Reached queue slot, wait a bit then check again
                        current_t += 5
                
                # Arrived at Station Processing
                if tele_2: stats['Visit'] += 1 # Count as visit
                self.monitor.log_success('Visit')
                
                stop_time = task['stops'][0]['time'] # Assume 1 stop
                leave_t = current_t + stop_time
                wid = task['wave_id']
                w_type = "IN" if "RECEIVING" in str(wid) else "OUT"
                w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"WS_{target_st}", current_shelf_pos[1], current_shelf_pos[0], current_shelf_pos[1], current_shelf_pos[0], 'STATION_STATUS', f'BLUE|{w_type}|{wid}'])
                w_evt.writerow([self.to_dt(current_t), self.to_dt(leave_t), floor, f"AGV_{best_agv}", current_shelf_pos[1], current_shelf_pos[0], current_shelf_pos[1], current_shelf_pos[0], 'PICKING', f"Processing"])
                for t in range(current_t, int(leave_t)): res_table[t].add(current_shelf_pos)
                current_t = int(leave_t)
                
                # Release Station & Exit Strategy
                q_mgr.release_station(target_st, best_agv)
                exit_pos = q_mgr.get_exit_spot(target_st)
                
                # Move to Exit Spot first (Side Exit)
                if exit_pos:
                    current_shelf_pos, current_t, _ = self._move_agv_segment(
                        current_shelf_pos, exit_pos, current_t, True, f"AGV_{best_agv}",
                        floor, astar, shuffler, traffic, w_evt, res_table, grid, reason_label="EXIT"
                    )
                
                # 3. Return
                candidates = self._find_smart_storage_spot(
                    current_shelf_pos, self.valid_storage_spots[floor], 
                    {s['pos'] for k,s in agv_pool.items()}, self.shelf_occupancy[floor], agv_pool, grid, limit=20
                )
                if not candidates: candidates = [shelf_pos]
                drop_pos = candidates[0]
                if grid[drop_pos[0]][drop_pos[1]] == -1: drop_pos = self._get_strict_spawn_spot(grid, set(), floor)

                current_shelf_pos, current_t, tele_3 = self._move_agv_segment(
                    current_shelf_pos, drop_pos, current_t, True, f"AGV_{best_agv}",
                    floor, astar, shuffler, traffic, w_evt, res_table, grid,
                    is_returning=True, agv_pool=agv_pool, reason_label="RETURN"
                )
                if tele_3: stats['Return'] += 1
                self.monitor.log_success('Return')

                w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+5), floor, f"AGV_{best_agv}", drop_pos[1], drop_pos[0], drop_pos[1], drop_pos[0], 'SHELF_UNLOAD', 'Done'])
                current_t += 5
                
                self.shelf_occupancy[floor].add(drop_pos)
                self.shelf_coords[shelf_id]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['pos'] = drop_pos
                self.agv_state[floor][best_agv]['time'] = current_t
                
                # 4. Park
                park_spot = parking.get_fast_parking_spot(agv_pool)
                if park_spot:
                    current_shelf_pos, current_t, tele_4 = self._move_agv_segment(
                        drop_pos, park_spot, current_t, False, f"AGV_{best_agv}",
                        floor, astar, shuffler, traffic, w_evt, res_table, grid, 
                        is_returning=False, agv_pool=agv_pool, reason_label="PARK_FINAL"
                    )
                    if not tele_4:
                        self.agv_state[floor][best_agv]['pos'] = park_spot
                        self.agv_state[floor][best_agv]['time'] = current_t
                        w_evt.writerow([self.to_dt(current_t), self.to_dt(current_t+1), floor, f"AGV_{best_agv}", park_spot[1], park_spot[0], park_spot[1], park_spot[0], 'PARKING', 'Hidden'])
                        stats['Park'] += 1
                        self.monitor.log_success('Park')
                else:
                    self.monitor.log_teleport('PARK', 'NoSpot')

                for raw_o in task['raw_orders']:
                     wid = raw_o.get('WAVE_ID', 'UNK')
                     ttype = 'INBOUND' if 'RECEIVING' in wid else 'OUTBOUND'
                     total_wave_count = self.wave_totals.get(wid, 0)
                     deadline_dt = self.to_dt(0) + timedelta(hours=4)
                     st_label = f"WS_{task['stops'][-1]['station']}" 
                     w_kpi.writerow([
                         self.to_dt(current_t), ttype, wid, 'N', 
                         self.to_dt(current_t).date(), st_label, 
                         total_wave_count, deadline_dt
                     ])

                done_count += 1
                if done_count % 50 == 0 or done_count == total_tasks: 
                     self._cleanup_reservations(res_table, current_t)
                     self.monitor.print_status(done_count, total_tasks, agv_pool, cleaner)

        w_evt.close()
        f_kpi.close()
        print(f"\n✅ 模擬完成！ Total Teleports: {sum(stats.values())}")

if __name__ == "__main__":
    AdvancedSimulationRunner().run()