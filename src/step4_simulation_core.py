import pandas as pd
import numpy as np
import os
import time
import heapq
import csv
import random
import pickle
from collections import defaultdict, deque, Counter
from datetime import datetime, timedelta

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
INPUT_FILE = os.path.join(BASE_DIR, 'processed_sim_data.pkl')
os.makedirs(LOG_DIR, exist_ok=True)

# ---------------- 核心演算法 ----------------

class TimeAwareAStar:
    # [修正 1] __init__ 補上 station_spots 參數
    def __init__(self, grid, reservations_dict, edge_reservations, shelf_occupancy_set, floor_name, station_spots):
        self.grid = grid
        self.rows, self.cols = grid.shape
        self.reservations = reservations_dict
        self.edge_reservations = edge_reservations
        self.shelf_occupancy = shelf_occupancy_set
        self.floor = floor_name
        self.station_spots = station_spots # [修正 2] 儲存下來
        self.moves = [(0, 1), (1, 0), (0, -1), (-1, 0), (0, 0)] 

    def heuristic(self, a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

    def find_path(self, start, goal, start_time, start_dir=4, is_loaded=False, ignore_dynamic=False):
        if not (0 <= start[0] < self.rows and 0 <= start[1] < self.cols): return None, None, None
        if not (0 <= goal[0] < self.rows and 0 <= goal[1] < self.cols): return None, None, None
        if self.grid[start[0]][start[1]] == -1 or self.grid[goal[0]][goal[1]] == -1: return None, None, None
        if start == goal: return [(start, start_time)], start_time, start_dir

        base_steps = 5000 
        max_steps = 10000 if ignore_dynamic else base_steps
        base_weight = 1.0 if ignore_dynamic else 1.5
        
        TURN_COST = 2.0; U_TURN_COST = 4.0; WAIT_COST = 1.0; TUNNEL_COST = 3.0   

        open_set = []
        heapq.heappush(open_set, (self.heuristic(start, goal), self.heuristic(start, goal), start_time, start, start_dir))
        g_score = {(start, start_time, start_dir): 0}
        came_from = {}
        steps_count = 0
        final_node = None

        while open_set:
            steps_count += 1
            if steps_count > max_steps: break
            
            f, h, current_time, current, current_dir = heapq.heappop(open_set)

            if current == goal:
                final_node = (current, current_time, current_dir)
                break
            
            if g_score.get((current, current_time, current_dir), float('inf')) < (f - h * base_weight): continue

            for i, (dr, dc) in enumerate(self.moves):
                nr, nc = current[0] + dr, current[1] + dc
                next_time = current_time + 1
                next_dir = i
                
                if not (0 <= nr < self.rows and 0 <= nc < self.cols): continue
                if self.grid[nr][nc] == -1: continue

                # [修正 3] 加入防止穿越工作站的邏輯
                # 如果下一步是工作站，且它既不是起點(離開工作站)也不是終點(進入工作站)，就視為牆壁
                if (nr, nc) in self.station_spots:
                    if (nr, nc) != goal and (nr, nc) != start:
                        continue
                # ------------------------------------------------

                if not ignore_dynamic:
                    if (next_time - start_time) < 60:
                        if next_time in self.reservations and (nr, nc) in self.reservations[next_time]: continue
                        if current_time in self.edge_reservations and ((nr, nc), current) in self.edge_reservations[current_time]: continue
                
                step_cost = 1.0
                is_shelf_spot = ((nr, nc) in self.shelf_occupancy)
                
                if is_loaded:
                    if is_shelf_spot and (nr, nc) != goal and (nr, nc) != start: continue
                else:
                    if is_shelf_spot: step_cost += TUNNEL_COST 

                if dr == 0 and dc == 0: step_cost += WAIT_COST; next_dir = current_dir 
                else:
                    if current_dir != 4 and next_dir != current_dir:
                        step_cost += (U_TURN_COST if abs(next_dir - current_dir) == 2 else TURN_COST)
                
                new_g = g_score.get((current, current_time, current_dir), float('inf')) + step_cost
                state_key = ((nr, nc), next_time, next_dir)
                if new_g < g_score.get(state_key, float('inf')):
                    g_score[state_key] = new_g
                    new_h = self.heuristic((nr, nc), goal)
                    heapq.heappush(open_set, (new_g + new_h * base_weight, new_h, next_time, (nr, nc), next_dir))
                    came_from[state_key] = (current, current_time, current_dir)

        if final_node:
            path = []
            curr = final_node
            while curr in came_from:
                pos, t, d = curr
                path.append((pos, t))
                curr = came_from[curr]
            path.append((start, start_time))
            path.reverse()
            return path, path[-1][1], final_node[2]
        return None, None, None

class ZoneManager:
    def __init__(self, stations_info, capacity=4):
        self.stats = {sid: {'en_route': 0, 'occupied': 0} for sid in stations_info}
        self.capacity = capacity

    def get_remaining_quota(self, sid):
        if sid not in self.stats: return 0
        current_load = self.stats[sid]['en_route'] + self.stats[sid]['occupied']
        return max(0, self.capacity - current_load)
    
    # [V7.3 新增] 嚴格檢查：取得當前總負載
    def get_total_load(self, sid):
        if sid not in self.stats: return 999
        return self.stats[sid]['en_route'] + self.stats[sid]['occupied']

    def reserve(self, sid):
        if sid in self.stats: self.stats[sid]['en_route'] += 1

    def enter(self, sid):
        if sid in self.stats:
            if self.stats[sid]['en_route'] > 0: self.stats[sid]['en_route'] -= 1
            self.stats[sid]['occupied'] += 1

    def exit(self, sid):
        if sid in self.stats and self.stats[sid]['occupied'] > 0:
            self.stats[sid]['occupied'] -= 1

class PhysicalQueueManager:
    def __init__(self, stations_info):
        self.station_queues = {} 
        for sid, info in stations_info.items():
            r, c = info['pos']
            direction = 1 if c < 30 else -1
            q_slots = []
            for i in range(1, 4): 
                slot_pos = (r, c + direction * i)
                q_slots.append(slot_pos)
            
            self.station_queues[sid] = {
                'station_pos': (r, c),
                'slots': q_slots, 
                'occupants': [None] * len(q_slots),
                'processing': None,
                'busy_until': 0,
                'slot_free_at': [0] * len(q_slots),
                'station_free_at': 0
            }
    
    def has_vacancy(self, sid):
        q = self.station_queues.get(sid)
        if not q: return False
        last_idx = len(q['slots']) - 1
        return q['occupants'][last_idx] is None
    
    # [V7.3] 取得物理格數
    def get_queue_capacity(self, sid):
        q = self.station_queues.get(sid)
        return len(q['slots']) if q else 0

    def allocate_slot(self, sid, agv_id, current_time):
        q = self.station_queues.get(sid)
        if not q: return None, 0, -1
        
        target_idx = -1
        for i in range(len(q['slots'])):
            if q['occupants'][i] is None:
                target_idx = i
                break
        
        if target_idx == -1: return None, 0, -1 
        
        q['occupants'][target_idx] = agv_id
        avail_time = max(current_time, q['slot_free_at'][target_idx])
        q['slot_free_at'][target_idx] = float('inf') 
        
        return q['slots'][target_idx], avail_time, target_idx

    def advance_slot(self, sid, agv_id, current_idx, current_time, move_duration=5):
        q = self.station_queues.get(sid)
        if not q: return None, 0, -1, False
        
        if current_idx == 0:
            st_ready_time = max(current_time, q['station_free_at'])
            leave_time = st_ready_time + move_duration
            q['slot_free_at'][0] = leave_time 
            q['occupants'][0] = None 
            q['processing'] = agv_id
            return q['station_pos'], st_ready_time, -1, True
            
        next_idx = current_idx - 1
        if next_idx >= 0:
            next_slot_ready = max(current_time, q['slot_free_at'][next_idx])
            q['occupants'][next_idx] = agv_id
            q['slot_free_at'][next_idx] = float('inf') 
            
            leave_time = next_slot_ready + move_duration
            q['slot_free_at'][current_idx] = leave_time
            q['occupants'][current_idx] = None
            
            return q['slots'][next_idx], next_slot_ready, next_idx, False
            
        return None, 0, -1, False 

    def process_finished(self, sid, agv_id, finish_time):
        q = self.station_queues.get(sid)
        if not q: return
        q['station_free_at'] = finish_time
        if q['processing'] == agv_id:
            q['processing'] = None

    def release_station(self, sid, agv_id):
        q = self.station_queues.get(sid)
        if q and q['processing'] == agv_id:
            q['processing'] = None

class BatchWriter:
    def __init__(self, filepath, header):
        self.f = open(filepath, 'w', newline='', encoding='utf-8')
        self.writer = csv.writer(self.f)
        self.writer.writerow(header)
    def writerow(self, row): self.writer.writerow(row)
    def close(self): self.f.close()

# ---------------- 主模擬器 ----------------

class SimulationRunner:
    def __init__(self):
        print(f"🚀 [Core] 啟動模擬核心 (V7.3: Strict Quota + Yield/Retry)...")
        self._load_data()
        self.reservations = {'2F': defaultdict(set), '3F': defaultdict(set)}
        self.edge_reservations = {'2F': defaultdict(set), '3F': defaultdict(set)}
        self.shelf_occupancy = {'2F': set(), '3F': set()}
        self.pos_to_sid = {'2F': {}, '3F': {}}
        self._init_shelves()
        self.agv_state = self._init_agvs()
        st_2f = {k:v for k,v in self.stations.items() if v['floor']=='2F'}
        st_3f = {k:v for k,v in self.stations.items() if v['floor']=='3F'}
        self.qm = {'2F': PhysicalQueueManager(st_2f), '3F': PhysicalQueueManager(st_3f)}
        self.zm = {'2F': ZoneManager(st_2f, capacity=4), '3F': ZoneManager(st_3f, capacity=4)}
        
        self.event_writer = BatchWriter(
            os.path.join(LOG_DIR, 'simulation_events.csv'), 
            ['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text']
        )
        self.kpi_writer = BatchWriter(
            os.path.join(LOG_DIR, 'simulation_kpi.csv'), 
            ['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation', 'total_in_wave', 'deadline_ts']
        )
        self.agv_kpi_writer = BatchWriter(
            os.path.join(LOG_DIR, 'agv_kpi.csv'), 
            ['time', 'floor', 'agv_id', 'status', 'battery']
        )

        self.wave_totals = Counter()
        for floor in ['2F', '3F']:
            for t in self.queues[floor]:
                wid = t.get('wave_id', 'UNK')
                self.wave_totals[wid] += 1
        
        self.rescue_locks = set()

    def _load_data(self):
        with open(INPUT_FILE, 'rb') as f: data = pickle.load(f)
        self.grid_2f = data['grid_2f']; self.grid_3f = data['grid_3f']
        self.stations = data['stations']; self.shelf_coords = data['shelf_coords']
        self.queues = {'2F': deque(data['queues']['2F']), '3F': deque(data['queues']['3F'])}
        self.base_time = data['base_time']
        self.valid_spots = {'2F': [], '3F': []}
        for r in range(32):
            for c in range(61):
                if self.grid_2f[r][c] == 1: self.valid_spots['2F'].append((r,c))
                if self.grid_3f[r][c] == 1: self.valid_spots['3F'].append((r,c))

    def _init_shelves(self):
        for sid, info in self.shelf_coords.items():
            f, p = info['floor'], info['pos']
            if f == '2F' and self.grid_2f[p[0]][p[1]] != -1: self.shelf_occupancy['2F'].add(p); self.pos_to_sid['2F'][p] = sid
            elif f == '3F' and self.grid_3f[p[0]][p[1]] != -1: self.shelf_occupancy['3F'].add(p); self.pos_to_sid['3F'][p] = sid

    def _init_agvs(self):
        states = {'2F': {}, '3F': {}}
        count_2f = 66; count_3f = 66
        spots_2f = random.sample(self.valid_spots['2F'], min(len(self.valid_spots['2F']), count_2f + 50))
        spots_3f = random.sample(self.valid_spots['3F'], min(len(self.valid_spots['3F']), count_3f + 50))
        
        free_spots_2f = [p for p in spots_2f if p not in self.shelf_occupancy['2F']]
        free_spots_3f = [p for p in spots_3f if p not in self.shelf_occupancy['3F']]
        if len(free_spots_2f) < count_2f: free_spots_2f = spots_2f
        if len(free_spots_3f) < count_3f: free_spots_3f = spots_3f

        for i in range(count_2f): states['2F'][i+1] = {'time': 0, 'pos': free_spots_2f[i], 'dir': 4, 'task': None}
        for i in range(count_3f): states['3F'][i+101] = {'time': 0, 'pos': free_spots_3f[i], 'dir': 4, 'task': None}
        return states

    def to_dt(self, sec): return self.base_time + timedelta(seconds=sec)

    def _lock_spot(self, floor, pos, start_t, duration):
        end_t = start_t + duration
        for t in range(int(start_t), int(end_t) + 1):
            self.reservations[floor][t].add(pos)

    def write_move(self, path, floor, agv_id, res_table, edge_res_table):
        if not path: return
        for i in range(len(path)-1):
            curr_pos, curr_t = path[i]; next_pos, next_t = path[i+1]
            res_table[next_t].add(next_pos); edge_res_table[curr_t].add((curr_pos, next_pos))
            self.event_writer.writerow([
                self.to_dt(curr_t), self.to_dt(next_t), 
                floor, f"AGV_{agv_id}", 
                curr_pos[1], curr_pos[0], next_pos[1], next_pos[0], 
                'AGV_MOVE', ''
            ])
        res_table[path[-1][1]].add(path[-1][0])

    def _find_smart_storage_spot(self, floor, start_pos, agv_pool, avoid_pos=None):
        grid = self.grid_2f if floor=='2F' else self.grid_3f
        candidates = []
        heatmap = Counter()
        for s in agv_pool.values():
            r, c = s['pos']
            for dr in range(-2, 3):
                 for dc in range(-2, 3):
                     heatmap[(r+dr, c+dc)] += 1
        samples = random.sample(self.valid_spots[floor], min(30, len(self.valid_spots[floor])))
        for spot in samples:
            if spot in self.shelf_occupancy[floor] or spot == avoid_pos or grid[spot[0]][spot[1]] == -1: continue
            dist = abs(spot[0]-start_pos[0]) + abs(spot[1]-start_pos[1])
            candidates.append((dist + heatmap[spot]*20, spot))
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1] if candidates else start_pos

    def _move_agv(self, floor, agv_id, target, loaded, astar):
        state = self.agv_state[floor][agv_id]
        curr = state['pos']; curr_t = state['time']; curr_dir = state['dir']
        path, end_t, end_dir = astar.find_path(curr, target, curr_t, curr_dir, is_loaded=loaded)
        if path:
            self.write_move(path, floor, agv_id, self.reservations[floor], self.edge_reservations[floor])
            state['pos'] = target; state['time'] = end_t; state['dir'] = end_dir
            return True, None
        else:
            if loaded:
                path_s, _, _ = astar.find_path(curr, target, curr_t, curr_dir, is_loaded=False, ignore_dynamic=True)
                if path_s:
                    for p, t in path_s:
                        if p in self.shelf_occupancy[floor] and p != curr and p != target: return False, {'type': 'BLOCKED', 'pos': p}
            
            self._lock_spot(floor, curr, curr_t, 10) 
            state['time'] += 5
            return False, {'type': 'WAIT'}

    def _find_closest_idle_agv(self, floor, target_pos, exclude_agv_id):
        agv_pool = self.agv_state[floor]
        best_agv = None
        min_dist = float('inf')
        
        for aid, s in agv_pool.items():
            if aid == exclude_agv_id: continue
            if s.get('task') is not None: continue 
            dist = abs(s['pos'][0]-target_pos[0]) + abs(s['pos'][1]-target_pos[1])
            if dist < min_dist:
                min_dist = dist
                best_agv = aid
        return best_agv

    def run(self):
        station_spots = set()
        for info in self.stations.values():
            if info['floor'] == '2F': station_spots.add(info['pos'])
            # 注意：這裡假設 station_spots 是給特定樓層用的，
            # 為了簡化，我們可以傳入整個 set，但在 A* 內判斷，
            # 或是分樓層 set。這裡建議直接分樓層：
        
        station_spots_2f = {info['pos'] for info in self.stations.values() if info['floor'] == '2F'}
        station_spots_3f = {info['pos'] for info in self.stations.values() if info['floor'] == '3F'}
        # --- [V7.4 修改結束] ---

        astars = {
            # --- [V7.4 修改] 傳入 station_spots ---
            '2F': TimeAwareAStar(self.grid_2f, self.reservations['2F'], self.edge_reservations['2F'], self.shelf_occupancy['2F'], '2F', station_spots_2f),
            '3F': TimeAwareAStar(self.grid_3f, self.reservations['3F'], self.edge_reservations['3F'], self.shelf_occupancy['3F'], '3F', station_spots_3f)
        }
        total_tasks = len(self.queues['2F']) + len(self.queues['3F'])
        done_cnt = 0
        task_retry_counter = defaultdict(int)

        for floor in ['2F', '3F']:
            print(f"🏁 Start {floor} Loop (V7.3)")
            agv_pool = self.agv_state[floor]
            astar = astars[floor]; qm = self.qm[floor]; zm = self.zm[floor]
            
            rescue_queue = deque()
            station_tasks = defaultdict(deque)
            raw_queue = self.queues[floor]
            while raw_queue:
                t = raw_queue.popleft()
                if t.get('type') == 'RESCUE': rescue_queue.append(t)
                else: station_tasks[t['stops'][0]['station']].append(t)
            
            active_stations = sorted(list(station_tasks.keys()))
            loop_idx = 0
            
            while rescue_queue or any(station_tasks.values()):
                loop_idx += 1
                if loop_idx % 2000 == 0: 
                    left = len(rescue_queue) + sum(len(q) for q in station_tasks.values())
                    print(f"🔄 Loop {loop_idx} | Left: {left} | Done: {done_cnt}")
                    time.sleep(0.01) 

                best_agv = min(agv_pool, key=lambda k: agv_pool[k]['time'])
                state = agv_pool[best_agv]
                
                selected_task = None
                source_queue = None
                
                if rescue_queue:
                    selected_task = rescue_queue[0]
                    source_queue = rescue_queue
                else:
                    candidate_list = []
                    for st in active_stations:
                        if not station_tasks[st]: continue
                        
                        # [V7.3 FIX] 嚴格預約制 (Fix Purple Army)
                        # 邏輯：(在途 + 佔用) < 實體排隊格數
                        current_load = zm.get_total_load(st)
                        max_slots = qm.get_queue_capacity(st)
                        if current_load >= max_slots: continue
                        
                        first_task = station_tasks[st][0]
                        
                        # [V7.3] 任務綁定檢查：如果任務已經綁定給其他 AGV，跳過
                        if first_task.get('assigned_agv') is not None:
                            if first_task['assigned_agv'] != best_agv:
                                continue

                        sid = first_task['shelf_id']
                        if sid in self.rescue_locks: continue

                        task_id = first_task.get('task_id', 'unk')
                        penalty = task_retry_counter[task_id] * 60
                        score = first_task['datetime'].timestamp() + penalty
                        candidate_list.append((score, st))
                    
                    if candidate_list:
                        candidate_list.sort(key=lambda x: x[0])
                        best_st = candidate_list[0][1]
                        selected_task = station_tasks[best_st][0]
                        source_queue = station_tasks[best_st]
                        
                        # 如果不是重試任務，才扣配額 (避免重複扣)
                        if not selected_task.get('_is_retry'):
                            zm.reserve(best_st)

                if selected_task is None: 
                    self._lock_spot(floor, state['pos'], state['time'], 10)
                    state['time'] += 5
                    continue

                task = source_queue.popleft()
                
                # --- RESCUE LOGIC ---
                if task.get('type') == 'RESCUE':
                    print(f"🚑 AGV_{best_agv} 執行移庫: {task['shelf_id']}")
                    target_shelf_pos = self.shelf_coords[task['shelf_id']]['pos']
                    
                    ok, err = self._move_agv(floor, best_agv, target_shelf_pos, False, astar)
                    if not ok:
                        state['time'] += 5; rescue_queue.append(task); continue
                    
                    self.shelf_occupancy[floor].remove(target_shelf_pos)
                    self.event_writer.writerow([
                        self.to_dt(state['time']), self.to_dt(state['time']+1),
                        floor, f"AGV_{best_agv}", target_shelf_pos[1], target_shelf_pos[0], target_shelf_pos[1], target_shelf_pos[0],
                        'SHUFFLE_LOAD', f"{task['shelf_id']}"
                    ])
                    
                    safe_spot = self._find_smart_storage_spot(floor, target_shelf_pos, agv_pool, avoid_pos=target_shelf_pos)
                    ok_buf, _ = self._move_agv(floor, best_agv, safe_spot, True, astar)
                    
                    if not ok_buf:
                        state['pos'] = safe_spot; state['time'] += 30
                        # 救援車如果也被擋，這裡還是允許瞬移，因為這是「最後手段」
                        self.event_writer.writerow([
                            self.to_dt(state['time']-30), self.to_dt(state['time']),
                            floor, f"AGV_{best_agv}", target_shelf_pos[1], target_shelf_pos[0], safe_spot[1], safe_spot[0],
                            'FORCE_TELE', 'RescueMoveBlocked'
                        ])
                    
                    self.shelf_occupancy[floor].add(safe_spot)
                    self.event_writer.writerow([
                        self.to_dt(state['time']), self.to_dt(state['time']+1),
                        floor, f"AGV_{best_agv}", safe_spot[1], safe_spot[0], safe_spot[1], safe_spot[0],
                        'SHUFFLE_UNLOAD', f"{task['shelf_id']}"
                    ])
                    
                    self.shelf_coords[task['shelf_id']]['pos'] = safe_spot
                    self.pos_to_sid[floor][safe_spot] = task['shelf_id']
                    self.rescue_locks.clear() 
                    continue

                # --- NORMAL TASK LOGIC ---
                target_st = task['stops'][0]['station']
                shelf_id = task['shelf_id']
                if shelf_id not in self.shelf_coords: 
                    zm.stats[target_st]['en_route'] -= 1; continue
                
                shelf_pos = self.shelf_coords[shelf_id]['pos']
                
                # [V7.3] 狀態檢查：如果已經載貨了 (SKIP_PICKUP)，直接跳過取貨階段
                if not task.get('_skip_pickup'):
                    success, err = self._move_agv(floor, best_agv, shelf_pos, False, astar)
                    if not success:
                        zm.stats[target_st]['en_route'] -= 1 # 失敗退票
                        if err['type'] == 'BLOCKED':
                            blocker_pos = err['pos'] 
                            blocker_sid = self.pos_to_sid[floor].get(blocker_pos)
                            if blocker_sid:
                                print(f"🚨 AGV_{best_agv} 取貨被擋，呼叫救援 {blocker_sid}")
                                rescue_task = {'type': 'RESCUE', 'shelf_id': blocker_sid}
                                rescue_queue.appendleft(rescue_task)
                                self.rescue_locks.add(shelf_id)
                        
                        task_retry_counter[task.get('task_id')] += 1
                        state['time'] += 5
                        # 重試：放回佇列頭
                        station_tasks[target_st].appendleft(task)
                        continue

                    # 成功抵達料架
                    self.shelf_occupancy[floor].remove(shelf_pos)
                    self.event_writer.writerow([
                        self.to_dt(state['time']), self.to_dt(state['time']+1),
                        floor, f"AGV_{best_agv}", shelf_pos[1], shelf_pos[0], shelf_pos[1], shelf_pos[0],
                        'SHELF_LOAD', f"{shelf_id}"
                    ])
                else:
                    # 已經載貨了，不需要再移動去料架，也不需要 remove occupancy (上次已做)
                    pass

                # --- MOVE TO QUEUE ---
                zm.enter(target_st)
                q_pos, avail_time, current_idx = qm.allocate_slot(target_st, best_agv, state['time'])
                
                if q_pos is None:
                    # Race condition safety
                    state['time'] += 5
                    q_pos = qm.station_queues[target_st]['station_pos']
                
                if avail_time > state['time']: state['time'] = avail_time
                
                ok_q, err_q = self._move_agv(floor, best_agv, q_pos, True, astar)
                
                # [V7.3 FIX] 載貨被擋 -> Yield & Retry (Fix Teleport)
                if not ok_q:
                    if err_q and err_q['type'] == 'BLOCKED':
                         print(f"🚨 AGV_{best_agv} (載貨中) 被擋住！暫停並呼叫救援...")
                         blocker_pos = err_q['pos']
                         blocker_sid = self.pos_to_sid[floor].get(blocker_pos)
                         
                         if blocker_sid:
                             rescue_task = {'type': 'RESCUE', 'shelf_id': blocker_sid}
                             rescue_queue.appendleft(rescue_task)
                         
                         # 重要：不瞬移，而是「中斷並重試」
                         # 1. 標記任務狀態為「已載貨」，下次跳過 pickup
                         task['_skip_pickup'] = True
                         task['_is_retry'] = True
                         task['assigned_agv'] = best_agv # 綁定這台車
                         
                         # 2. 退票 (因為 enter 了，要退出來變成 en_route 狀態等待下一次嘗試)
                         # 這裡邏輯有點複雜，我們簡單處理：視為還在 en_route
                         # zm.enter 已經把 en_route-1, occupied+1
                         # 我們要退回 en_route+1, occupied-1
                         zm.stats[target_st]['occupied'] -= 1
                         zm.stats[target_st]['en_route'] += 1
                         
                         # 3. 釋放剛剛佔用的 Queue Slot (allocate_slot)
                         qm.station_queues[target_st]['occupants'][current_idx] = None
                         qm.station_queues[target_st]['slot_free_at'][current_idx] = 0

                         # 4. 放回任務佇列最前端
                         station_tasks[target_st].appendleft(task)
                         
                         # 5. AGV 原地等待
                         state['time'] += 5
                         
                         # 6. 中斷本次執行，讓出 CPU 給救援車
                         continue 
                    else:
                        # 真的無路可走 (Deadlock or Static block)，這時候才瞬移
                        state['pos'] = q_pos; state['time'] += 20
                        self.event_writer.writerow([
                            self.to_dt(state['time']-20), self.to_dt(state['time']),
                            floor, f"AGV_{best_agv}", state['pos'][1], state['pos'][0], q_pos[1], q_pos[0],
                            'FORCE_TELE', 'QueueEntryBlocked'
                        ])

                # --- PROCESS AT STATION ---
                in_processing = False
                while not in_processing:
                    next_pos, start_t, next_idx, is_proc = qm.advance_slot(target_st, best_agv, current_idx, state['time'])
                    
                    if start_t > state['time']:
                        self._lock_spot(floor, state['pos'], state['time'], int(start_t - state['time']))
                        state['time'] = start_t
                    
                    move_time = 5 
                    self._lock_spot(floor, state['pos'], state['time'], move_time) 
                    state['time'] += move_time
                    state['pos'] = next_pos
                    
                    current_idx = next_idx
                    in_processing = is_proc
                
                proc_time = task['stops'][0]['time']
                self._lock_spot(floor, state['pos'], state['time'], proc_time)
                state['time'] += proc_time
                
                qm.process_finished(target_st, best_agv, state['time'])
                
                wave_id = task.get('wave_id', 'UNK')
                ttype = 'INBOUND' if 'RECEIVING' in wave_id else 'OUTBOUND'
                deadline_dt = self.to_dt(0) + timedelta(hours=4)
                self.kpi_writer.writerow([
                    self.to_dt(state['time']), ttype, wave_id, 'N', 
                    self.to_dt(state['time']).date(), target_st, 
                    self.wave_totals[wave_id], int(deadline_dt.timestamp())
                ])
                
                qm.release_station(target_st, best_agv)
                zm.exit(target_st)

                drop_pos = self._find_smart_storage_spot(floor, shelf_pos, agv_pool)
                self._move_agv(floor, best_agv, drop_pos, True, astar)
                
                self.shelf_occupancy[floor].add(drop_pos)
                self.event_writer.writerow([
                    self.to_dt(state['time']), self.to_dt(state['time']+1),
                    floor, f"AGV_{best_agv}", drop_pos[1], drop_pos[0], drop_pos[1], drop_pos[0],
                    'SHELF_UNLOAD', f"{shelf_id}"
                ])

                self.shelf_coords[shelf_id]['pos'] = drop_pos
                
                done_cnt += 1
                if done_cnt % 10 == 0: print(f"✅ Done {done_cnt}/{total_tasks}")

        self.event_writer.close()
        self.kpi_writer.close()
        self.agv_kpi_writer.close()
        print("🎉 模擬結束")

if __name__ == "__main__":
    SimulationRunner().run()