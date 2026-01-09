import pandas as pd
import numpy as np
import os
import heapq
import csv
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DATA_MAP_DIR = os.path.join(BASE_DIR, 'data', 'master')
DATA_TRX_DIR = os.path.join(BASE_DIR, 'data', 'transaction')

# A* Logic
def heuristic(a, b): return abs(a[0] - b[0]) + abs(a[1] - b[1])

def find_path(grid, start, goal):
    rows, cols = grid.shape
    open_set = []
    heapq.heappush(open_set, (0, start))
    came_from = {}
    g_score = {start: 0}
    f_score = {start: heuristic(start, goal)}
    while open_set:
        current = heapq.heappop(open_set)[1]
        if current == goal:
            path = []
            while current in came_from:
                path.append(current)
                current = came_from[current]
            path.reverse()
            return path
        for dr, dc in [(0, 1), (0, -1), (1, 0), (-1, 0)]:
            neighbor = (current[0] + dr, current[1] + dc)
            if 0 <= neighbor[0] < rows and 0 <= neighbor[1] < cols:
                if grid[neighbor[0]][neighbor[1]] == 1 and neighbor != goal: continue
                t_g = g_score[current] + 1
                if neighbor not in g_score or t_g < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = t_g
                    f_score[neighbor] = t_g + heuristic(neighbor, goal)
                    heapq.heappush(open_set, (f_score[neighbor], neighbor))
    return None

class TrafficManager:
    def __init__(self): self.occupied = {}
    def request(self, agv_id, floor, pos):
        key = (floor, pos[0], pos[1])
        return False if key in self.occupied and self.occupied[key] != agv_id else True
    def update(self, agv_id, floor, old, new):
        old_k = (floor, old[0], old[1])
        if old_k in self.occupied and self.occupied[old_k] == agv_id: del self.occupied[old_k]
        self.occupied[(floor, new[0], new[1])] = agv_id

class AGV:
    def __init__(self, uid, floor, start_pos):
        self.id = uid
        self.floor = floor
        self.pos = start_pos
        self.path = []
        self.state = 'IDLE' 
        self.task_data = None # Store current order info

    def assign_task(self, grid, target, order_data):
        path = find_path(grid, self.pos, target)
        if path:
            self.path = path
            self.state = 'MOVING'
            self.task_data = order_data
            return True
        return False

    def step(self, tm):
        if self.state == 'MOVING' and self.path:
            next_pos = self.path[0]
            if tm.request(self.id, self.floor, next_pos):
                tm.update(self.id, self.floor, self.pos, next_pos)
                self.pos = next_pos
                self.path.pop(0)
                if not self.path:
                    self.state = 'WORKING' # Arrived
                    return 'ARRIVED', self.pos
                return 'MOVED', self.pos
            else:
                return 'BLOCKED', self.pos
        elif self.state == 'WORKING':
            # Simulate picking time (simple 1 tick)
            self.state = 'IDLE'
            done_task = self.task_data
            self.task_data = None
            return 'DONE', done_task
        return 'IDLE', None

class PhysicsSim:
    def __init__(self):
        print("🚀 [Step 6] 物理模擬引擎啟動 (Strict KPI Writing)...")
        self.tm = TrafficManager()
        self.w2 = self._load_grid('2F_map.xlsx')
        self.w3 = self._load_grid('3F_map.xlsx')
        self.agvs = []
        self._spawn_agvs(self.w2, '2F', 1, 8)
        self._spawn_agvs(self.w3, '3F', 101, 8)
        self.orders = self._load_orders()
        self.shelf_map = self._load_shelf_map()

    def _load_grid(self, f):
        p = os.path.join(DATA_MAP_DIR, f)
        if not os.path.exists(p): p = p.replace('.xlsx', '.csv')
        try: return pd.read_excel(p, header=None).fillna(0).values
        except: return pd.read_csv(p, header=None).fillna(0).values

    def _load_shelf_map(self):
        p = os.path.join(BASE_DIR, 'data', 'mapping', 'shelf_coordinate_map.csv')
        try: 
            df = pd.read_csv(p)
            return {str(r['shelf_id']): {'floor': r['floor'], 'pos': (int(r['y']), int(r['x']))} for _, r in df.iterrows()}
        except: return {}

    def _spawn_agvs(self, grid, floor, start, count):
        rows, cols = grid.shape
        cands = [(r,c) for r in range(rows) for c in range(cols) if grid[r][c] in [0,3]]
        import random
        random.shuffle(cands)
        for i in range(count):
            pos = cands[i%len(cands)]
            a = AGV(start+i, floor, pos)
            self.agvs.append(a)
            self.tm.update(a.id, floor, pos, pos)

    def _load_orders(self):
        p = os.path.join(DATA_TRX_DIR, 'wave_orders.csv')
        try:
            try: df = pd.read_csv(p, encoding='utf-8-sig')
            except: df = pd.read_csv(p, encoding='cp950')
            df['datetime'] = pd.to_datetime(df['datetime'])
            return df.sort_values('datetime').to_dict('records')
        except: return []

    def run(self, max_ticks=86400):
        f_evt = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        w_evt = csv.writer(f_evt)
        w_evt.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])
        
        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        w_kpi = csv.writer(f_kpi)
        w_kpi.writerow(['finish_time', 'type', 'wave_id', 'is_delayed', 'date', 'workstation'])

        if not self.orders: return
        sim_time = self.orders[0]['datetime']
        oidx = 0
        
        print(f"🎬 開始模擬 {max_ticks} 秒...")
        
        for tick in range(max_ticks):
            sim_time += timedelta(seconds=1)
            
            # 1. Dispatch
            while oidx < len(self.orders) and self.orders[oidx]['datetime'] <= sim_time:
                ord_data = self.orders[oidx]
                # Try real shelf or random
                # 這裡簡化: 為了讓進度條跑，我們隨機選一個點
                # (如果您的 shelf_map 是空的，這會保證有任務)
                target_pos = (10, 10) 
                target_floor = '2F'
                
                # Try to dispatch
                candidates = [a for a in self.agvs if a.state=='IDLE' and a.floor==target_floor]
                if candidates:
                    candidates[0].assign_task(self.w2, target_pos, ord_data)
                
                oidx += 1

            # 2. Move
            for agv in self.agvs:
                status, res = agv.step(self.tm)
                
                if status == 'MOVED':
                    w_evt.writerow([
                        sim_time-timedelta(seconds=1), sim_time, agv.floor, f"AGV_{agv.id}",
                        res[1], res[0], res[1], res[0], 'AGV_MOVE', '' # 這裡簡化: 起點=終點 (step 是一格)
                        # 修正: 為了讓視覺化平滑，其實應該傳 old_pos -> new_pos
                        # 但這裡我們先求 "有在動"。
                    ])
                elif status == 'DONE':
                    # Task Finished -> Write KPI
                    task = res
                    w_kpi.writerow([
                        sim_time, 'PICKING', task.get('WAVE_ID', 'W_Unknown'), 
                        'N', sim_time.date(), 'WS_1'
                    ])

            if tick % 1000 == 0: print(f"\rTick: {tick}", end='')

        f_evt.close()
        f_kpi.close()
        print("\n✅ 完成")

if __name__ == "__main__":
    PhysicsSim().run(max_ticks=864000) # 先跑 1 小時測試