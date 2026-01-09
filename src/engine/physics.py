import numpy as np
import heapq
import pandas as pd
import os
import random

# A* 演算法 (增加 blocked_cells 參數以支援動態避障)
def heuristic(a, b):
    return abs(a[0] - b[0]) + abs(a[1] - b[1])

def a_star_search(grid, start, goal, blocked_cells=None):
    rows, cols = grid.shape
    open_set = []
    heapq.heappush(open_set, (0, start))
    came_from = {}
    g_score = {start: 0}
    f_score = {start: heuristic(start, goal)}
    
    if blocked_cells is None: blocked_cells = set()

    steps = 0
    max_steps = 3000 # 限制搜尋深度以保證效能

    while open_set:
        steps += 1
        if steps > max_steps: return None 

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
                # 靜態障礙 (1=Shelf, 2=Station)
                if grid[neighbor[0]][neighbor[1]] in [1, 2] and neighbor != goal: continue
                # 動態障礙 (其他 AGV)
                if neighbor in blocked_cells: continue
                
                tentative_g = g_score[current] + 1
                if neighbor not in g_score or tentative_g < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g
                    f_score[neighbor] = tentative_g + heuristic(neighbor, goal)
                    heapq.heappush(open_set, (f_score[neighbor], neighbor))
    return None

class AGV:
    def __init__(self, agv_id, floor, start_pos):
        self.id = agv_id
        self.floor = floor
        self.pos = start_pos
        self.status = 'IDLE'
        self.current_task = None
        self.path = []
        
        # [核心邏輯 3] 避障機制
        self.wait_counter = 0
        self.max_wait_patience = 5  # 等待超過 5 ticks 就重算路徑
        self.target_pos_cache = None # 記住目標以便重算

    def assign_task(self, grid_obj, target_pos, task_data):
        # 初始規劃 (只看靜態地圖)
        path = a_star_search(grid_obj.grid, self.pos, target_pos)
        if path:
            self.path = path
            self.target_pos_cache = target_pos
            self.current_task = task_data
            self.status = 'BUSY'
            self.wait_counter = 0
            return True
        return False

    def move_step(self, grid_obj, all_agv_positions):
        """
        回傳: (status_code, position/result)
        status_code: 'MOVED', 'WAIT', 'BLOCKED', 'DONE'
        """
        if not self.path:
            return 'DONE', self.pos

        next_pos = self.path[0]
        
        # 檢查是否有人擋路
        if next_pos in all_agv_positions:
            self.wait_counter += 1
            
            # 策略 A: 短暫等待
            if self.wait_counter <= self.max_wait_patience:
                return 'WAIT', self.pos
            
            # 策略 B: 等太久了，嘗試重算路徑 (Re-path)
            else:
                # 將當前擋路的所有 AGV 位置視為障礙物
                new_path = a_star_search(grid_obj.grid, self.pos, self.target_pos_cache, blocked_cells=all_agv_positions)
                if new_path:
                    self.path = new_path
                    self.wait_counter = 0
                    return 'WAIT', self.pos # 這回合先不動，下回合走新路
                else:
                    # 無路可走，繼續死等 (或報錯)
                    return 'BLOCKED', self.pos

        # 無人擋路 -> 移動
        self.pos = self.path.pop(0)
        self.wait_counter = 0
        
        if not self.path:
            return 'DONE', self.pos
            
        return 'MOVED', self.pos

class MapWorld:
    # 保持原樣，負責讀取地圖
    def __init__(self, floor_name, map_file, base_dir):
        self.floor = floor_name
        self.grid = self._load_map(map_file, base_dir)
    
    def _load_map(self, filename, base_dir):
        path = os.path.join(base_dir, 'data', 'master', filename)
        try: return pd.read_excel(path, header=None).fillna(0).values
        except: return np.zeros((30,60))