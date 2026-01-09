import numpy as np
import heapq
import pandas as pd
import os
import random

# ==========================================
# 1. 尋路演算法 (A*)
# ==========================================
def heuristic(a, b):
    return abs(a[0] - b[0]) + abs(a[1] - b[1])

def a_star_search(grid, start, goal, ignore_start_obstacle=False):
    """
    [優化版 A*] 
    注意：這裡只考慮靜態地圖障礙 (Static Map)，不考慮動態 AGV。
    動態避障移到移動時判斷 (Wait logic)。
    """
    rows, cols = grid.shape
    open_set = []
    heapq.heappush(open_set, (0, start))
    
    came_from = {}
    g_score = {start: 0}
    f_score = {start: heuristic(start, goal)}
    
    # 限制搜索深度，避免在無解路徑卡死太久
    max_steps = 5000 
    steps_count = 0

    while open_set:
        steps_count += 1
        if steps_count > max_steps:
            return None 

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
                # 靜態障礙檢查
                cell_value = grid[neighbor[0]][neighbor[1]]
                # 假設 1=料架, 2=工作站 是牆壁 (3=充電站可走)
                is_wall = (cell_value in [1, 2])
                
                if is_wall:
                    continue
                
                tentative_g_score = g_score[current] + 1
                if neighbor not in g_score or tentative_g_score < g_score[neighbor]:
                    came_from[neighbor] = current
                    g_score[neighbor] = tentative_g_score
                    f_score[neighbor] = tentative_g_score + heuristic(neighbor, goal)
                    heapq.heappush(open_set, (f_score[neighbor], neighbor))
    
    return None

# ==========================================
# 2. 地圖管理器 (保持不變)
# ==========================================
class MapWorld:
    def __init__(self, floor_name, map_file, base_dir):
        self.floor = floor_name
        self.grid = self._load_map(map_file, base_dir)
        self.rows, self.cols = self.grid.shape
        self.charging_stations = self._find_poi(3)
        self.workstations = self._find_poi(2)
        print(f"   [{self.floor}] 地圖: {self.rows}x{self.cols}")

    def _load_map(self, filename, base_dir):
        path = os.path.join(base_dir, 'data', 'master', filename)
        if not os.path.exists(path):
            return np.zeros((10, 10), dtype=int)
        df = pd.read_excel(path, header=None).fillna(0)
        return df.to_numpy(dtype=int)

    def _find_poi(self, type_id):
        coords = []
        rows, cols = self.grid.shape
        for r in range(rows):
            for c in range(cols):
                if self.grid[r][c] == type_id:
                    coords.append((r, c))
        return coords

# ==========================================
# 3. AGV 代理人 (核心修改：路徑快取)
# ==========================================
class AGV:
    def __init__(self, agv_id, floor, start_pos, is_under_shelf=False):
        self.id = agv_id
        self.floor = floor
        self.pos = start_pos
        self.battery = 100.0
        self.status = 'IDLE'
        self.current_task = None
        
        # [優化] 路徑快取
        self.cached_path = []  # 存儲規劃好的一連串座標
        self.wait_counter = 0  # 紀錄被擋住多久了
        self.is_emerging = is_under_shelf 

    def plan_path(self, grid_obj, target_pos):
        """
        只在接任務時呼叫一次。計算靜態 A* 路徑並存起來。
        """
        # 如果正在鑽出料架，允許無視起點障礙
        path = a_star_search(grid_obj.grid, self.pos, target_pos, ignore_start_obstacle=self.is_emerging)
        
        if path:
            self.cached_path = path
            self.wait_counter = 0
            return True
        return False

    def move_step(self, grid_obj, occupied_positions):
        """
        每秒呼叫一次。
        不重算路徑，只檢查下一步能不能走。
        """
        if not self.cached_path:
            return False, self.pos

        # 1. 偷看下一步 (Peek)
        next_step = self.cached_path[0]
        
        # 2. 檢查是否有別的 AGV 擋路 (Dynamic Check)
        if next_step in occupied_positions:
            # 被擋住了！
            self.wait_counter += 1
            
            # [進階策略] 如果等太久 (例如 5秒)，嘗試重算路徑 (Re-path)
            # 但為了效能，這裡暫時選擇「死等」，因為倉庫路通常很窄，繞路也沒用
            return False, self.pos # 留在原地

        # 3. 沒人擋路 -> 移動
        self.pos = self.cached_path.pop(0) # 真的取出並移動
        self.wait_counter = 0
        
        # 4. 解除 Emerging 狀態
        if self.is_emerging:
            val = grid_obj.grid[self.pos[0]][self.pos[1]]
            if val == 0 or val == 3:
                self.is_emerging = False

        return True, self.pos