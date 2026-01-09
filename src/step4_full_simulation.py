import pandas as pd
import numpy as np
import os
import random
import time
from datetime import timedelta

from engine.configs import SimConfig
from engine.physics import MapWorld, AGV
from logic.dispatcher import TaskDispatcher

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class FullSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動模擬 (Debug Mode)...")
        self.cfg = SimConfig(BASE_DIR)
        
        # 1. 初始化地圖
        self.world_2f = MapWorld('2F', '2F_map.xlsx', BASE_DIR)
        self.world_3f = MapWorld('3F', '3F_map.xlsx', BASE_DIR)
        
        # [DEBUG] 顯示地圖載入資訊
        print(f"🗺️ 地圖檢查:")
        print(f"   -> 2F Grid Shape: {self.world_2f.grid.shape}")
        print(f"   -> 3F Grid Shape: {self.world_3f.grid.shape}")
        
        # 2. 初始化 AGV (簡化生成邏輯: 隨機找空地)
        self.agvs_2f = self._spawn_agvs(self.world_2f, 8, 1)
        self.agvs_3f = self._spawn_agvs(self.world_3f, 8, 101)
        
        # 3. 載入資料
        self.inventory_map = self._load_inventory()
        self.shelf_coords = self._load_shelf_coords()
        self.all_tasks = self._load_orders()
        self.dispatcher = TaskDispatcher(self.cfg)
        
        # 4. 波次規劃
        all_stations = list(range(1, 9)) + list(range(101, 109))
        if self.all_tasks:
            start_time = self.all_tasks[0]['datetime']
            deadline = self.all_tasks[-1]['WAVE_DEADLINE']
            if pd.isna(deadline): deadline = start_time + timedelta(hours=4)
            self.dispatcher.plan_wave_assignments(self.all_tasks, all_stations, start_time, deadline)

        self.agv_unlock_times = {}

    def _spawn_agvs(self, world, count, start_id):
        """確保 AGV 生成在合法空地上"""
        agvs = []
        rows, cols = world.grid.shape
        candidates = []
        for r in range(rows):
            for c in range(cols):
                if world.grid[r][c] in [0, 3]: # 0=空地, 3=充電站
                    candidates.append((r, c))
        
        if not candidates:
            print(f"⚠️ 警告: {world.floor} 地圖完全沒有空地！AGV 將生在 (0,0)")
            candidates = [(0,0)]
            
        random.shuffle(candidates)
        for i in range(count):
            pos = candidates[i % len(candidates)]
            agvs.append(AGV(start_id + i, world.floor, pos))
        return agvs

    def _load_shelf_coords(self):
        path = os.path.join(BASE_DIR, 'data', 'mapping', 'shelf_coordinate_map.csv')
        coords = {}
        try:
            df = pd.read_csv(path)
            print(f"📖 載入座標表: {len(df)} 筆")
            for _, r in df.iterrows():
                coords[str(r['shelf_id'])] = {'floor': r['floor'], 'pos': (int(r['x']), int(r['y']))}
        except: print("⚠️ 警告: 找不到 shelf_coordinate_map.csv")
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
                    p = str(r[part_col]).strip()
                    c = str(r[cell_col]).strip()[:7] 
                    if p not in inv: inv[p] = []
                    inv[p].append(c)
        except: pass
        return inv

    def _load_orders(self):
        path = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        try: 
            df = pd.read_csv(path).sort_values('datetime')
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['WAVE_DEADLINE'] = pd.to_datetime(df['WAVE_DEADLINE'])
            return df.to_dict('records')
        except: return []

    def get_target_shelf(self, order):
        part_no = str(order.get('PARTNO', '')).strip()
        candidate_shelves = self.inventory_map.get(part_no, [])
        target_info = None
        for sid in candidate_shelves:
            if sid in self.shelf_coords:
                target_info = self.shelf_coords[sid]
                break 
        
        if not target_info and self.shelf_coords:
            rand_sid = random.choice(list(self.shelf_coords.keys()))
            target_info = self.shelf_coords[rand_sid]
            
        if not target_info:
            target_info = {'floor': '2F', 'pos': (10, 10)}
        return target_info

    def run(self):
        if not self.all_tasks: return
        
        sim_time = self.all_tasks[0]['datetime']
        task_idx = 0
        completed = 0
        
        while task_idx < len(self.all_tasks) or self.agv_unlock_times:
            # 1. 釋放完成的 AGV
            finished_agvs = [aid for aid, t in self.agv_unlock_times.items() if t <= sim_time]
            for aid in finished_agvs:
                del self.agv_unlock_times[aid]
                completed += 1

            # 2. 分派任務
            while task_idx < len(self.all_tasks) and self.all_tasks[task_idx]['datetime'] <= sim_time:
                order = self.all_tasks[task_idx]
                st_id = self.dispatcher.get_assigned_station(order)
                if not st_id: st_id = 1 
                
                if not self.dispatcher.check_station_availability(st_id, sim_time):
                    break 
                
                target_info = self.get_target_shelf(order)
                
                floor_agvs = self.agvs_2f if target_info['floor'] == '2F' else self.agvs_3f
                idle_agvs = [a for a in floor_agvs if a.id not in self.agv_unlock_times]
                
                if idle_agvs:
                    agv = idle_agvs[0]
                    target_grid = self.world_2f.grid if agv.floor=='2F' else self.world_3f.grid
                    
                    path_found = agv.assign_task(
                        self.world_2f if agv.floor=='2F' else self.world_3f, 
                        target_info['pos'], order
                    )
                    
                    if path_found:
                        travel_time = len(agv.path) * 1.5 
                        pick_time = 20
                        total_sec = travel_time * 2 + pick_time 
                        finish_time = sim_time + timedelta(seconds=total_sec)
                        self.agv_unlock_times[agv.id] = finish_time
                        self.dispatcher.occupy_station(st_id, total_sec, sim_time)
                        task_idx += 1
                    else:
                        # [DEBUG 重點區] 輸出詳細錯誤資訊
                        rows, cols = target_grid.shape
                        tx, ty = target_info['pos'] # (Row, Col)
                        sx, sy = agv.pos
                        
                        print(f"\n🛑 [PATHFAIL] AGV {agv.id} ({agv.floor}) 無法建立路徑")
                        print(f"   -> 起點: ({sx}, {sy}) | 值: {target_grid[sx][sy] if 0<=sx<rows and 0<=sy<cols else 'Out'}")
                        print(f"   -> 終點: ({tx}, {ty}) | 值: {target_grid[tx][ty] if 0<=tx<rows and 0<=ty<cols else 'Out'}")
                        print(f"   -> 地圖大小: {rows}x{cols}")
                        
                        if not (0 <= tx < rows and 0 <= ty < cols):
                            print(f"   -> ❌ 錯誤：目標座標超出地圖範圍！")
                        elif target_grid[tx][ty] == 2:
                             print(f"   -> ❌ 錯誤：目標點是工作站 (Value=2)，視為障礙物")
                        elif target_grid[tx][ty] == 1:
                             print(f"   -> ⚠️ 注意：目標點是料架 (Value=1)，A* 是否允許終點為障礙？")
                        else:
                             print(f"   -> ❓ 原因不明：可能是孤島或被圍住")

                        # 暫時跳過此單以免卡死迴圈
                        task_idx += 1 
                else:
                    break 

            next_events = [t for t in self.agv_unlock_times.values() if t > sim_time]
            if task_idx < len(self.all_tasks):
                next_events.append(self.all_tasks[task_idx]['datetime'])
            
            if next_events:
                next_time = min(next_events)
                sim_time = max(sim_time + timedelta(seconds=1), next_time)
            else:
                break
                
            if completed % 100 == 0:
                print(f"\r⏳ Time: {sim_time} | Done: {completed}/{len(self.all_tasks)}", end='')

        print(f"\n✅ 模擬完成！")

if __name__ == "__main__":
    FullSimulationRunner().run()