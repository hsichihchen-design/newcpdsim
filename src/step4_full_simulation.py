import pandas as pd
import numpy as np
import os
import sys
import csv
import time
from datetime import datetime, timedelta

from engine.configs import SimConfig
from engine.physics import MapWorld, AGV
from logic.dispatcher import TaskDispatcher

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

class FullSimulationRunner:
    def __init__(self):
        print(f"🚀 [Step 4] 啟動全邏輯模擬 (Wave-Aware + Receiving Fix)...")
        self.cfg = SimConfig(BASE_DIR)
        
        # 1. 物理世界
        self.world_2f = MapWorld('2F', '2F_map.xlsx', BASE_DIR)
        self.world_3f = MapWorld('3F', '3F_map.xlsx', BASE_DIR)
        
        self.shelf_map = self._load_shelf_map()
        self.st_coords = {}
        self._map_station_coords(self.world_2f, 1)
        self._map_station_coords(self.world_3f, 101)

        # 2. 資源
        self.agvs_2f = self._init_agvs(self.world_2f, self.cfg.get('planned_staff_2f', 8), 1)
        self.agvs_3f = self._init_agvs(self.world_3f, self.cfg.get('planned_staff_3f', 8), 101)
        self.all_agvs = self.agvs_2f + self.agvs_3f
        
        self.dispatcher = TaskDispatcher(self.cfg)
        
        # 3. 訂單載入
        self.agv_unlock_times = {} 
        self.orders = self._load_orders()
        self.receivings = self._load_receiving()
        
        # 時間對齊
        self._align_receiving_year()
        
        self.all_tasks_source = sorted(self.orders + self.receivings, key=lambda x: x['datetime'])
        
        if self.all_tasks_source:
            min_dt = self.all_tasks_source[0]['datetime']
            max_dt = self.all_tasks_source[-1]['datetime']
            print(f"📅 模擬範圍: {min_dt} ~ {max_dt}")

        self.order_queue = [] 
        self.stats = {'ship_done': 0, 'recv_done': 0, 'delayed': 0}

    def _map_station_coords(self, world, start_id):
        rows, cols = world.grid.shape
        found = 0
        for r in range(rows):
            for c in range(cols):
                if world.grid[r][c] == 2:
                    st_id = start_id + found
                    # 存入 (row, col)
                    self.st_coords[st_id] = {'floor': world.floor, 'pos': (r, c)}
                    found += 1
        print(f"   -> {world.floor} 偵測到 {found} 個工作站")

    def _load_shelf_map(self):
        path = os.path.join(BASE_DIR, 'data', 'mapping', 'shelf_coordinate_map.csv')
        try: df = pd.read_csv(path, encoding='utf-8')
        except: df = pd.read_csv(path, encoding='cp950')
        mapping = {}
        for _, row in df.iterrows():
            mapping[str(row['shelf_id'])] = {'floor': row['floor'], 'pos': (int(row['x']), int(row['y']))}
        return mapping

    def _init_agvs(self, world, count, start_id):
        agvs = []
        candidates = []
        rows, cols = world.grid.shape
        # 優先找充電站(3)
        for r in range(rows):
            for c in range(cols):
                if world.grid[r][c] == 3: candidates.append((r,c))
        
        if len(candidates) < count:
            # 不夠就找空地(0)
            for r in range(rows):
                for c in range(cols):
                    if world.grid[r][c] == 0: candidates.append((r,c))
        
        if not candidates: candidates = [(0,0)] # Fallback

        for i in range(count):
            pos = candidates[i % len(candidates)]
            agv = AGV(start_id + i, world.floor, pos)
            agvs.append(agv)
        return agvs

    def _load_orders(self):
        path = os.path.join(BASE_DIR, 'data', 'transaction', 'wave_orders.csv')
        try: df = pd.read_csv(path, encoding='utf-8-sig')
        except: df = pd.read_csv(path, encoding='cp950')
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['WAVE_DEADLINE'] = pd.to_datetime(df['WAVE_DEADLINE'])
        df['TASK_TYPE'] = 'PICKING'
        return df.to_dict('records')

    def _load_receiving(self):
        path = os.path.join(BASE_DIR, 'data', 'transaction', 'historical_receiving_ex.csv')
        if not os.path.exists(path): return []
        try:
            try: df = pd.read_csv(path, encoding='utf-8')
            except: df = pd.read_csv(path, encoding='cp950')
            df['datetime'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'], errors='coerce')
            df.dropna(subset=['datetime'], inplace=True)
            df['TASK_TYPE'] = 'RECEIVING'
            # 進貨沒有 Wave ID，給一個標記
            df['WAVE_ID'] = 'RECEIVING_DAILY'
            df['WAVE_DEADLINE'] = df['datetime'] + timedelta(days=3)
            return df.to_dict('records')
        except: return []

    def _align_receiving_year(self):
        if not self.orders or not self.receivings: return
        order_year = self.orders[0]['datetime'].year
        recv_year = self.receivings[0]['datetime'].year
        if order_year != recv_year:
            offset = order_year - recv_year
            for r in self.receivings:
                try:
                    r['datetime'] = r['datetime'].replace(year=r['datetime'].year + offset)
                    if 'WAVE_DEADLINE' in r:
                        r['WAVE_DEADLINE'] = r['WAVE_DEADLINE'].replace(year=r['WAVE_DEADLINE'].year + offset)
                except: pass

    def get_travel_time(self, agv, target_pos):
        # 曼哈頓距離 / 速度
        dist = abs(agv.pos[0] - target_pos[0]) + abs(agv.pos[1] - target_pos[1])
        return timedelta(seconds=dist / 1.5)

    def run(self, duration_days=30):
        print(f"\n🎬 開始模擬...")
        
        # Log 1: KPI (加入 wave_id)
        f_kpi = open(os.path.join(LOG_DIR, 'simulation_kpi.csv'), 'w', newline='', encoding='utf-8')
        kpi_writer = csv.writer(f_kpi)
        kpi_writer.writerow(['task_id', 'type', 'floor', 'workstation', 'wave_id', 'finish_time', 'is_delayed', 'duration'])
        
        # Log 2: Events
        f_event = open(os.path.join(LOG_DIR, 'simulation_events.csv'), 'w', newline='', encoding='utf-8')
        event_writer = csv.writer(f_event)
        event_writer.writerow(['start_time', 'end_time', 'floor', 'obj_id', 'sx', 'sy', 'ex', 'ey', 'type', 'text'])

        if not self.all_tasks_source: return

        sim_start_time = self.all_tasks_source[0]['datetime']
        current_time = sim_start_time
        last_order_time = self.all_tasks_source[-1]['datetime']
        end_time = max(sim_start_time + timedelta(days=duration_days), last_order_time + timedelta(hours=1))
        
        task_idx = 0
        total_tasks = len(self.all_tasks_source)
        real_start = time.time()
        
        while current_time < end_time and (task_idx < total_tasks or self.order_queue or self.agv_unlock_times):
            
            # 1. 釋放任務
            while task_idx < total_tasks and self.all_tasks_source[task_idx]['datetime'] <= current_time:
                raw_task = self.all_tasks_source[task_idx]
                target_shelf_id = list(self.shelf_map.keys())[task_idx % len(self.shelf_map)]
                target_info = self.shelf_map[target_shelf_id]
                assigned_st = self.dispatcher.get_best_workstation(target_info['floor'], current_time)
                
                sim_task = {
                    'id': f"T_{task_idx}",
                    'raw': raw_task,
                    'type': raw_task['TASK_TYPE'],
                    'floor': target_info['floor'],
                    'shelf_pos': target_info['pos'],
                    'workstation_id': assigned_st,
                    'start_time': current_time,
                    'wave_id': raw_task.get('WAVE_ID', 'UNKNOWN')
                }
                self.order_queue.append(sim_task)
                task_idx += 1

            # 2. 完成檢查
            for agv_id, unlock_time in list(self.agv_unlock_times.items()):
                if unlock_time <= current_time:
                    del self.agv_unlock_times[agv_id] 
                    agv = next((a for a in self.all_agvs if a.id == agv_id), None)
                    if agv and agv.current_task:
                        task = agv.current_task
                        self.dispatcher.release_task(task['workstation_id'])
                        agv.pos = task['shelf_pos']
                        
                        raw = task['raw']
                        duration = (unlock_time - task['start_time']).total_seconds()
                        is_delayed = unlock_time > raw['WAVE_DEADLINE']
                        
                        # 寫入 KPI (含 Wave ID)
                        kpi_writer.writerow([
                            task['id'], task['type'], task['floor'], task['workstation_id'],
                            task['wave_id'], unlock_time, 'Y' if is_delayed else 'N', duration
                        ])
                        
                        if task['type'] == 'PICKING': self.stats['ship_done'] += 1
                        else: self.stats['recv_done'] += 1
                        
                        agv.current_task = None
                        agv.status = 'IDLE'

            # 3. 指派任務
            remaining_queue = []
            for task in self.order_queue:
                assigned = False
                target_agvs = self.agvs_2f if task['floor'] == '2F' else self.agvs_3f
                available_agvs = [a for a in target_agvs if a.id not in self.agv_unlock_times]
                
                if available_agvs:
                    best_agv = available_agvs[0]
                    travel_duration = self.get_travel_time(best_agv, task['shelf_pos'])
                    op_seconds = 20 if task['type'] == 'PICKING' else 30
                    total_duration = travel_duration + timedelta(seconds=op_seconds)
                    finish_time = current_time + total_duration
                    
                    self.dispatcher.assign_task(task['workstation_id'], total_duration.total_seconds(), current_time)
                    self.agv_unlock_times[best_agv.id] = finish_time
                    best_agv.current_task = task
                    best_agv.status = 'BUSY'
                    
                    # Log Events
                    # [重點修正] 座標寫入: output_x = pos[1] (Col), output_y = pos[0] (Row)
                    if task['workstation_id'] in self.st_coords:
                        st = self.st_coords[task['workstation_id']]
                        event_writer.writerow([
                            current_time, finish_time, st['floor'], 
                            f"WS_{task['workstation_id']}",
                            st['pos'][1], st['pos'][0], # X=Col, Y=Row
                            st['pos'][1], st['pos'][0],
                            'STATION_BUSY', 
                            task['wave_id'] # 顯示 Wave ID 而非單號
                        ])

                    event_writer.writerow([
                        current_time, finish_time, best_agv.floor, 
                        f"AGV_{best_agv.id}",
                        best_agv.pos[1], best_agv.pos[0], # X=Col, Y=Row
                        task['shelf_pos'][1], task['shelf_pos'][0],
                        'AGV_MOVE', ''
                    ])
                    assigned = True
                
                if not assigned: remaining_queue.append(task)
            self.order_queue = remaining_queue

            # 4. 跳躍
            next_times = []
            if task_idx < total_tasks: next_times.append(self.all_tasks_source[task_idx]['datetime'])
            if self.agv_unlock_times: next_times.append(min(self.agv_unlock_times.values()))
            
            if next_times:
                next_wake = min(next_times)
                if next_wake > current_time: current_time = next_wake
                else: current_time += timedelta(seconds=1)
            else:
                break
                
            if (self.stats['ship_done'] + self.stats['recv_done']) % 5000 == 0:
                print(f"\r🚀 時間: {current_time} | 出貨: {self.stats['ship_done']} | 進貨: {self.stats['recv_done']}", end='')

        f_kpi.close()
        f_event.close()
        print(f"\n✅ 模擬結束！耗時: {time.time() - real_start:.2f} 秒")

if __name__ == "__main__":
    sim = FullSimulationRunner()
    sim.run(duration_days=180)