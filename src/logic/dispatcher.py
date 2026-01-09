import pandas as pd
import collections

class TaskDispatcher:
    def __init__(self, cfg):
        self.cfg = cfg
        # 工作站狀態: {station_id: busy_until_time}
        self.station_schedule = {} 
        
        # 簡易產能限制 (Buffer): 每個工作站同時只能處理 N 個箱子
        self.max_buffer = 5
        self.station_buffer = collections.defaultdict(int)

    def get_best_workstation(self, floor, current_time):
        """
        [決策邏輯]
        1. 根據樓層 (2F/3F) 篩選工作站
        2. 找出目前 Buffer 最空、或者最快能閒置的工作站
        """
        # 假設工作站 ID: 2F (1~8), 3F (101~108)
        if floor == '2F':
            candidates = list(range(1, 9))
        else:
            candidates = list(range(101, 109)) # 假設 3F ID 是 101 起跳

        best_st = None
        min_load = 9999
        
        for st_id in candidates:
            # 檢查 Buffer
            current_load = self.station_buffer[st_id]
            
            # 檢查時間 (是否忙碌)
            busy_until = self.station_schedule.get(st_id, current_time)
            if busy_until > current_time:
                # 換算成虛擬負載 (每忙 1 分鐘 = 1 load)
                time_load = (busy_until - current_time).total_seconds() / 60
                current_load += time_load

            if current_load < min_load:
                min_load = current_load
                best_st = st_id
        
        return best_st

    def assign_task(self, st_id, duration_seconds, current_time):
        """
        佔用工作站資源
        """
        # 更新 Buffer
        self.station_buffer[st_id] += 1
        
        # 更新時間表
        last_busy = self.station_schedule.get(st_id, current_time)
        start_time = max(last_busy, current_time)
        finish_time = start_time + pd.Timedelta(seconds=duration_seconds)
        
        self.station_schedule[st_id] = finish_time
        return finish_time

    def release_task(self, st_id):
        """
        任務完成，釋放 Buffer
        """
        if self.station_buffer[st_id] > 0:
            self.station_buffer[st_id] -= 1