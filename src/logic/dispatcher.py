import pandas as pd
import collections
import math

class TaskDispatcher:
    def __init__(self, cfg):
        self.cfg = cfg
        # 記錄每個工作站的分配結果: {station_id: [order_list]}
        self.station_assignments = collections.defaultdict(list)
        self.station_loads = collections.defaultdict(float) # 預估負載 (秒)
        
        # 工作站即時狀態 (用於模擬執行時)
        self.station_busy_until = {} 
        self.station_buffer = collections.defaultdict(int)
        self.max_buffer = 5

    def plan_wave_assignments(self, orders, available_stations, start_time, deadline):
        """
        [核心邏輯 1] 波次規劃與客戶分配
        1. 根據 Deadine 決定需要幾個工作站 (N)
        2. 執行貪婪演算法分配客戶
        """
        if not orders: return {}
        
        # 1. 歸戶：計算每個客戶的總負載
        customer_groups = collections.defaultdict(list)
        customer_loads = collections.defaultdict(float)
        
        pick_time_per_line = 20.0 # 預估每行揀貨時間
        
        for order in orders:
            cust_id = order.get('PARTCUSTID', 'UNKNOWN')
            customer_groups[cust_id].append(order)
            customer_loads[cust_id] += pick_time_per_line

        # 2. 計算需要幾個站
        total_load_seconds = sum(customer_loads.values())
        available_seconds = (deadline - start_time).total_seconds()
        if available_seconds <= 0: available_seconds = 3600 # 防呆: 預設1小時
        
        # 產能估算: 考慮 AGV 搬運佔比 (假設揀貨佔 40%, 搬運佔 60%)
        # 純作業時間 / 0.4 
        estimated_needed_capacity = total_load_seconds / 0.4
        needed_stations = math.ceil(estimated_needed_capacity / available_seconds)
        
        # 限制在可用工作站範圍內
        needed_stations = max(1, min(needed_stations, len(available_stations)))
        active_stations = available_stations[:needed_stations]
        
        print(f"⚖️ [波次規劃] 訂單總數: {len(orders)} | 客戶數: {len(customer_groups)}")
        print(f"   -> 總需工時: {total_load_seconds:.1f}s | 可用時間: {available_seconds:.1f}s")
        print(f"   -> 建議開啟工作站: {needed_stations} 站 (Pool: {len(available_stations)})")

        # 3. 貪婪分配 (Sort by Load Descending)
        sorted_customers = sorted(customer_loads.items(), key=lambda x: x[1], reverse=True)
        
        # 重置分配表
        self.station_assignments.clear()
        self.station_loads.clear()
        for st in active_stations: self.station_loads[st] = 0.0

        for cust_id, load in sorted_customers:
            # 找出目前負載最小的站
            best_st = min(active_stations, key=lambda st: self.station_loads[st])
            
            # 分配
            for order in customer_groups[cust_id]:
                self.station_assignments[best_st].append(order)
                # 標記訂單屬於該站 (重要：供 Step4 使用)
                order['assigned_station_id'] = best_st
            
            self.station_loads[best_st] += load
            
        return self.station_assignments

    def get_assigned_station(self, order):
        """
        取得該訂單被預先分配的工作站
        """
        return order.get('assigned_station_id')

    def check_station_availability(self, st_id, current_time):
        """
        檢查工作站是否可收貨 (Buffer Check)
        """
        if self.station_buffer[st_id] >= self.max_buffer:
            return False
        return True

    def occupy_station(self, st_id, duration, current_time):
        """
        佔用資源
        """
        self.station_buffer[st_id] += 1
        last_busy = self.station_busy_until.get(st_id, current_time)
        start = max(last_busy, current_time)
        finish = start + pd.Timedelta(seconds=duration)
        self.station_busy_until[st_id] = finish
        return finish

    def release_station(self, st_id):
        if self.station_buffer[st_id] > 0:
            self.station_buffer[st_id] -= 1