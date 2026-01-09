import pandas as pd
import os

class SimConfig:
    def __init__(self, base_dir):
        self.params = {}
        self._load_parameters(base_dir)

    def _load_parameters(self, base_dir):
        # 路徑指向 data/master/system_parameters.csv
        path = os.path.join(base_dir, 'data', 'master', 'system_parameters.csv')
        if not os.path.exists(path):
            print(f"⚠️ 警告: 找不到參數檔 {path}，使用預設值")
            return

        try:
            # 強健讀取 (utf-8 或 cp950)
            try:
                df = pd.read_csv(path, encoding='utf-8')
            except:
                df = pd.read_csv(path, encoding='cp950')

            # 轉為字典: {parameter_name: parameter_value}
            # 依據 CSV 欄位: parameter_name, parameter_value, data_type...
            for _, row in df.iterrows():
                name = str(row['parameter_name']).strip()
                val_str = str(row['parameter_value']).strip()
                dtype = str(row['data_type']).strip().lower()

                # 型別轉換
                if val_str.lower() == 'nan' or val_str == '':
                    val = None
                elif dtype in ['integer', 'int']:
                    try:
                        val = int(float(val_str))
                    except:
                        val = 0
                elif dtype == 'float':
                    try:
                        val = float(val_str)
                    except:
                        val = 0.0
                else:
                    val = val_str
                
                self.params[name] = val
            
            print(f"⚙️ 系統參數已載入: {len(self.params)} 筆")

        except Exception as e:
            print(f"❌ 讀取參數檔失敗: {e}")

    def get(self, key, default=None):
        return self.params.get(key, default)

    # --- 常用參數捷徑 (Property) ---
    @property
    def pick_time_normal(self): 
        return self.get('picking_base_time_no_repack', 18)
    
    @property
    def pick_time_repack(self): 
        return self.get('picking_base_time_repack', 15)
    
    @property
    def repack_add_time(self): 
        return self.get('repack_additional_time', 7)
    
    @property
    def skill_impact(self): 
        return self.get('skill_impact_multiplier', 0.2)
    
    @property
    def wave_prep_time(self):
        return self.get('wave_preparation_minutes', 3)