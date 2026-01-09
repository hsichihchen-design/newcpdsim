import pandas as pd
import numpy as np
import os
import sys

# ==========================================
# 設定檔案路徑
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_MASTER_DIR = os.path.join(BASE_DIR, 'data', 'master')
DATA_MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')

# 確保輸出資料夾存在
os.makedirs(DATA_MAPPING_DIR, exist_ok=True)

# 檔案名稱
MAP_2F_FILE = '2F_map.xlsx'
MAP_3F_FILE = '3F_map.xlsx'
ALL_CELL_LIST_FILE = 'all_cell_list.csv'
ITEM_INVENTORY_FILE = 'item_inventory.csv'

OUTPUT_MAP_FILE = 'shelf_coordinate_map.csv'

def load_excel_map(filename):
    """讀取 Excel 地圖並回傳 numpy array"""
    path = os.path.join(DATA_MASTER_DIR, filename)
    print(f"📖 正在讀取地圖: {filename} ...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到地圖檔: {path}")
    
    df = pd.read_excel(path, header=None).fillna(0)
    return df.to_numpy()

def get_shelf_coordinates(grid):
    """從地圖網格中提取所有 '1' (料架) 的座標 (row, col)"""
    rows, cols = grid.shape
    shelf_coords = []
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == 1:
                shelf_coords.append((r, c))
    # 排序：優先由上到下 (Row)，再由左到右 (Col)
    # 確保料架填入順序是線性的
    shelf_coords.sort(key=lambda x: (x[0], x[1]))
    return shelf_coords

def main():
    print("🚀 [Step 1] 啟動資料載入與地圖初始化 (修正版: Cell -> Shelf)...")
    
    # 1. 讀取地圖
    try:
        grid_2f = load_excel_map(MAP_2F_FILE)
        grid_3f = load_excel_map(MAP_3F_FILE)
    except Exception as e:
        print(f"❌ 地圖讀取失敗: {e}")
        sys.exit(1)

    shelves_2f_coords = get_shelf_coordinates(grid_2f)
    shelves_3f_coords = get_shelf_coordinates(grid_3f)

    print(f"   -> 2F 地圖料架空位: {len(shelves_2f_coords)} 格")
    print(f"   -> 3F 地圖料架空位: {len(shelves_3f_coords)} 格")

    # 2. 讀取儲位清單並歸戶為料架
    cell_list_path = os.path.join(DATA_MASTER_DIR, ALL_CELL_LIST_FILE)
    if not os.path.exists(cell_list_path):
        print(f"❌ 找不到儲位清單: {cell_list_path}")
        sys.exit(1)
    
    df_cells = pd.read_csv(cell_list_path)
    target_col = 'CELL_ID' if 'CELL_ID' in df_cells.columns else df_cells.columns[0]
    all_cells = df_cells[target_col].astype(str).tolist()

    print(f"📖 讀取到 {len(all_cells)} 筆儲位編號 (來源: {ALL_CELL_LIST_FILE})")

    # --- 關鍵修正：歸戶邏輯 ---
    # Shelf ID = Cell ID 前 7 碼
    # 結構: { 'Shelf_ID': [Cell_ID_1, Cell_ID_2, ...] }
    
    shelves_2f_map = {} # 2F 的實體料架 -> 包含哪些 Cell
    shelves_3f_map = {} # 3F 的實體料架 -> 包含哪些 Cell

    for cell in all_cells:
        cell = cell.strip()
        if len(cell) < 7: continue # 防呆

        shelf_id = cell[:7] # 取前7碼
        
        if cell.startswith('2'):
            if shelf_id not in shelves_2f_map: shelves_2f_map[shelf_id] = []
            shelves_2f_map[shelf_id].append(cell)
        elif cell.startswith('3'):
            if shelf_id not in shelves_3f_map: shelves_3f_map[shelf_id] = []
            shelves_3f_map[shelf_id].append(cell)

    unique_shelves_2f = sorted(list(shelves_2f_map.keys()))
    unique_shelves_3f = sorted(list(shelves_3f_map.keys()))

    print(f"💡 歸戶後實體料架數量:")
    print(f"   -> 2F 料架: {len(unique_shelves_2f)} 架 (來自 {sum(len(v) for v in shelves_2f_map.values())} 個儲位)")
    print(f"   -> 3F 料架: {len(unique_shelves_3f)} 架 (來自 {sum(len(v) for v in shelves_3f_map.values())} 個儲位)")

    # 3. 容量檢核 (Strict Mode - Check Shelves, not Cells)
    errors = []
    if len(unique_shelves_2f) > len(shelves_2f_coords):
        errors.append(f"💥 [嚴重錯誤] 2樓地圖格位不足！需 {len(unique_shelves_2f)} 格，但地圖只有 {len(shelves_2f_coords)} 格")
    
    if len(unique_shelves_3f) > len(shelves_3f_coords):
        errors.append(f"💥 [嚴重錯誤] 3樓地圖格位不足！需 {len(unique_shelves_3f)} 格，但地圖只有 {len(shelves_3f_coords)} 格")

    if errors:
        for err in errors: print(err)
        print("🛑 請擴大地圖範圍或減少料架數量。程式終止。")
        sys.exit(1)

    # 4. 進行映射 (Shelf ID -> Coordinate -> All Cells)
    mapping_data = []

    # 2F Mapping
    for i, shelf_id in enumerate(unique_shelves_2f):
        r, c = shelves_2f_coords[i]
        # 該料架下的所有 Cell 都共用這個座標
        for cell_id in shelves_2f_map[shelf_id]:
            mapping_data.append({
                'cell_id': cell_id,
                'shelf_id': shelf_id,
                'floor': '2F',
                'x': r,
                'y': c
            })
    
    # 3F Mapping
    for i, shelf_id in enumerate(unique_shelves_3f):
        r, c = shelves_3f_coords[i]
        for cell_id in shelves_3f_map[shelf_id]:
            mapping_data.append({
                'cell_id': cell_id,
                'shelf_id': shelf_id,
                'floor': '3F',
                'x': r,
                'y': c
            })

    # 5. 輸出結果
    df_map = pd.DataFrame(mapping_data)
    output_path = os.path.join(DATA_MAPPING_DIR, OUTPUT_MAP_FILE)
    df_map.to_csv(output_path, index=False)

    print(f"✅ 映射成功！已建立座標對照表: {OUTPUT_MAP_FILE}")
    print(f"   -> 總計映射儲位: {len(df_map)} 筆")
    print(f"   -> 2F 剩餘料架空位: {len(shelves_2f_coords) - len(unique_shelves_2f)}")
    print(f"   -> 3F 剩餘料架空位: {len(shelves_3f_coords) - len(unique_shelves_3f)}")
    
    # 6. 驗證 Item Inventory
    print("🔍 驗證庫存資料一致性...")
    inv_path = os.path.join(DATA_MASTER_DIR, ITEM_INVENTORY_FILE)
    if os.path.exists(inv_path):
        df_inv = pd.read_csv(inv_path)
        # 尋找儲位欄位
        inv_cell_col = None
        for col in df_inv.columns:
            if 'CELL' in col.upper() or 'LOC' in col.upper():
                inv_cell_col = col
                break
        if not inv_cell_col: inv_cell_col = df_inv.columns[1] # Fallback

        mapped_cells = set(df_map['cell_id'].astype(str))
        inv_cells = set(df_inv[inv_cell_col].astype(str))
        
        missing = [c for c in inv_cells if c not in mapped_cells]
        if missing:
            print(f"⚠️ 警告: 有 {len(missing)} 個庫存儲位無法在地圖上找到 (可能是 4F 或舊儲位)")
            print(f"   (範例: {missing[:3]}...)")
        else:
            print("✅ 完美！所有庫存零件的儲位都在地圖上有對應座標。")

if __name__ == "__main__":
    main()