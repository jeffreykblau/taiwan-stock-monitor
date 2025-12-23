# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝/匯入必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# 路徑定義
MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 狀態管理檔案
MANIFEST_CSV = Path(LIST_DIR) / "jp_manifest.csv"
LIST_ALL_CSV = Path(LIST_DIR) / "jp_list_all.csv"
THREADS = 4 # GitHub Actions 環境建議 4，避免封鎖 IP

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_tse_list():
    """獲取日股清單：具備門檻檢查與歷史快取備援"""
    threshold = 3800 
    log("📡 正在獲取東京交易所標的清單...")
    try:
        df = pd.read_csv(tse.csv_file_path)
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Company Name'] if c in df.columns), None)

        if not code_col: raise KeyError("無法定位代碼欄位")

        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            if len(code) >= 4 and code[:4].isdigit():
                res.append({
                    "code": code[:4], 
                    "name": str(row[name_col]) if name_col else code[:4], 
                    "board": "T"
                })
        
        final_df = pd.DataFrame(res).drop_duplicates(subset=['code'])
        
        if len(final_df) < threshold:
            log(f"⚠️ 數量異常 ({len(final_df)})，嘗試讀取歷史快取...")
            if LIST_ALL_CSV.exists(): return pd.read_csv(LIST_ALL_CSV)
        else:
            final_df.to_csv(LIST_ALL_CSV, index=False, encoding='utf-8-sig')
            log(f"✅ 成功獲取 {len(final_df)} 檔日股清單")
        return final_df

    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return pd.read_csv(LIST_ALL_CSV) if LIST_ALL_CSV.exists() else pd.DataFrame()

def build_manifest(df_list):
    """建立續跑清單，並自動識別已下載完成的檔案"""
    if df_list.empty: return pd.DataFrame()

    if MANIFEST_CSV.exists():
        mf = pd.read_csv(MANIFEST_CSV)
        # 確保新的 code 若不在 mf 裡則加入
        new_codes = df_list[~df_list['code'].astype(str).isin(mf['code'].astype(str))]
        if not new_codes.empty:
            new_codes_df = new_codes.copy()
            new_codes_df['status'] = 'pending'
            mf = pd.concat([mf, new_codes_df], ignore_index=True)
        return mf
    
    df_list = df_list.copy()
    df_list["status"] = "pending"
    # 掃描資料夾，將已存在的檔案標記為 done
    existing_files = {f.split(".")[0] for f in os.listdir(DATA_DIR) if f.endswith(".T.csv")}
    df_list.loc[df_list['code'].astype(str).isin(existing_files), "status"] = "done"
    
    df_list.to_csv(MANIFEST_CSV, index=False)
    return df_list

def download_one(row_tuple):
    """強化版下載：加入 3 次重試機制與動態延遲"""
    idx, row = row_tuple
    code = str(row['code']).zfill(4)
    symbol = f"{code}.T"
    out_path = os.path.join(DATA_DIR, f"{code}.T.csv")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 隨機延遲保護：0.5 ~ 1.5 秒
            time.sleep(random.uniform(0.5, 1.5)) 
            
            tk = yf.Ticker(symbol)
            # 下載 2 年數據
            df_raw = tk.history(period="2y", interval="1d", auto_adjust=True, timeout=20)
            
            if df_raw is not None and not df_raw.empty:
                df_raw.reset_index(inplace=True)
                df_raw.columns = [c.lower() for c in df_raw.columns]
                
                if 'date' in df_raw.columns:
                    df_raw['date'] = pd.to_datetime(df_raw['date'], utc=True).dt.tz_localize(None)
                
                # 僅保留核心欄位
                cols = ['date','open','high','low','close','volume']
                df_final = df_raw[[c for c in cols if c in df_raw.columns]]
                df_final.to_csv(out_path, index=False, encoding='utf-8-sig')
                return idx, "done"
            
            if attempt == max_retries - 1:
                return idx, "empty"

        except Exception:
            if attempt == max_retries - 1:
                return idx, "failed"
            time.sleep(random.randint(3, 7))
            
    return idx, "failed"

def main():
    log("🇯🇵 日本股市 K 線同步器啟動 (數據統計優化版)")
    
    # 1. 獲取清單與 Manifest
    df_list = get_tse_list()
    if df_list.empty: 
        log("🚨 無法取得清單，結束程序。")
        return
    mf = build_manifest(df_list)

    # 2. 篩選待處理標的 (排除已成功或確定沒資料的)
    todo = mf[~mf["status"].isin(["done", "empty"])]
    
    if not todo.empty:
        log(f"📝 待處理標的數：{len(todo)} 檔 (含重試之前失敗項)")
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
            pbar = tqdm(total=len(todo), desc="日股下載進度")
            count = 0
            try:
                for f in as_completed(futures):
                    idx, status = f.result()
                    mf.at[idx, "status"] = status
                    count += 1
                    pbar.update(1)
                    if count % 100 == 0:
                        mf.to_csv(MANIFEST_CSV, index=False)
            except KeyboardInterrupt:
                log("🛑 使用者中斷下載...")
            finally:
                mf.to_csv(MANIFEST_CSV, index=False)
                pbar.close()
    else:
        log("✅ 數據已是最新狀態，無需下載新標的。")

    # 3. 計算數據統計 (用於 Email 通知)
    total_expected = len(mf)
    # 有效成功 = 狀態為 'done' 的總數 (包含歷史快取 + 本次新抓)
    effective_success = len(mf[mf['status'] == 'done'])
    fail_count = total_expected - effective_success

    download_stats = {
        "total": total_expected,
        "success": effective_success,
        "fail": fail_count
    }

    log("="*30)
    log(f"📊 下載統計報告:")
    log(f"   - 應收總數: {total_expected}")
    log(f"   - 成功(含舊檔): {effective_success}")
    log(f"   - 失敗/無數據: {fail_count}")
    log(f"   - 數據完整度: {(effective_success/total_expected)*100:.2f}%")
    log("="*30)

    # 4. 回傳統計數據供後續 notifier.py 使用
    # 在 GitHub Actions 流程中，你可以將此 dictionary 傳遞給發信函數
    return download_stats

if __name__ == "__main__":
    main()
