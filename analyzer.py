# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import matplotlib

# 強制使用 Agg 後端以在無顯示器的伺服器環境運行
matplotlib.use('Agg')

# 設定字型：優先使用 Linux 上的 Noto Sans CJK，確保 GitHub Actions 中文不亂碼
plt.rcParams['font.sans-serif'] = [
    'Noto Sans CJK TC', 
    'Noto Sans CJK JP', 
    'Microsoft JhengHei', 
    'Arial Unicode MS', 
    'sans-serif'
]
plt.rcParams['axes.unicode_minus'] = False

# 分箱參數設定
BIN_SIZE = 10.0
X_MIN, X_MAX = -100, 100
BINS = np.arange(X_MIN, X_MAX + 11, BIN_SIZE)

def build_company_list(arr_pct, codes, names, bins):
    """產出 HTML 格式的分箱公司清單"""
    lines = [f"{'報酬區間':<12} | {'家數(比例)':<14} | 公司清單", "-"*80]
    total = len(arr_pct)
    
    # 將數據限制在範圍內進行統計
    clipped_arr = np.clip(arr_pct, -100, 100)
    counts, edges = np.histogram(clipped_arr, bins=bins)

    for i in range(len(edges)-1):
        lo, up = edges[i], edges[i+1]
        lab = f"{int(lo)}%~{int(up)}%"
        mask = (arr_pct >= lo) & (arr_pct < up)
        
        # 若是最後一個區間，包含最大值
        if i == len(edges) - 2:
            mask = (arr_pct >= lo) & (arr_pct <= up)

        cnt = int(mask.sum())
        if cnt == 0: continue

        picked_indices = np.where(mask)[0]
        links = []
        for idx in picked_indices:
            code, name = codes[idx], names[idx]
            link = f'<a href="https://www.wantgoo.com/stock/{code}" style="text-decoration:none; color:#0366d6;">{code}({name})</a>'
            links.append(link)
        
        lines.append(f"{lab:<12} | {cnt:>4} ({(cnt/total*100):5.1f}%) | {', '.join(links)}")

    return "\n".join(lines)

def run_global_analysis(market_id="tw-share"):
    """核心分析引擎：產出 9 張圖表與文字報表"""
    print(f"📊 正在啟動 {market_id} 深度矩陣分析...")
    
    base_path = Path(os.path.abspath("./data"))
    data_path = base_path / market_id / "dayK"
    image_out_dir = Path(os.path.abspath("./output/images")) / market_id
    image_out_dir.mkdir(parents=True, exist_ok=True)
    
    all_files = list(data_path.glob("*.csv"))
    if not all_files:
        print("⚠️ 無數據檔案可供分析")
        return [], pd.DataFrame(), {}

    results = []
    for f in tqdm(all_files, desc="分析數據"):
        try:
            df = pd.read_csv(f)
            if len(df) < 20: continue # 至少需一個月數據
            df.columns = [c.lower() for c in df.columns]
            
            close = df['close'].values
            high = df['high'].values
            low = df['low'].values

            # 定義分析週期
            periods = [('Week', 5), ('Month', 20), ('Year', 250)]
            
            filename = f.stem
            ticker, company_name = filename.split('_', 1) if '_' in filename else (filename, filename)

            row = {'Ticker': ticker, 'Full_ID': company_name}

            for p_name, days in periods:
                if len(close) <= days: continue
                # 取得基準價 (週期前一天的收盤)
                prev_c = close[-(days+1)]
                if prev_c == 0: continue
                
                row[f'{p_name}_High'] = (max(high[-days:]) - prev_c) / prev_c * 100
                row[f'{p_name}_Close'] = (close[-1] - prev_c) / prev_c * 100
                row[f'{p_name}_Low'] = (min(low[-days:]) - prev_c) / prev_c * 100
            
            results.append(row)
        except: continue

    df_res = pd.DataFrame(results)
    generated_images = []
    
    # 產出 3x3 組合的 9 張圖表
    for p_name, p_zh in [('Week', '週'), ('Month', '月'), ('Year', '年')]:
        for t_name, t_zh in [('High', '最高-進攻'), ('Close', '收盤-實質'), ('Low', '最低-防禦')]:
            col = f"{p_name}_{t_name}"
            if col not in df_res.columns: continue
            data = df_res[col].dropna()
            
            fig, ax = plt.subplots(figsize=(11, 7))
            clipped_data = np.clip(data.values, X_MIN, X_MAX)
            counts, edges = np.histogram(clipped_data, bins=BINS)
            
            color_map = {'High': '#28a745', 'Close': '#007bff', 'Low': '#dc3545'}
            bars = ax.bar(edges[:-1], counts, width=9, align='edge', color=color_map[t_name], alpha=0.7, edgecolor='white')
            
            # 加上數字與百分比標籤，並預留頂部空間避免文字卡住
            max_count = counts.max()
            for bar in bars:
                h = bar.get_height()
                if h > 0:
                    ax.text(bar.get_x() + 4.5, h + max_count*0.02, f'{int(h)}\n({h/len(data)*100:.1f}%)', 
                            ha='center', va='bottom', fontsize=9)

            ax.set_title(f"{p_zh}K {t_zh} 報酬分布 ({market_id})", fontsize=18, pad=30, fontweight='bold')
            ax.set_ylim(0, max_count * 1.35) # 預留 35% 空間
            ax.set_xticks(BINS)
            ax.set_xticklabels([f"{int(x)}%" for x in BINS], rotation=45)
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            plt.subplots_adjust(top=0.85, bottom=0.15)

            img_path = image_out_dir / f"{col.lower()}.png"
            plt.savefig(img_path, dpi=120)
            plt.close()

            generated_images.append({'id': col.lower(), 'path': str(img_path), 'label': f"{p_zh}K {t_zh}"})

    # 產出分箱文字清單
    text_reports = {}
    for p_name, p_zh in [('Week', '週K'), ('Month', '月K'), ('Year', '年K')]:
        col = f'{p_name}_High'
        if col in df_res.columns:
            data_list = df_res[col].values
            codes = df_res['Ticker'].tolist()
            names = df_res['Full_ID'].tolist()
            report_text = build_company_list(data_list, codes, names, BINS)
            text_reports[p_name] = f"<h3>📊 {p_zh} 最高價分箱清單</h3><pre style='font-family:monospace; background:#f4f4f4; padding:15px;'>{report_text}</pre>"

    return generated_images, df_res, text_reports
