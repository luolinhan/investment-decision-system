#!/bin/bash
# 同步板块数据: Mac获取akshare数据 → 推送Windows DB
# 用法: bash sync_sector_to_win.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JSON_FILE="/tmp/sector_data_$(date +%Y%m%d).json"
IMPORT_SCRIPT="/tmp/import_sector.py"

echo "=== 同步板块数据到Windows ==="
echo "日期: $(date)"

# 1. Mac端获取板块数据
echo "[1/4] 从东方财富获取板块数据..."
export HTTP_PROXY=http://127.0.0.1:7890 HTTPS_PROXY=http://127.0.0.1:7890

python3 -c "
import os, json
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'
import akshare as ak
from datetime import datetime

df = ak.stock_board_industry_name_em()
today = datetime.now().strftime('%Y-%m-%d')

def _safe(v):
    if v in (None, '', '-', '--'): return None
    try: return float(str(v).replace(',','').strip())
    except: return None
def _safe_int(v):
    p = _safe(v)
    return None if p is None else int(p)

result = []
for _, row in df.iterrows():
    result.append([
        today,
        str(row.get('板块代码', '')),
        str(row.get('板块名称', '')),
        _safe(row.get('涨跌幅')),
        _safe(row.get('成交额')),
        _safe(row.get('成交量')),
        _safe_int(row.get('上涨家数')),
        _safe_int(row.get('下跌家数')),
        str(row.get('领涨股票', '')),
        _safe(row.get('领涨股票-涨跌幅')),
    ])

with open('$JSON_FILE', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False)
print(f'获取 {len(result)} 条板块数据')
"

if [ ! -f "$JSON_FILE" ]; then
    echo "ERROR: 获取数据失败"
    exit 1
fi

# 2. 推送JSON到Windows
echo "[2/4] 推送数据到Windows..."
scp "$JSON_FILE" win-exec:'C:\Projects\research_report_system\sector_data.json'

# 3. 推送导入脚本
echo "[3/4] 推送导入脚本..."
cat > "$IMPORT_SCRIPT" << 'PYEOF'
import sqlite3, json
from datetime import datetime

db_path = r'C:\Projects\research_report_system\data\investment.db'
json_path = r'C:\Projects\research_report_system\sector_data.json'

conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS sector_performance (
    trade_date TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    change_pct REAL,
    turnover REAL,
    volume REAL,
    rise_count INTEGER,
    fall_count INTEGER,
    lead_stock TEXT,
    lead_stock_pct REAL,
    PRIMARY KEY (trade_date, sector_code)
)''')

with open(json_path, encoding='utf-8') as f:
    data = json.load(f)

today = datetime.now().strftime('%Y-%m-%d')
c.execute('DELETE FROM sector_performance WHERE trade_date = ?', (today,))

for row in data:
    c.execute('INSERT OR REPLACE INTO sector_performance VALUES (?,?,?,?,?,?,?,?,?,?)', row)

conn.commit()
c.execute('SELECT COUNT(*) FROM sector_performance WHERE trade_date = ?', (today,))
print(f'已导入 {c.fetchone()[0]} 条记录')
conn.close()
PYEOF
scp "$IMPORT_SCRIPT" win-exec:'C:\Projects\research_report_system\import_sector.py'

# 4. 执行导入
echo "[4/4] 执行导入..."
ssh win-exec "cd C:\Projects\research_report_system && python import_sector.py"

# 清理
rm -f "$JSON_FILE" "$IMPORT_SCRIPT"

echo "=== 同步完成 ==="
