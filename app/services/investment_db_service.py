"""
投资数据API服务 - 从本地数据库读取
"""
import sqlite3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

DB_PATH = "data/investment.db"


class InvestmentDataService:
    """投资数据服务"""

    def __init__(self):
        self.db_path = DB_PATH

    def _get_db(self):
        return sqlite3.connect(self.db_path)

    def get_index_list(self) -> List[Dict]:
        """获取指数列表"""
        conn = self._get_db()
        c = conn.cursor()
        c.execute('''
            SELECT DISTINCT code, name FROM index_history ORDER BY code
        ''')
        result = [{"code": row[0], "name": row[1]} for row in c.fetchall()]
        conn.close()
        return result

    def get_index_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取指数历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, open, high, low, close, volume, change_pct
            FROM index_history
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "change_pct": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_index_latest(self, code: str) -> Optional[Dict]:
        """获取指数最新数据"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT code, name, trade_date, close, change_pct
            FROM index_history
            WHERE code = ?
            ORDER BY trade_date DESC LIMIT 1
        ''', (code,))

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "code": row[0],
                "name": row[1],
                "date": row[2],
                "close": row[3],
                "change_pct": row[4]
            }
        return None

    def get_all_indices_latest(self) -> Dict[str, Dict]:
        """获取所有指数最新数据"""
        conn = self._get_db()
        c = conn.cursor()

        # 获取每个指数的最新数据
        c.execute('''
            SELECT code, name, trade_date, close, change_pct
            FROM index_history
            WHERE (code, trade_date) IN (
                SELECT code, MAX(trade_date) FROM index_history GROUP BY code
            )
        ''')

        result = {}
        for row in c.fetchall():
            result[row[0]] = {
                "code": row[0],
                "name": row[1],
                "date": row[2],
                "close": row[3],
                "change_pct": row[4]
            }

        conn.close()
        return result

    def get_stock_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取股票历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, open, high, low, close, volume, change_pct
            FROM stock_daily
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "change_pct": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_interest_rates(self, days: int = 365) -> List[Dict]:
        """获取利率数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, shibor_overnight, shibor_1w, shibor_1m,
                   hibor_overnight, hibor_1w, hibor_1m
            FROM interest_rates
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "shibor_overnight": row[1],
            "shibor_1w": row[2],
            "shibor_1m": row[3],
            "hibor_overnight": row[4],
            "hibor_1w": row[5],
            "hibor_1m": row[6]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_interest_rates_latest(self) -> Optional[Dict]:
        """获取最新利率"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, shibor_overnight, shibor_1w, shibor_1m,
                   shibor_3m, shibor_6m, shibor_1y,
                   hibor_overnight, hibor_1w, hibor_1m, hibor_3m
            FROM interest_rates ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "shibor": {
                    "overnight": row[1],
                    "1w": row[2],
                    "1m": row[3],
                    "3m": row[4],
                    "6m": row[5],
                    "1y": row[6]
                },
                "hibor": {
                    "overnight": row[7],
                    "1w": row[8],
                    "1m": row[9],
                    "3m": row[10]
                }
            }
        return None

    def get_north_money(self, days: int = 180) -> List[Dict]:
        """获取北向资金数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, sh_net_inflow, sz_net_inflow, total_net_inflow
            FROM north_money
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "sh_inflow": row[1],
            "sz_inflow": row[2],
            "total_inflow": row[3]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_market_sentiment_latest(self) -> Optional[Dict]:
        """获取最新市场情绪"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, up_count, down_count, flat_count, limit_up_count, limit_down_count
            FROM market_sentiment ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "up_count": row[1],
                "down_count": row[2],
                "flat_count": row[3],
                "limit_up_count": row[4],
                "limit_down_count": row[5]
            }
        return None

    def get_watch_list(self) -> List[Dict]:
        """获取关注股票列表"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT code, name, market, category, weight, notes
            FROM watch_list WHERE enabled=1 ORDER BY category, code
        ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "market": row[2],
            "category": row[3],
            "weight": row[4],
            "notes": row[5]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_vix_latest(self) -> Optional[Dict]:
        """获取最新VIX数据"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            SELECT trade_date, vix_open, vix_high, vix_low, vix_close
            FROM vix_history ORDER BY trade_date DESC LIMIT 1
        ''')

        row = c.fetchone()
        conn.close()

        if row:
            return {
                "date": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4]
            }
        return None

    def get_vix_history(self, days: int = 365) -> List[Dict]:
        """获取VIX历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, vix_open, vix_high, vix_low, vix_close
            FROM vix_history
            WHERE trade_date >= ?
            ORDER BY trade_date
        ''', (cutoff,))

        result = [{
            "date": row[0],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_market_overview(self) -> Dict[str, Any]:
        """获取市场概览（整合数据）"""
        # 从数据库获取
        indices = self.get_all_indices_latest()
        rates = self.get_interest_rates_latest()
        sentiment = self.get_market_sentiment_latest()
        vix = self.get_vix_latest()

        # 实时获取股票行情（这个还是需要实时API）
        from app.services.investment_data import InvestmentDataService as RealtimeService
        realtime = RealtimeService()
        watch_stocks = realtime.get_watch_stocks()

        return {
            "update_time": datetime.now().isoformat(),
            "indices": indices,
            "rates": rates,
            "sentiment": sentiment,
            "vix": vix,
            "watch_stocks": watch_stocks
        }

    # ==================== 估值水位模块 ====================

    def get_valuation_latest(self, code: str = None) -> List[Dict]:
        """获取最新估值水位"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, trade_date, pe_ttm, pe_percentile_3y, pe_percentile_5y, pe_percentile_10y,
                       pb, pb_percentile_3y, pb_percentile_5y, pb_percentile_10y, valuation_level
                FROM valuation_bands
                WHERE code = ?
                ORDER BY trade_date DESC LIMIT 1
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, trade_date, pe_ttm, pe_percentile_3y, pe_percentile_5y, pe_percentile_10y,
                       pb, pb_percentile_3y, pb_percentile_5y, pb_percentile_10y, valuation_level
                FROM valuation_bands
                WHERE (code, trade_date) IN (
                    SELECT code, MAX(trade_date) FROM valuation_bands GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "date": row[2],
            "pe_ttm": row[3],
            "pe_percentile_3y": row[4],
            "pe_percentile_5y": row[5],
            "pe_percentile_10y": row[6],
            "pb": row[7],
            "pb_percentile_3y": row[8],
            "pb_percentile_5y": row[9],
            "pb_percentile_10y": row[10],
            "valuation_level": row[11]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_valuation_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取估值历史数据"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, pe_ttm, pe_percentile_5y, pb, pb_percentile_5y
            FROM valuation_bands
            WHERE code = ? AND trade_date >= ?
            ORDER BY trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "pe_ttm": row[1],
            "pe_percentile_5y": row[2],
            "pb": row[3],
            "pb_percentile_5y": row[4]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== 技术指标模块 ====================

    def get_technical_latest(self, code: str = None) -> List[Dict]:
        """获取最新技术指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, trade_date, ma5, ma10, ma20, ma50, ma200,
                       macd, macd_signal, macd_hist, rsi_14, atr_14, atr_pct,
                       beta_1y, trend_signal
                FROM technical_indicators
                WHERE code = ?
                ORDER BY trade_date DESC LIMIT 1
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, trade_date, ma5, ma10, ma20, ma50, ma200,
                       macd, macd_signal, macd_hist, rsi_14, atr_14, atr_pct,
                       beta_1y, trend_signal
                FROM technical_indicators
                WHERE (code, trade_date) IN (
                    SELECT code, MAX(trade_date) FROM technical_indicators GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "date": row[2],
            "ma5": row[3],
            "ma10": row[4],
            "ma20": row[5],
            "ma50": row[6],
            "ma200": row[7],
            "macd": row[8],
            "macd_signal": row[9],
            "macd_hist": row[10],
            "rsi_14": row[11],
            "atr_14": row[12],
            "atr_pct": row[13],
            "beta_1y": row[14],
            "trend_signal": row[15]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_technical_history(self, code: str, days: int = 365) -> List[Dict]:
        """获取技术指标历史"""
        conn = self._get_db()
        c = conn.cursor()

        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        c.execute('''
            SELECT trade_date, close, ma20, ma50, ma200, macd, macd_signal, rsi_14
            FROM technical_indicators t
            JOIN index_history i ON t.code = i.code AND t.trade_date = i.trade_date
            WHERE t.code = ? AND t.trade_date >= ?
            ORDER BY t.trade_date
        ''', (code, cutoff))

        result = [{
            "date": row[0],
            "close": row[1],
            "ma20": row[2],
            "ma50": row[3],
            "ma200": row[4],
            "macd": row[5],
            "macd_signal": row[6],
            "rsi_14": row[7]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== 行业模型模块 ====================

    def get_tmt_metrics(self, code: str = None) -> List[Dict]:
        """获取TMT行业指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, report_date, mau, dau, arpu, arppu, paying_ratio,
                       retention_d1, retention_d7, retention_d30,
                       revenue, revenue_yoy, net_profit, net_profit_yoy,
                       gross_margin, operating_margin, rd_ratio
                FROM sector_tmt
                WHERE code = ?
                ORDER BY report_date DESC
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, report_date, mau, dau, arpu, arppu, paying_ratio,
                       retention_d1, retention_d7, retention_d30,
                       revenue, revenue_yoy, net_profit, net_profit_yoy,
                       gross_margin, operating_margin, rd_ratio
                FROM sector_tmt
                WHERE (code, report_date) IN (
                    SELECT code, MAX(report_date) FROM sector_tmt GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "report_date": row[2],
            "mau": row[3],
            "dau": row[4],
            "arpu": row[5],
            "arppu": row[6],
            "paying_ratio": row[7],
            "retention_d1": row[8],
            "retention_d7": row[9],
            "retention_d30": row[10],
            "revenue": row[11],
            "revenue_yoy": row[12],
            "net_profit": row[13],
            "net_profit_yoy": row[14],
            "gross_margin": row[15],
            "operating_margin": row[16],
            "rd_ratio": row[17]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_biotech_pipeline(self, company_code: str = None, phase: str = None) -> List[Dict]:
        """获取创新药管线"""
        conn = self._get_db()
        c = conn.cursor()

        query = '''
            SELECT company_code, company_name, drug_name, drug_type, indication,
                   phase, phase_cn, start_date, expected_approval, status, region,
                   partner, market_size_est, notes
            FROM sector_biotech WHERE 1=1
        '''
        params = []

        if company_code:
            query += ' AND company_code = ?'
            params.append(company_code)
        if phase:
            query += ' AND phase = ?'
            params.append(phase)

        query += ' ORDER BY company_code, phase'

        c.execute(query, params)

        result = [{
            "company_code": row[0],
            "company_name": row[1],
            "drug_name": row[2],
            "drug_type": row[3],
            "indication": row[4],
            "phase": row[5],
            "phase_cn": row[6],
            "start_date": row[7],
            "expected_approval": row[8],
            "status": row[9],
            "region": row[10],
            "partner": row[11],
            "market_size_est": row[12],
            "notes": row[13]
        } for row in c.fetchall()]

        conn.close()
        return result

    def get_consumer_metrics(self, code: str = None) -> List[Dict]:
        """获取消费行业指标"""
        conn = self._get_db()
        c = conn.cursor()

        if code:
            c.execute('''
                SELECT code, name, report_date, revenue, revenue_yoy, same_store_sales_yoy,
                       store_count, store_change, online_ratio, gross_margin, operating_margin,
                       inventory_turnover, accounts_receivable_days, marketing_ratio,
                       member_count, member_growth_yoy
                FROM sector_consumer
                WHERE code = ?
                ORDER BY report_date DESC
            ''', (code,))
        else:
            c.execute('''
                SELECT code, name, report_date, revenue, revenue_yoy, same_store_sales_yoy,
                       store_count, store_change, online_ratio, gross_margin, operating_margin,
                       inventory_turnover, accounts_receivable_days, marketing_ratio,
                       member_count, member_growth_yoy
                FROM sector_consumer
                WHERE (code, report_date) IN (
                    SELECT code, MAX(report_date) FROM sector_consumer GROUP BY code
                )
            ''')

        result = [{
            "code": row[0],
            "name": row[1],
            "report_date": row[2],
            "revenue": row[3],
            "revenue_yoy": row[4],
            "same_store_sales_yoy": row[5],
            "store_count": row[6],
            "store_change": row[7],
            "online_ratio": row[8],
            "gross_margin": row[9],
            "operating_margin": row[10],
            "inventory_turnover": row[11],
            "accounts_receivable_days": row[12],
            "marketing_ratio": row[13],
            "member_count": row[14],
            "member_growth_yoy": row[15]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== ETL日志模块 ====================

    def log_etl_job(self, job_name: str, job_type: str, start_time: str,
                    status: str, records_processed: int = 0, records_failed: int = 0,
                    error_message: str = None, details: str = None) -> int:
        """记录ETL任务日志"""
        conn = self._get_db()
        c = conn.cursor()

        c.execute('''
            INSERT INTO etl_logs (job_name, job_type, start_time, status,
                                  records_processed, records_failed, error_message, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_name, job_type, start_time, status, records_processed,
              records_failed, error_message, details))

        log_id = c.lastrowid
        conn.commit()
        conn.close()
        return log_id

    def update_etl_job(self, log_id: int, end_time: str, status: str,
                       records_processed: int = None, records_failed: int = None,
                       error_message: str = None):
        """更新ETL任务日志"""
        conn = self._get_db()
        c = conn.cursor()

        updates = ["end_time = ?", "status = ?"]
        params = [end_time, status]

        if records_processed is not None:
            updates.append("records_processed = ?")
            params.append(records_processed)
        if records_failed is not None:
            updates.append("records_failed = ?")
            params.append(records_failed)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)

        params.append(log_id)

        c.execute(f'''
            UPDATE etl_logs SET {', '.join(updates)} WHERE id = ?
        ''', params)

        conn.commit()
        conn.close()

    def get_etl_logs(self, limit: int = 50, job_type: str = None) -> List[Dict]:
        """获取ETL日志"""
        conn = self._get_db()
        c = conn.cursor()

        if job_type:
            c.execute('''
                SELECT id, job_name, job_type, start_time, end_time, status,
                       records_processed, records_failed, error_message
                FROM etl_logs WHERE job_type = ?
                ORDER BY start_time DESC LIMIT ?
            ''', (job_type, limit))
        else:
            c.execute('''
                SELECT id, job_name, job_type, start_time, end_time, status,
                       records_processed, records_failed, error_message
                FROM etl_logs
                ORDER BY start_time DESC LIMIT ?
            ''', (limit,))

        result = [{
            "id": row[0],
            "job_name": row[1],
            "job_type": row[2],
            "start_time": row[3],
            "end_time": row[4],
            "status": row[5],
            "records_processed": row[6],
            "records_failed": row[7],
            "error_message": row[8]
        } for row in c.fetchall()]

        conn.close()
        return result

    # ==================== CSV导入功能 ====================

    def import_csv_to_table(self, table_name: str, csv_path: str) -> Dict:
        """从CSV导入数据到指定表"""
        import csv

        if not os.path.exists(csv_path):
            return {"status": "error", "message": f"文件不存在: {csv_path}"}

        conn = self._get_db()
        c = conn.cursor()

        # 获取表结构
        c.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in c.fetchall() if col[1] != 'id' and col[1] != 'created_at']

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                # 检查CSV列是否匹配
                csv_columns = reader.fieldnames
                common_columns = [col for col in columns if col in csv_columns]

                if not common_columns:
                    conn.close()
                    return {"status": "error", "message": "CSV列与表结构不匹配"}

                imported = 0
                for row in reader:
                    values = [row.get(col) for col in common_columns]
                    placeholders = ', '.join(['?' for _ in common_columns])
                    col_names = ', '.join(common_columns)

                    try:
                        c.execute(f'''
                            INSERT OR REPLACE INTO {table_name} ({col_names})
                            VALUES ({placeholders})
                        ''', values)
                        imported += 1
                    except Exception as e:
                        print(f"导入行失败: {e}")
                        continue

                conn.commit()
                conn.close()
                return {"status": "success", "imported": imported, "table": table_name}

        except Exception as e:
            conn.close()
            return {"status": "error", "message": str(e)}


# 测试
if __name__ == "__main__":
    service = InvestmentDataService()

    print("=== 投资数据服务测试 ===\n")

    # 测试指数
    print("指数列表:")
    for idx in service.get_index_list():
        latest = service.get_index_latest(idx['code'])
        if latest:
            print(f"  {latest['name']}: {latest['close']} ({latest['date']})")

    # 测试利率
    print("\n最新利率:")
    rates = service.get_interest_rates_latest()
    if rates:
        print(f"  SHIBOR隔夜: {rates['shibor']['overnight']}")
        print(f"  HIBOR隔夜: {rates['hibor']['overnight']}")

    # 测试市场情绪
    print("\n市场情绪:")
    sentiment = service.get_market_sentiment_latest()
    if sentiment:
        print(f"  上涨: {sentiment['up_count']}, 下跌: {sentiment['down_count']}")