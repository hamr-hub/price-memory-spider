import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import init_db, create_product, get_conn, product_trend


def test_product_trend_hourly_aggregate():
    init_db()
    pid = create_product("小时趋势商品", "http://example.com/h", "类目")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 10.0, "2024-02-01T08:00:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 12.0, "2024-02-01T08:30:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 20.0, "2024-02-01T09:10:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 18.0, "2024-02-01T09:50:00Z"))
    conn.commit()
    conn.close()

    resp = product_trend(pid, granularity="hourly")
    assert resp["success"] is True
    series = resp["data"]["series"]
    assert len(series) == 2
    h8 = next(s for s in series if s["date"] == "2024-02-01T08")
    h9 = next(s for s in series if s["date"] == "2024-02-01T09")
    assert h8["open"] == 10.0 and h8["close"] == 12.0 and h8["low"] == 10.0 and h8["high"] == 12.0
    assert h9["open"] == 20.0 and h9["close"] == 18.0 and h9["low"] == 18.0 and h9["high"] == 20.0
