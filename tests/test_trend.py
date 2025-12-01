import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import init_db, create_product, get_conn, product_trend


def test_product_trend_daily_aggregate():
    init_db()
    pid = create_product("趋势商品", "http://example.com/t", "类目")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 10.0, "2024-02-01T00:00:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 12.0, "2024-02-01T12:00:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 20.0, "2024-02-02T08:00:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 18.0, "2024-02-02T20:00:00Z"))
    conn.commit()
    conn.close()

    resp = product_trend(pid)
    assert resp["success"] is True
    series = resp["data"]["series"]
    assert len(series) == 2
    d1 = next(s for s in series if s["date"] == "2024-02-01")
    d2 = next(s for s in series if s["date"] == "2024-02-02")
    assert d1["open"] == 10.0 and d1["close"] == 12.0 and d1["low"] == 10.0 and d1["high"] == 12.0
    assert d2["open"] == 20.0 and d2["close"] == 18.0 and d2["low"] == 18.0 and d2["high"] == 20.0
