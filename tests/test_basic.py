import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import init_db, create_product, get_product, get_conn, system_status, create_task, execute_task, listing, TaskCreate, ListingRequest, export_product_prices


def test_init_db_and_create_product():
    init_db()
    pid = create_product("测试商品", "http://example.com/p1", "测试")
    p = get_product(pid)
    assert p is not None
    assert p["name"] == "测试商品"
    assert p["url"].startswith("http")


def test_system_status_envelope():
    res = system_status()
    assert isinstance(res, dict)
    assert res["success"] is True
    assert res["data"]["health"] == "ok"


def test_task_execute_creates_price():
    init_db()
    pid = create_product("商品A", "http://example.com/a", "类目")
    body = TaskCreate(product_id=pid)
    created = create_task(body)
    tid = created["data"]["id"]
    executed = execute_task(tid)
    assert executed["success"] is True
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM prices WHERE product_id = ?", (pid,))
    cnt = cur.fetchone()[0]
    conn.close()
    assert cnt >= 1


def test_listing_validation():
    bad = listing(ListingRequest(url="not-a-url", max_items=10))
    assert bad["success"] is False
    assert bad["error"]["code"] == "VALIDATION_ERROR"


def test_export_csv_contains_header():
    init_db()
    pid = create_product("商品B", "http://example.com/b", "类目")
    body = TaskCreate(product_id=pid)
    created = create_task(body)
    tid = created["data"]["id"]
    execute_task(tid)
    resp = export_product_prices(pid)
    assert getattr(resp, "media_type", None) == "text/csv"
    filename = resp.headers.get("content-disposition", "")
    assert "product_" in filename
