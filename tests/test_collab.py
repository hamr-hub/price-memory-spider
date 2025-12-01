import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import init_db, create_product, get_conn
from spider.main import add_product_to_public_pool, list_public_pool_products, user_select_from_pool
from spider.main import create_collection, add_collection_product, collection_detail, export_collection_xlsx
from spider.main import CollectionCreate, PoolAddProduct, SelectFromPoolBody, CollectionAddProduct


def test_public_pool_flow():
    init_db()
    pid = create_product("池商品", "http://example.com/pool", "类目")
    resp_add = add_product_to_public_pool(PoolAddProduct(product_id=pid))
    assert resp_add["success"] is True
    resp_list = list_public_pool_products(page=1, size=10)
    items = resp_list["data"]["items"]
    assert any(i["id"] == pid for i in items)


def test_select_from_pool_creates_follow():
    init_db()
    # create user
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO users(username, display_name, created_at) VALUES(?, ?, ?)", ("u1", "用户1", "2024-01-01T00:00:00Z"))
    conn.commit()
    uid = cur.lastrowid
    conn.close()
    pid = create_product("池商品2", "http://example.com/pool2", "类目")
    add_product_to_public_pool(PoolAddProduct(product_id=pid))
    resp = user_select_from_pool(uid, SelectFromPoolBody(product_id=pid))
    assert resp["success"] is True
    conn2 = get_conn()
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM user_follows WHERE user_id = ? AND product_id = ?", (uid, pid))
    cnt = cur2.fetchone()[0]
    conn2.close()
    assert cnt == 1


def test_collection_create_and_add_product_and_export():
    init_db()
    # create owner user
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO users(username, display_name, created_at) VALUES(?, ?, ?)", ("owner", "拥有者", "2024-01-01T00:00:00Z"))
    conn.commit()
    uid = cur.lastrowid
    conn.close()
    # create product and price
    pid = create_product("集合商品", "http://example.com/coll", "类目")
    conn3 = get_conn()
    cur3 = conn3.cursor()
    cur3.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid, 12.34, "2024-02-01T00:00:00Z"))
    conn3.commit()
    conn3.close()
    # create collection
    created = create_collection(CollectionCreate(name="测试集合", owner_user_id=uid))
    cid = created["data"]["id"]
    # add product
    add_collection_product(cid, CollectionAddProduct(product_id=pid))
    detail = collection_detail(cid)
    assert any(p["id"] == pid for p in detail["data"]["products"]) 
    # export xlsx if available
    try:
        import openpyxl  # noqa: F401
        resp = export_collection_xlsx(cid)
        assert getattr(resp, "media_type", None) == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    except Exception:
        resp = export_collection_xlsx(cid)
        assert resp["success"] is False
        assert resp["error"]["code"] == "DEPENDENCY_MISSING"

