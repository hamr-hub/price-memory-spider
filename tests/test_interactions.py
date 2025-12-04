import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import (
    init_db,
    create_product,
    get_conn,
    add_follow,
    remove_follow,
    FollowCreate,
    create_user,
    UserCreate,
    create_push,
    update_push_status,
    PushCreate,
    PushUpdate,
    export_products,
)


def test_add_and_remove_follow_flow():
    init_db()
    uid = create_user(UserCreate(username="u_follow_test", display_name="F"))["data"]["id"]
    pid = create_product("被关注商品", "http://example.com/follow", "类目")
    resp_add = add_follow(uid, FollowCreate(product_id=pid))
    assert resp_add["success"] is True
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM user_follows WHERE user_id = ? AND product_id = ?", (uid, pid))
    assert cur.fetchone()[0] == 1
    conn.close()
    resp_rm = remove_follow(uid, pid)
    assert resp_rm["success"] is True
    conn2 = get_conn()
    cur2 = conn2.cursor()
    cur2.execute("SELECT COUNT(*) FROM user_follows WHERE user_id = ? AND product_id = ?", (uid, pid))
    assert cur2.fetchone()[0] == 0
    conn2.close()


def test_push_status_update_flow():
    init_db()
    uid_sender = create_user(UserCreate(username="u_push_sender", display_name="S"))["data"]["id"]
    uid_rec = create_user(UserCreate(username="u_push_rec", display_name="R"))["data"]["id"]
    pid = create_product("推送商品", "http://example.com/push", "类目")
    created = create_push(uid_sender, PushCreate(recipient_id=uid_rec, product_id=pid, message="hi"))
    push_id = created["data"]["id"]
    updated = update_push_status(push_id, PushUpdate(status="accepted"))
    assert updated["success"] is True
    assert updated["data"]["status"] == "accepted"


def test_export_quota_enforcement():
    init_db()
    u = create_user(UserCreate(username="u_quota", display_name="Q"))["data"]
    uid = u["id"]
    api_key = u["api_key"]
    pid1 = create_product("导出商品1", "http://example.com/e1", "类目")
    pid2 = create_product("导出商品2", "http://example.com/e2", "类目")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid1, 9.99, "2024-02-01T00:00:00Z"))
    cur.execute("INSERT INTO prices(product_id, price, created_at) VALUES(?, ?, ?)", (pid2, 19.99, "2024-02-01T00:00:00Z"))
    conn.commit()
    conn.close()
    for _ in range(5):
        resp = export_products(f"{pid1},{pid2}", api_key=api_key)
        assert getattr(resp, "media_type", None) == "text/csv"
    exceeded = export_products(f"{pid1},{pid2}", api_key=api_key)
    assert exceeded["success"] is False
    assert exceeded["error"]["code"] == "QUOTA_EXCEEDED"
