import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import secrets
from spider.main import init_db, create_product, get_conn, create_user, UserCreate, create_alert, AlertCreate, create_task, execute_task, TaskCreate


def test_alert_triggers_push_on_price_below_threshold():
    init_db()
    uname = "u_alert_" + secrets.token_hex(4)
    uid = create_user(UserCreate(username=uname, display_name="U"))["data"]["id"]
    pid = create_product("告警商品", "http://example.com/alert", "类目")
    create_alert(AlertCreate(user_id=uid, product_id=pid, rule_type="price_lte", threshold=1000.0, percent=None))
    tid = create_task(TaskCreate(product_id=pid))["data"]["id"]
    execute_task(tid)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pushes WHERE recipient_id = ? AND product_id = ?", (uid, pid))
    cnt = cur.fetchone()[0]
    conn.close()
    assert cnt >= 1
