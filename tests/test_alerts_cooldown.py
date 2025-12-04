import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from spider.main import init_db, create_product, get_conn, create_user, UserCreate, create_alert, AlertCreate, create_task, execute_task, TaskCreate, list_alert_events


def test_alert_cooldown_prevents_duplicate_events():
    init_db()
    uid = create_user(UserCreate(username="cooldown_user", display_name="U"))["data"]["id"]
    pid = create_product("冷却商品", "http://example.com/cd", "类目")
    # cooldown 60 minutes default
    create_alert(AlertCreate(user_id=uid, product_id=pid, rule_type="price_lte", threshold=10000.0))
    tid1 = create_task(TaskCreate(product_id=pid))["data"]["id"]
    tid2 = create_task(TaskCreate(product_id=pid))["data"]["id"]
    execute_task(tid1)
    execute_task(tid2)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM alert_events WHERE user_id = ? AND product_id = ?", (uid, pid))
    cnt = cur.fetchone()[0]
    conn.close()
    assert cnt == 1
