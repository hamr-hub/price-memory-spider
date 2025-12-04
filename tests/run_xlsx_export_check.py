import os, sys
CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(os.path.join(CURRENT_DIR, ".."))
from main import create_product, create_task, execute_task, export_products_xlsx, TaskCreate

def main():
    pid1 = create_product("Excel商品1", "http://example.com/e1", "Excel")
    pid2 = create_product("Excel商品2", "http://example.com/e2", "Excel")
    t1 = create_task(TaskCreate(product_id=pid1))["data"]["id"]
    t2 = create_task(TaskCreate(product_id=pid2))["data"]["id"]
    execute_task(t1)
    execute_task(t2)
    resp = export_products_xlsx(f"{pid1},{pid2}")
    print(resp.media_type)
    print(resp.headers.get("content-disposition"))
    body = getattr(resp, "body", None) or resp.body
    print(len(body) if isinstance(body, (bytes, bytearray)) else len(body.encode("utf-8")))

if __name__ == "__main__":
    main()
