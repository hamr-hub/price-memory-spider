import json
from fastapi.testclient import TestClient
from spider.main import app

client = TestClient(app)

def gql(query: str, variables: dict = None):
    resp = client.post("/api/v1/graphql", json={"query": query, "variables": variables or {}})
    assert resp.status_code == 200
    data = resp.json().get("data")
    assert data is not None
    return data

def test_graphql_products_crud():
    create_q = "mutation($input:ProductInput){ createProduct(input:$input){ id name url category } }"
    p = gql(create_q, {"input": {"name": "GQL商品", "url": "http://example.com/x", "category": "测试"}})["createProduct"]
    assert p["name"] == "GQL商品"
    pid = int(p["id"])

    one_q = "query($id:Int!){ product(id:$id){ id name url category } }"
    got = gql(one_q, {"id": pid})["product"]
    assert got["id"] == pid

    upd_q = "mutation($id:Int!,$input:ProductUpdate){ updateProduct(id:$id,input:$input){ id name url category } }"
    up = gql(upd_q, {"id": pid, "input": {"name": "更新后"}})["updateProduct"]
    assert up["name"] == "更新后"

    del_q = "mutation($id:Int!){ deleteProduct(id:$id){ id } }"
    ret = gql(del_q, {"id": pid})["deleteProduct"]
    assert int(ret["id"]) == pid
