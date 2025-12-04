import os
import sys
import tempfile
import shutil
import pytest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import spider.main as main


@pytest.fixture(autouse=True)
def temp_db(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="spiderdb_")
    db_path = os.path.join(tmpdir, "spider.db")
    monkeypatch.setattr(main, "DB_PATH", db_path, raising=False)
    yield
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except Exception:
        pass
    shutil.rmtree(tmpdir, ignore_errors=True)
