import os
import tempfile
import shutil
import pytest
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
