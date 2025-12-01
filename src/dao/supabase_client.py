import os
from typing import Optional
from supabase import create_client, Client

def get_client() -> Optional[Client]:
    url: Optional[str] = os.environ.get("SUPABASE_URL")
    key: Optional[str] = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)
