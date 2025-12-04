import os
from typing import List, Optional, Tuple

def _target_dims() -> Tuple[int, int]:
    td = int(os.environ.get("AI_EMBED_DIM_TEXT", "1024") or 1024)
    idim = int(os.environ.get("AI_EMBED_DIM_IMAGE", "1024") or 1024)
    return td, idim

def _pad_or_truncate(vec: List[float], dim: int) -> List[float]:
    if len(vec) == dim:
        return vec
    if len(vec) > dim:
        return vec[:dim]
    return vec + [0.0] * (dim - len(vec))

def embed_text(text: str) -> Optional[List[float]]:
    api_key = os.environ.get("DASHSCOPE_API_KEY")
    if not api_key or not text or not text.strip():
        return None
    # Try OpenAI-compatible client first
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
        model = os.environ.get("AI_EMBED_TEXT_MODEL", "text-embedding-v4")
        resp = client.embeddings.create(model=model, input=text)
        vec = resp.data[0].embedding if getattr(resp, "data", None) else None
        if not vec:
            return None
        dim_text, _ = _target_dims()
        return _pad_or_truncate(list(vec), dim_text)
    except Exception:
        pass
    # Fallback: DashScope SDK
    try:
        import dashscope
        model = os.environ.get("AI_EMBED_TEXT_MODEL", "text-embedding-v4")
        resp = dashscope.TextEmbedding.call(model=model, input=text)
        data = getattr(resp, "output", None)
        if isinstance(data, dict):
            items = (data.get("embeddings") or [])
            vec = (items[0] or {}).get("embedding") if items else None
        else:
            vec = None
        if not vec:
            return None
        dim_text, _ = _target_dims()
        return _pad_or_truncate(list(vec), dim_text)
    except Exception:
        return None

def embed_image(image_url: str) -> Optional[List[float]]:
    api_key = os.environ.get("DASHSCOPE_API_KEY")
    if not api_key or not image_url or not str(image_url).strip():
        return None
    # DashScope MultiModalEmbedding API (recommended for image)
    try:
        import dashscope
        resp = dashscope.MultiModalEmbedding.call(model="multimodal-embedding-v1", input=[{"image": image_url}])
        data = getattr(resp, "output", None)
        # Expected structure: { "embeddings": [ { "embedding": [...], ... } ] }
        items = (data.get("embeddings") or []) if isinstance(data, dict) else []
        vec = (items[0] or {}).get("embedding") if items else None
        if not vec:
            return None
        _, dim_image = _target_dims()
        return _pad_or_truncate(list(vec), dim_image)
    except Exception:
        return None

