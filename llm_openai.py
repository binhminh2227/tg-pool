# llm_openai.py
import os, json, asyncio, aiohttp
from typing import Dict, Any, Tuple, List

DEFAULT_SYS_PROMPT = (
    "You are a writing assistant. "
    "Rewrite the input post to be clearer and more concise, but KEEP the original language (do NOT translate). "
    "Preserve the original layout and line breaks (headings, bullet points, paragraph breaks). "
    "Remove ALL URLs. Do NOT include any source captions, credits, signatures, calls-to-action, "
    "or promotional tags. Output ONLY the cleaned content. No extra commentary. No emojis."
)

def _mk_msgs(system_prompt: str, text: str) -> List[Dict[str, str]]:
    return [
        {"role": "system", "content": system_prompt.strip()},
        {"role": "user",   "content": f"POST CONTENT:\n{text or '(empty)'}"}
    ]

class OpenAIKeys:
    """Quản lý danh sách key OpenAI + round-robin cursor."""
    def __init__(self, keys_csv: str):
        self.keys: List[str] = [k.strip() for k in (keys_csv or "").split(",") if k.strip()]
        self._cursor = 0

    def has_keys(self) -> bool:
        return len(self.keys) > 0

    def seq_for_round(self, round_idx: int) -> List[str]:
        """Mỗi vòng bắt đầu ở vị trí cursor + round_idx để phân tán key."""
        if not self.keys: return []
        start = (self._cursor + round_idx) % len(self.keys)
        return self.keys[start:] + self.keys[:start]

    def advance_on_success(self):
        if self.keys:
            self._cursor = (self._cursor + 1) % len(self.keys)

class LLMOpenAI:
    """
    OpenAI-only, multi-key router + retry + delay + backoff.
    - Thử lần lượt các key trong 1 vòng; nếu thất bại → đợi rồi sang vòng kế với offset khác.
    - Thành công → advance cursor để phân tán tải.
    """
    def __init__(self):
        self.base = (os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.keys = OpenAIKeys(os.getenv("OPENAI_KEYS", ""))

        self.retry_attempts = max(1, int(os.getenv("OPENAI_RETRY_ATTEMPTS", "3")))
        self.retry_delay_ms = max(0, int(os.getenv("OPENAI_RETRY_DELAY_MS", "400")))
        try:
            self.backoff = float(os.getenv("OPENAI_BACKOFF_FACTOR", "2.0"))
        except Exception:
            self.backoff = 2.0

    async def rewrite(self, raw_text: str, user_prompt: str = "", sys_extra: str = "") -> Tuple[str, Dict[str, Any]]:
        system_prompt = "\n\n".join([p for p in [DEFAULT_SYS_PROMPT, sys_extra.strip(), user_prompt.strip()] if p]).strip()
        msgs = _mk_msgs(system_prompt, raw_text)

        tried: List[Dict[str, Any]] = []
        if not self.keys.has_keys():
            return raw_text, {"ok": False, "status": 0, "error": "No OPENAI_KEYS", "provider": "openai", "tried": tried}

        delay = self.retry_delay_ms / 1000.0
        for round_idx in range(self.retry_attempts):
            seq = self.keys.seq_for_round(round_idx)
            for ki, key in enumerate(seq):
                ok, status, text, err = await self._call_one_key(key, msgs)
                tried.append({"provider": "openai", "key_index": (round_idx, ki), "ok": ok, "status": status, "error": err})
                if ok:
                    # Thành công → tiến cursor để cân bằng
                    self.keys.advance_on_success()
                    return text, {"ok": True, "status": status, "error": None, "provider": "openai", "tried": tried}
                # lỗi → thử key kế tiếp trong cùng vòng
                await asyncio.sleep(0.15)
            # hết vòng: delay rồi backoff
            if round_idx < self.retry_attempts - 1:
                await asyncio.sleep(delay)
                delay *= self.backoff

        # thất bại hết
        return raw_text, {"ok": False, "status": 0, "error": "All keys failed", "provider": "openai", "tried": tried}

    async def _call_one_key(self, key: str, messages: List[Dict[str, str]]) -> Tuple[bool, int, str, str]:
        url = f"{self.base}/chat/completions"
        body = {"model": self.model, "temperature": 0.3, "messages": messages}
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=body, headers=headers, timeout=60) as r:
                    status = r.status
                    data = await r.json(content_type=None)
                    if 200 <= status < 300:
                        text = (data.get("choices", [{}])[0].get("message", {}).get("content") or "").trim()
                        text = text if isinstance(text, str) else (data.get("choices", [{}])[0].get("message", {}).get("content") or "")
                        text = (text or "").strip()
                        return (bool(text), status, text or "", None if text else "Empty text")
                    err_obj = data.get("error", {})
                    err_msg = err_obj.get("message") if isinstance(err_obj, dict) else (err_obj or data)
                    return (False, status, "", str(err_msg))
        except Exception as e:
            return (False, 0, "", str(e))
