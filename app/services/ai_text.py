"""Read the answer out of a Claude Messages response.

Newer models (e.g. Sonnet 5.5, Opus 5.5) think by default, so `content` starts
with `thinking` blocks; the answer is in the `text` blocks. Reading
`content[0].text` raises on those models.
"""

from typing import Any


class AIResponseError(Exception):
    """The response has no usable answer (refusal, truncation, or no text)."""


def response_text(message: Any) -> str:
    stop = getattr(message, "stop_reason", None)
    if stop == "refusal":
        details = getattr(message, "stop_details", None)
        category = getattr(details, "category", None) if details else None
        raise AIResponseError(f"model declined the request ({category or 'no category'})")
    text = "".join(getattr(b, "text", "") for b in (message.content or []) if getattr(b, "type", None) == "text")
    if stop == "max_tokens":
        raise AIResponseError("response hit max_tokens before finishing")
    if not text.strip():
        raise AIResponseError(f"no text in response (stop_reason={stop})")
    return text
