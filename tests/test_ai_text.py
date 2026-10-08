"""Answer extraction must skip thinking blocks and surface refusals / truncation."""
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.services.ai_text import AIResponseError, response_text  # noqa: E402


def msg(blocks, stop="end_turn", details=None):
    return NS(content=blocks, stop_reason=stop, stop_details=details)


def test_skips_thinking_blocks():
    m = msg([NS(type="thinking", thinking=""), NS(type="text", text='{"a": 1}')])
    assert response_text(m) == '{"a": 1}'


def test_refusal_and_truncation_and_empty_raise():
    with pytest.raises(AIResponseError, match="declined"):
        response_text(msg([], stop="refusal", details=NS(category="cyber")))
    with pytest.raises(AIResponseError, match="max_tokens"):
        response_text(msg([NS(type="text", text='{"a"')], stop="max_tokens"))
    with pytest.raises(AIResponseError, match="no text"):
        response_text(msg([NS(type="thinking", thinking="")]))
