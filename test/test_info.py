import re


def test_user_agent() -> None:
    from denokv import info

    assert re.match(r"^denokv-python/\d+\.\d+\.\d+.*", info.user_agent)
