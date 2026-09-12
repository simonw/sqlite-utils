import io

import pytest

from sqlite_utils.utils import UpdateWrapper


@pytest.mark.parametrize("encoding", ("utf-8", "utf-16-le", "utf-32-le"))
def test_update_wrapper_tracks_underlying_bytes(encoding):
    text = "id,name\r\n1,Café\r\n2,猫\r\n"
    raw = text.encode(encoding)
    updates = []
    decoded = io.TextIOWrapper(io.BytesIO(raw), encoding=encoding)

    lines = list(UpdateWrapper(decoded, updates.append))

    assert lines == ["id,name\n", "1,Café\n", "2,猫\n"]
    assert sum(updates) == len(raw)


def test_update_wrapper_read_tracks_underlying_bytes():
    raw = "é猫".encode("utf-8")
    updates = []
    decoded = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8")
    wrapped = UpdateWrapper(decoded, updates.append)

    assert wrapped.read(1) == "é"
    assert wrapped.read() == "猫"
    assert sum(updates) == len(raw)
