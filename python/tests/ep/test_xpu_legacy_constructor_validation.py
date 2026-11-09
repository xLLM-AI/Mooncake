"""Validate legacy XPU arguments before the constructor touches a SYCL queue."""

import pytest


ep = pytest.importorskip("mooncake.ep", reason="native EP module is not built")
pytestmark = pytest.mark.skipif(
    getattr(ep, "DEVICE_TYPE", None) != "xpu", reason="requires the XPU EP build"
)


@pytest.mark.parametrize(
    ("rank", "world", "size", "message"),
    [
        (-1, 1, 4096, "invalid EP rank or world size"),
        (1, 1, 4096, "invalid EP rank or world size"),
        (0, 0, 4096, "invalid EP rank or world size"),
        (0, 1009, 4096, "invalid EP rank or world size"),
        (0, 1, 0, "EP buffer size must be positive"),
        (0, 1, -1, "EP buffer size must be positive"),
    ],
)
def test_invalid_argument_precedes_missing_queue(rank, world, size, message):
    with pytest.raises(ValueError, match=message):
        ep.Buffer(rank, world, size, False, 0)


def test_original_1008_rank_limit_still_reaches_queue_validation():
    with pytest.raises(RuntimeError, match="a torch.xpu stream"):
        ep.Buffer(1007, 1008, 4096, False, 0)
