"""Per-file stripe layout (the Lustre LOV analogue) for the POSIX path.

A layout is chosen once, when the file is created, and stored inside the
file's MDT entry (fs/mdt_node.py) -- it is metadata, so it lives with the
rest of the file's metadata, not on OST[0] the way fs/striping.py's
MPI-IO path keeps its `.lov` object.

`stripe_offset` is derived from the path so different files start on
different OSTs (Lustre's allocator does the same balancing); without it
every file's stripe 0 -- and every small file, which is only stripe 0 --
would pile onto OST 0.
"""
from __future__ import annotations

import hashlib

from fs.striping import DEFAULT_STRIPE_SIZE, StripeLayout


def choose_layout(path: str, num_osts: int, *, stripe_count: int | None = None,
                  stripe_size: int = DEFAULT_STRIPE_SIZE) -> StripeLayout:
    count = num_osts if stripe_count is None else max(1, min(stripe_count, num_osts))
    offset = int.from_bytes(hashlib.sha256(path.encode()).digest()[:8], "big") % num_osts
    return StripeLayout(stripe_count=count, stripe_size=stripe_size, stripe_offset=offset)


def layout_to_dict(layout: StripeLayout) -> dict:
    return {"stripe_count": layout.stripe_count, "stripe_size": layout.stripe_size,
            "stripe_offset": layout.stripe_offset}


def layout_from_dict(d: dict) -> StripeLayout:
    return StripeLayout(stripe_count=d["stripe_count"], stripe_size=d["stripe_size"],
                        stripe_offset=d["stripe_offset"])
