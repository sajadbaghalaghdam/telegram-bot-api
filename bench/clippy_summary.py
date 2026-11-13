#!/usr/bin/env python3
"""Counts clippy/rustc warnings by lint from the raw stderr of `cargo clippy --all-targets`."""
import collections
import re
import sys

txt = open(sys.argv[1]).read()
# Each diagnostic: "warning: <msg>\n  --> file" ... later "= note: `#[warn(lint)]` on by default"
# (only the first occurrence of a lint carries that note), so count by header and map lint names
# via the note where present.
blocks = re.split(r"\n(?=warning|error)", txt)
lints = collections.Counter()
total = 0
for b in blocks:
    if not (b.startswith("warning:") or b.startswith("error:")):
        continue
    if "generated" in b.split("\n", 1)[0] and "warning" in b.split("\n", 1)[0]:
        continue  # "warning: `crate` (bin) generated N warnings" summary lines
    total += 1
    m = re.search(r"clippy::[a-z_0-9]+|#\[warn\((\w+)\)\]", b)
    link = re.search(r"rust-clippy/[^#]*#([a-z_0-9]+)", b)
    if link:
        lints["clippy::" + link.group(1)] += 1
    elif m:
        lints[m.group(0)] += 1
    else:
        lints["(other: " + b.split("\n", 1)[0][:60] + ")"] += 1
print(f"warnings_total {total}")
for k, v in lints.most_common():
    print(f"{k} {v}")
