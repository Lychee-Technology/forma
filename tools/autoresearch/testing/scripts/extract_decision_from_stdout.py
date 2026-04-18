#!/usr/bin/env python3

import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        return 1

    stdout_path = Path(sys.argv[1])
    decision_path = Path(sys.argv[2])

    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    begin = "AUTORESEARCH_DECISION_BEGIN"
    end = "AUTORESEARCH_DECISION_END"

    start = text.rfind(begin)
    if start == -1:
        return 1
    start += len(begin)

    finish = text.find(end, start)
    if finish == -1:
        return 1

    required_fields = {
        "status",
        "reason",
        "scenario",
        "description",
        "evidence",
    }
    lines = []
    seen_fields = set()
    for raw in text[start:finish].splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        lines.append(line)
        key = line.split("=", 1)[0].strip()
        if key:
            seen_fields.add(key)

    if not required_fields.issubset(seen_fields):
        return 1

    decision_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
