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

    lines = []
    for raw in text[start:finish].splitlines():
        line = raw.strip()
        if not line or "=" not in line:
            continue
        lines.append(line)

    if not any(line.startswith("status=") for line in lines):
        return 1

    decision_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
