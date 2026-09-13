"""Read CLI lifecycle events without depending on human-readable log messages."""

import json


def has_event(lines, expected):
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            # Argument and subscriber setup failures can precede JSON logging.
            continue
        if (
            isinstance(record, dict)
            and record.get("target") == "kotoconn"
            and record.get("fields", {}).get("event") == expected
        ):
            return True
    return False
