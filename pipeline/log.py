import json
import sys
from datetime import datetime

def log(level, event, **details):
    record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "event": event,
        **details
    }
    output = json.dumps(record)
    if level == "ERROR":
        print(output, file=sys.stderr)
    else:
        print(output)
