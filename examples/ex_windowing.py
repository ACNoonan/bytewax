from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.windowing import EventClock, TumblingWindower, WindowMetadata
from bytewax.testing import TestingSource

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.examples.ex_windowing

flow = Dataflow("windowing")

align_to = datetime(2025, 1, 1, tzinfo=timezone.utc)
inp = [
    {"time": align_to, "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=5), "user": "b", "val": 1},
    {"time": align_to + timedelta(seconds=8), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=14), "user": "b", "val": 1},
]
stream = op.input("input", flow, TestingSource(inp))
keyed_stream = op.key_on("key_on_user", stream, lambda e: e["user"])

clock = EventClock(lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0))
windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

win_out = win.collect_window("add", keyed_stream, clock, windower)

op.output("out", win_out.down, StdOutSink())