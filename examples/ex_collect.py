from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import TestingSource

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.examples.collect_example

flow = Dataflow("collect")
stream = op.input("input", flow, TestingSource(list(range(10))))

# We want to consider all the items together, so we assign the same fixed key to each of them.
keyed_stream = op.key_on("key", stream, lambda _x: "ALL")
collected_stream = op.collect(
    "collect", keyed_stream, timeout=timedelta(seconds=10), max_size=3
)
op.output("out", collected_stream, StdOutSink())