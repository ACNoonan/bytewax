import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.examples.ex_basic


# Create a Dataflow instance in a variable named flow - this defines the empty dataflow
flow = Dataflow("basic")

# Every stream needs input - we're using input operator with a TestingSource to produce a stream of ints
stream = op.input("input", flow, TestingSource(range(21)))

# Each Operator method will return a new Stream with the results of the step which you can call more operators on.
def times_two(inp: int) -> int:
    return inp * 2

# We'll use the Map operator to apply the times_two function to each element in the stream
double = op.map("double", stream, times_two)

op.output("out", double, StdOutSink())