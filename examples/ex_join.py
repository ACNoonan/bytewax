from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.visualize import to_mermaid

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.examples.ex_join

flow = Dataflow("join")

src_1 = [
    {"user_id": "123", "name": "Bumble"},
]
inp1 = op.input("inp1", flow, TestingSource(src_1))
keyed_inp1 = op.key_on("key_stream_1", inp1, lambda x: x["user_id"])

src_2 = [
    {"user_id": "123", "email": "Bee@bytewax.com"},
    {"user_id": "456", "email": "Hive@bytewax.com"},
]
inp2 = op.input("inp2", flow, TestingSource(src_2))
keyed_inp2 = op.key_on("key_stream_2", inp2, lambda x: x["user_id"])

merged_stream = op.join("join", keyed_inp1, keyed_inp2)
op.inspect("debug", merged_stream)

mermaid_diagram = to_mermaid(flow)
print(mermaid_diagram)
