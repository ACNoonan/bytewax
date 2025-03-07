import operator 
import re

from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow 
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

# Run command (From Documents): 
# python -m bytewax.run projects.bytewax.examples.ex_wordcount

flow = Dataflow("wordcount")
inp = op.input("input", flow, FileSource("projects/bytewax/tutorials/wordcount.txt"))

def lower(line):
    return line.lower()

lowers = op.map("lowercase_words", inp, lower)

def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)

tokens = op.flat_map("tokenize_input", lowers, tokenize)

counts = op.count_final("count", tokens, lambda word: word)

op.output("out", counts, StdOutSink())