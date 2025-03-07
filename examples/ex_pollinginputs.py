import logging
import requests

from typing import Optional
from datetime import timedelta
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.examples.ex_pollinginputs

class HNSource(SimplePollingSource):
    def next_item(self):
        return (
            "GLOBAL_ID",
            requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()
        )
    
flow = Dataflow("hn_scraper")
max_id = op.input("in", flow, HNSource(timedelta(seconds=15)))

def mapper(old_max_id, new_max_id):
    if old_max_id is None:
        old_max_id = new_max_id - 10
    return (new_max_id, range(old_max_id, new_max_id))

ids = op.stateful_map("range", max_id, mapper)

ids = op.flat_map("strip_key_flatten", ids, lambda key_ids: key_ids[1])
ids = op.redistribute("redist", ids)

def download_metadata(hn_id) -> Optional[dict]:
    data = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()

    if data is None:
        logging.warning(f"Couldn't find {hn_id}, skipping")
    return data

items =op.filter_map("meta_download", ids, download_metadata)

split_stream = op.branch("split_comments", items, lambda item: item["type"] == "story")
stories = split_stream.trues
comments = split_stream.falses
op.output("stories-out", stories, StdOutSink())
op.output("comments-out", comments, StdOutSink())