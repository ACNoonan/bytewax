from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as win
from bytewax.operators.windowing import EventClock, SessionWindower
from bytewax.testing import TestingSource

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.tutorials.sessions_search_logs:flow

flow = Dataflow("search_ctr")

@dataclass
class AppOpen:
    """ Represents an app opening event.
    
    This class encapsulates the data for an app
    opening event, including the user ID and the 
    timestamp when the app was opened.
    """

    user: int
    time: datetime

@dataclass
class Search:
    """ Represents a search event.
    
    This class encapsulates the data for an app
     search event, including the user ID, and the 
    timestamp when the search was performed, and the search query.
    """

    user: int
    query: str
    time: datetime

@dataclass
class Results:
    """ Represents a search event.
    This class encapsulates the data for an app
    search event, including the user ID and the 
    timestamp when the app was opened."""
    user: int
    items: List[str]
    time: datetime

@dataclass
class ClickResult:
    """ Represents a click result event.
    
    This class encapsulates the data for an app
    click event, including the user ID and the
    timestamp when the click was performed.
    """

    user: int
    item: str
    time: datetime


# Simulated events to emit into our Dataflow
align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
client_events = [
    Search(user=1, query="dogs", time=align_to + timedelta(seconds=5)),
    Results(
        user=1, items=["fido", "rover", "buddy"], time=align_to + timedelta(seconds=6)
    ),
    ClickResult(user=1, item="rover", time=align_to + timedelta(seconds=7)),
    Search(user=2, query="cats", time=align_to + timedelta(seconds=5)),
    Results(
        user=2,
        items=["fluffy", "burrito", "kathy"],
        time=align_to + timedelta(seconds=6),
    ),
    ClickResult(user=2, item="fluffy", time=align_to + timedelta(seconds=7)),
    ClickResult(user=2, item="kathy", time=align_to + timedelta(seconds=8))
]

# Feed input data
inp = op.input("inp", flow, TestingSource(client_events))

def user_event(event):
    """Extract user ID as a key and pass the event itself"""
    #print(f"Processing event for user: {event.user}")
    return str(event.user), event

# Map the user event function to the input
user_event_map = op.map("user_event", inp, user_event)


def calc_ctr(window_out):
    """Calculate the click-through rate (CTR)."""
    user, search_session = window_out  # Unpack window_out
    _, events = search_session  # Unpack search_session to get the events list


    # Filter events to find searches and clicks
    searches = [event for event in events if isinstance(event, Search)]
    clicks = [event for event in events if isinstance(event, ClickResult)]

    # Calculate CTR
    search_count = len(searches)
    click_count = len(clicks)
    print(f"User {user}: {search_count} searches, {click_count} clicks")

    return (user, click_count / search_count if search_count else 0)

# Configure the event clock and session windower
event_time_config: EventClock = EventClock(
    ts_getter=lambda e: e.time, wait_for_system_duration=timedelta(seconds=1)
)
clock_config = SessionWindower(gap=timedelta(seconds=10))

# Collect the windowed data
window = win.collect_window(
    "windowed_data", user_event_map, clock=event_time_config, windower=clock_config
)

# Calculate the click-through rate using the
# calc_ctr function and the windowed data
calc = op.map("calc_ctr", window.down, calc_ctr)

# Output the results to the standard output
op.output("out", calc, StdOutSink())
