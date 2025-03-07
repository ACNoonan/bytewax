from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulBatchLogic

# Run command (From Documents): 
#  python -m bytewax.run projects.bytewax.tutorials.smoothie:flow

flow = Dataflow("init_smoothie")

# Step 1: Show all orders
orders = op.input("orders", flow, CSVSource(Path("projects/bytewax/tutorials/smoothie_orders.csv")))
#op.inspect("see_data", orders)


# Step 2: Filter orders for containing Banana or Almond Milk
def contains_banana_or_almond_milk(order):
    return "Banana" in order["ingredients"] or "Almond Milk" in order["ingredients"]

banana_orders = op.filter("banana_orders", orders, contains_banana_or_almond_milk)
#op.inspect("filter_results", banana_orders)

# Step 3: Setup a Mock Pricing Service
def mock_pricing_service(smoothie_name):
    """Mock pricing service to get the price of a smoothie."""
    prices = {
        "Green Machine": 5.99,
        "Berry Blast": 6.49,
        "Tropical Twist": 7.49,
        "Protein Power": 8.99,
        "Citrus Zing": 6.99,
        "Mocha Madness": 7.99,
        "Morning Glow": 5.49,
        "Nutty Delight": 8.49,
    }
    return prices.get(smoothie_name, 0)

def enrich_with_price(cache, order):
    order["price"] = cache.get(order["order_requested"])
    return order

enriched_orders = op.enrich_cached(
    "enrich_with_price", orders, mock_pricing_service, enrich_with_price
)

#op.inspect("enrich_results", enriched_orders)

# Step 4: Calculate total price for the order with tax
TAX_RATE = 0.15

def calculate_total_price(order):
    """Calculate the total price of the order including tax."""
    # Assuming each order has a quantity of 1 for simplicity
    order["total_price"] = round(float(order["price"]) * (1 + TAX_RATE), ndigits=2)
    return order

total_price_orders = op.map(
    "calculate_total_price", enriched_orders, calculate_total_price
)

#op.inspect("total_price_orders", total_price_orders)

# Step 5: Calculate the Total Number of orders & Total Revenue per type of Smoothie
counted_orders = op.count_final(
    "count_smoothies", total_price_orders, key=lambda order: order["order_requested"]
)

#op.inspect("counted_orders", counted_orders)

def calculate_total_revenue_with_tax(counts, pricing_service):
    """Multiply the count of smoothies by the cahced price and include tax"""
    smoothie, count = counts
    price = pricing_service(smoothie)
    total_without_tax = count * price
    total_with_tax = round(total_without_tax * (1 + TAX_RATE), ndigits=2)
    return (smoothie, total_with_tax)

total_revenue_orders = op.map(
    "calculate_total_revenue_with_tax", 
    counted_orders, 
    lambda counts: calculate_total_revenue_with_tax(counts, mock_pricing_service)
)

op.inspect("inspect_total_revenue", total_revenue_orders)