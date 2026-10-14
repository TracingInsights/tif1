"""Example: Using get_event_by_round and get_event_by_name"""

import tif1

print("=" * 60)
print("GET EVENT BY ROUND")
print("=" * 60)

# Get event by round number
event = tif1.get_event_by_round(2021, 10)
print(f"Round 10 of 2021: {event['EventName']}")
print(f"Location: {event['Location']}, {event['Country']}")
print(f"Date: {event['EventDate']}")
print()

print("=" * 60)
print("GET EVENT BY NAME - EXACT MATCH")
print("=" * 60)

# Exact match (case insensitive)
event = tif1.get_event_by_name(2021, "British Grand Prix", exact_match=True)
print(f"Exact match: {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=" * 60)
print("GET EVENT BY NAME - ALIASES AND FUZZY MATCH")
print("=" * 60)

# Circuit and country names resolve through the alias table
event = tif1.get_event(2021, "Silverstone")
print(f"By circuit 'Silverstone': {event['EventName']}")

event = tif1.get_event(2021, "Great Britain")
print(f"By country 'Great Britain': {event['EventName']}")

event = tif1.get_event(2024, "Bahrain")
print(f"By short name 'Bahrain': {event['EventName']}")

# Other names fall back to fuzzy matching (a warning logs the correction)
event = tif1.get_event(2021, "Monacco")
print(f"By misspelled name 'Monacco': {event['EventName']}")

# A name that resembles no event: get_event() returns None,
# get_event_by_name() raises EventNotFoundError
event = tif1.get_event(2021, "Moon GP")
print(f"get_event(2021, 'Moon GP'): {event}")
try:
    tif1.get_event_by_name(2021, "Moon GP")
except tif1.EventNotFoundError as e:
    print(f"get_event_by_name(2021, 'Moon GP'): EventNotFoundError: {e}")
