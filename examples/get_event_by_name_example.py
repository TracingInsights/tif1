"""Example: Using get_event_by_name to fetch events by name"""

import tif1

print("=== Exact Match ===")
event = tif1.get_event_by_name(2021, "British Grand Prix", exact_match=True)
print(f"{event['EventName']} - Round {event['RoundNumber']}")
print(f"Location: {event['Location']}, {event['Country']}")
print()

print("=== Alias Match by Circuit ===")
event = tif1.get_event_by_name(2021, "Silverstone")
print(f"'Silverstone' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Alias Match by Country ===")
event = tif1.get_event_by_name(2021, "Great Britain")
print(f"'Great Britain' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Alias Match by Short Name ===")
event = tif1.get_event_by_name(2024, "Bahrain")
print(f"'Bahrain' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Fuzzy Match by Misspelled Name ===")
# Names that are not exact and not in the alias table use fuzzy matching.
# A warning logs the correction.
event = tif1.get_event_by_name(2021, "Monacco")
print(f"'Monacco' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Name That Matches No Event ===")
# A name that resembles no event raises EventNotFoundError (a ValueError).
try:
    tif1.get_event_by_name(2021, "Moon GP")
except tif1.EventNotFoundError as e:
    print(f"'Moon GP' -> EventNotFoundError: {e}")
