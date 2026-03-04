"""Example: Using get_event_by_name to fetch events by name"""
import tif1

print("=== Exact Match ===")
event = tif1.get_event_by_name(2021, "British Grand Prix", exact_match=True)
print(f"{event['EventName']} - Round {event['RoundNumber']}")
print(f"Location: {event['Location']}, {event['Country']}")
print()

print("=== Fuzzy Match by Location ===")
event = tif1.get_event_by_name(2021, "Silverstone")
print(f"'Silverstone' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Fuzzy Match by Country ===")
event = tif1.get_event_by_name(2021, "Great Britain")
print(f"'Great Britain' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

print("=== Fuzzy Match by Partial Name ===")
event = tif1.get_event_by_name(2024, "Bahrain")
print(f"'Bahrain' -> {event['EventName']} (Round {event['RoundNumber']})")
print()

event = tif1.get_event_by_name(2021, "Monaco")
print(f"'Monaco' -> {event['EventName']} (Round {event['RoundNumber']})")
