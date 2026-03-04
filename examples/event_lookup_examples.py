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
print("GET EVENT BY NAME - FUZZY MATCH")
print("=" * 60)

# Fuzzy match by location
event = tif1.get_event(2021, "Silverstone")
print(f"By location 'Silverstone': {event['EventName']}")

# Fuzzy match by country
event = tif1.get_event(2021, "Great Britain")
print(f"By country 'Great Britain': {event['EventName']}")

# Fuzzy match by partial name
event = tif1.get_event(2024, "Bahrain")
print(f"By partial name 'Bahrain': {event['EventName']}")

event = tif1.get_event(2021, "Monaco")
print(f"By partial name 'Monaco': {event['EventName']}")
