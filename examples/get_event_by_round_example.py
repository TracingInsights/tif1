"""Example: Using get_event_by_round to fetch events by round number"""
import tif1

# Get event by round number
event = tif1.get_event_by_round(2021, 10)

print(f"Round {event['RoundNumber']}: {event['EventName']}")
print(f"Location: {event['Location']}, {event['Country']}")
print(f"Date: {event['EventDate']}")
print(f"Format: {event['EventFormat']}")
print()

# Get first round of 2024
first_event = tif1.get_event_by_round(2024, 1)
print(f"First event of 2024: {first_event['EventName']}")
print()

# You can also use get_event with round number (same result)
event2 = tif1.get_event(2021, 10)
print(f"Using get_event: {event2['EventName']}")
