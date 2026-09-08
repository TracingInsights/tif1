import json

with open("/tmp/sched2026.json") as f:
    d = json.load(f)

rows = {}
for col, mapping in d.items():
    for idx, val in mapping.items():
        rows.setdefault(int(idx), {})[col] = val

for idx in sorted(rows):
    r = rows[idx]
    if "onaco" in json.dumps(r):
        print(idx, json.dumps(r, indent=1))
print("total rows:", len(rows))
print([r.get("event_name") for r in rows.values()])
