"""Find a good test query and attempt to run it."""
import json
import sys
from datagen.dax_rewriter import extract_filter_bindings

with open("queries.json", "r", encoding="utf-8") as f:
    queries = json.load(f)

# Find a medium-complexity query with filter bindings
candidates = []
for i, q in enumerate(queries):
    text = q.get("[EventText]", "")
    bindings = extract_filter_bindings(text)
    dur = int(q.get("[Duration (ms)]", "0"))
    if 2 <= len(bindings) <= 5 and 100 < dur < 5000 and len(text) < 3000:
        candidates.append((i, dur, bindings, text))

print(f"Found {len(candidates)} candidate queries")
print()

# Show top 5
for idx, (i, dur, bindings, text) in enumerate(candidates[:5]):
    print(f"=== Candidate {idx+1} (query #{i}, {dur}ms) ===")
    for b in bindings:
        vals = b["values"][:3]
        print(f"  {b['pattern']:10s} '{b['table']}'[{b['column']}] = {vals}")
    print()

# Pick the first candidate and extract the clean DAX
if candidates:
    i, dur, bindings, text = candidates[0]
    # Strip trailing [WaitTime: ...] if present
    import re
    clean = re.sub(r'\s*\[WaitTime:.*?\]\s*$', '', text).strip()
    
    print(f"=== Selected query #{i} ({dur}ms) ===")
    print(f"Length: {len(clean)} chars")
    print(f"Bindings: {len(bindings)}")
    print()
    
    # Save for testing
    with open("test_query.dax", "w", encoding="utf-8") as f:
        f.write(clean)
    print("Saved to test_query.dax")
    
    # Also save the query index
    print(f"QUERY_INDEX={i}")
    print(f"ORIGINAL_DURATION={dur}")
