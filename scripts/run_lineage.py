"""Utility to view lineage events."""
import os
import sys
import json
import glob

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LINEAGE_DIR = os.path.join(PROJECT_ROOT, "artifacts", "lineage_events")


def main():
    print("=== Lineage Events ===\n")
    events = glob.glob(os.path.join(LINEAGE_DIR, "*.json"))
    if not events:
        print("No lineage events recorded yet.")
        return

    for event_file in sorted(events):
        with open(event_file) as f:
            event = json.load(f)
        print(f"Event: {os.path.basename(event_file)}")
        print(f"  Job: {event.get('job', {}).get('name', 'N/A')}")
        print(f"  Time: {event.get('eventTime', 'N/A')}")
        print(f"  Inputs: {[i['name'] for i in event.get('inputs', [])]}")
        print(f"  Outputs: {[o['name'] for o in event.get('outputs', [])]}")
        print()


if __name__ == "__main__":
    main()
