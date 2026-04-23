import os
import json

from dotenv import load_dotenv
import requests

load_dotenv()

BASE_URL = os.getenv("SHOW_API_BASE_URL")


def inspect(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, params=params, timeout=30)
    print(f"\nURL: {response.url}")
    print(f"Status: {response.status_code}")
    response.raise_for_status()

    data = response.json()

    print("\nTop-level type:", type(data).__name__)
    if isinstance(data, dict):
        print("Top-level keys:", list(data.keys())[:20])

        for key, value in data.items():
            if isinstance(value, list):
                print(f"\nList key '{key}' has {len(value)} items")
                if value:
                    print("First item keys:", list(value[0].keys())[:20])
                break
    elif isinstance(data, list):
        print("List length:", len(data))
        if data:
            print("First item keys:", list(data[0].keys())[:20])

    print("\nSample payload:")
    print(json.dumps(data, indent=2)[:3000])


def main():
    inspect("items.json")
    inspect("listings.json")


if __name__ == "__main__":
    main()