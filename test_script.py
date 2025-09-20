#!/usr/bin/env python3
"""
Test script for Skinport API connectivity and currency handling
"""
import sys
import requests
import json

def test_skinport_api():
    """Test Skinport API with correct headers and currency"""

    # These are the exact headers required by Skinport API
    headers = {
        "Accept-Encoding": "br",
        "User-Agent": "skinport-analysis-tool/11.0-concurrent"
    }

    api_base = "https://api.skinport.com/v1"

    # Test different currencies
    currencies_to_test = ["USD", "EUR", "PLN"]

    for currency in currencies_to_test:
        print(f"\n🧪 Testing Skinport API with {currency}...")

        try:
            url = f"{api_base}/items"
            params = {
                "currency": currency,
                "tradable": 0
            }

            print(f"📡 Request: {url}")
            print(f"📋 Headers: {headers}")
            print(f"🔧 Params: {params}")

            # Make the request
            response = requests.get(url, headers=headers, params=params, timeout=10)

            print(f"📊 Response Status: {response.status_code}")
            print(f"📦 Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            print(f"🗜️  Content-Encoding: {response.headers.get('Content-Encoding', 'None')}")
            print(f"📏 Content Length: {len(response.content)} bytes")

            if response.status_code == 200:
                try:
                    # Try to parse JSON
                    data = response.json()
                    if isinstance(data, list):
                        print(f"✅ Success! Got {len(data)} items as list")
                        if len(data) > 0:
                            print(f"🔍 First item keys: {list(data[0].keys())}")
                        return True
                    elif isinstance(data, dict):
                        if 'items' in data:
                            items = data['items']
                            print(f"✅ Success! Got {len(items)} items in 'items' field")
                            return True
                        else:
                            print(f"✅ Success! Got dict with keys: {list(data.keys())}")
                            return True
                    else:
                        print(f"⚠️  Unexpected data type: {type(data)}")
                        return False

                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                    print(f"📄 Response preview: {response.text[:200]}...")
                    return False

            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(f"📄 Response: {response.text[:200]}...")
                return False

        except requests.RequestException as e:
            print(f"❌ Request failed: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

if __name__ == "__main__":
    print("🚀 Testing Skinport API connectivity...")
    print("=" * 50)

    success = test_skinport_api()

    if success:
        print("\n✅ Skinport API test successful!")
        print("💡 The main script should now work with PLN currency.")
    else:
        print("\n❌ Skinport API test failed.")
        print("💡 Check your internet connection and make sure brotli is installed.")
        print("📦 Install brotli: pip install brotli")
