#!/usr/bin/env python3
"""
POC: Single request to verify GDELT API actually responds
Test if the API works at all before running the historical fetcher
"""
import requests
import json
import time

def test_gdelt_direct():
    """Test direct HTTP request to GDELT API"""
    print("🧪 POC: Testing direct GDELT API request")
    print("=" * 60)
    
    # Simple URL for recent sentiment data
    url = "https://api.gdeltproject.org/api/v2/sentiment"
    
    params = {
        "query": "bitcoin",
        "mode": "timelinetone",
        "format": "json"
    }
    
    print(f"\n📡 Sending request to: {url}")
    print(f"   Query: {json.dumps(params, indent=2)}")
    
    try:
        print("\n⏳ Waiting for response...")
        start = time.time()
        response = requests.get(url, params=params, timeout=30)
        elapsed = time.time() - start
        
        print(f"\n✓ Response received in {elapsed:.2f}s")
        print(f"   Status: {response.status_code}")
        print(f"   Content-Type: {response.headers.get('content-type', 'N/A')}")
        print(f"   Content-Length: {len(response.content)} bytes")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"\n✓ JSON parsed successfully")
                print(f"   Keys: {list(data.keys())}")
                
                if "timeline" in data:
                    timeline = data["timeline"]
                    print(f"   Timeline entries: {len(timeline)}")
                    if timeline:
                        print(f"   First entry: {timeline[0]}")
                        print(f"   Last entry: {timeline[-1]}")
                    return True
                else:
                    print(f"\n⚠️  No 'timeline' key in response")
                    print(f"   Full response: {json.dumps(data, indent=2)[:500]}")
                    return False
            except json.JSONDecodeError as e:
                print(f"\n❌ JSON decode error: {e}")
                print(f"   Response text: {response.text[:500]}")
                return False
        else:
            print(f"\n❌ HTTP error {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return False
            
    except requests.Timeout:
        print(f"\n❌ Request timeout after 30s")
        return False
    except requests.ConnectionError as e:
        print(f"\n❌ Connection error: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {type(e).__name__}: {e}")
        return False

def test_through_gdelt_client():
    """Test using the GDELTClient wrapper"""
    print("\n\n🧪 POC: Testing through GDELTClient")
    print("=" * 60)
    
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    
    from gdelt_client import GDELTClient
    
    client = GDELTClient(timeout=30)
    
    print("\n📡 Calling client.get_sentiment('bitcoin', timespan='7d')")
    
    try:
        print("\n⏳ Waiting for response...")
        start = time.time()
        result = client.get_sentiment(query="bitcoin", timespan="7d")
        elapsed = time.time() - start
        
        print(f"\n✓ Response received in {elapsed:.2f}s")
        print(f"   Result type: {type(result)}")
        print(f"   Result keys: {list(result.keys()) if result and isinstance(result, dict) else 'N/A'}")
        
        if result and "timeline" in result:
            timeline = result["timeline"]
            print(f"   Timeline entries: {len(timeline)}")
            if timeline:
                print(f"   First entry: {timeline[0]}")
            return True
        else:
            print(f"\n⚠️  No timeline in result")
            print(f"   Result: {result}")
            return False
            
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success_direct = test_gdelt_direct()
    success_client = test_through_gdelt_client()
    
    print("\n\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"Direct HTTP request: {'✓ PASS' if success_direct else '❌ FAIL'}")
    print(f"GDELTClient wrapper: {'✓ PASS' if success_client else '❌ FAIL'}")
    
    if success_direct and not success_client:
        print("\n⚠️  The API works but GDELTClient wrapper is broken!")
        print("   Check gdelt_client.py implementation")
    elif not success_direct:
        print("\n❌ API itself is not responding - may be rate limited or down")
    else:
        print("\n✓ Both work correctly - problem might be in historical_fetcher logic")
