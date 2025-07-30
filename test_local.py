#!/usr/bin/env python3
"""
Local test script for tap-rest-api-post
Run this from the tap-rest-api-post root directory
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add the tap to the Python path
sys.path.insert(0, str(Path(__file__).parent))

from tap_rest_api_post.tap import TapRestApiPost


def test_sui_api():
    """Test the SUI API configuration"""
    print("\n" + "="*50)
    print("🧪 Testing SUI Staking Rewards API (Luganodes)")
    print("="*50)
    
    # Get API key from environment variable
    api_key = os.getenv("SUI_API_KEY")
    if not api_key:
        print("❌ ERROR: SUI_API_KEY environment variable is not set!")
        print("Please run: export SUI_API_KEY='your-actual-api-key'")
        return
    
    print(f"✅ Using API key from environment (length: {len(api_key)} chars)")
    
    config = {
        "start_date": "2025-01-01",
        "current_date": "2025-01-15",
        "streams": [
            {
                "name": "sui_staking_rewards",
                "api_url": "https://staking.luganodes.com",
                "path": "/sui/api/rewards/daily",
                "api_key": api_key,
                "api_key_header": "x-api-key",
                "primary_keys": ["date"],
                "replication_key": "date",
                "records_path": "$.data.rewards[*]",
                "body": {
                    "stake_account_address": "0x8b0a21a9ea44e76a1b5c04e33a9b99f79fb947c5e621a1f3d5d178fb979784fe"
                },
                "date_handling": {
                    "type": "date_string",
                    "start_field": "start_date",
                    "end_field": "end_date"
                },
                "pagination": {
                    "strategy": "total_pages",
                    "page_param": "page",
                    "page_size_param": "limit",
                    "page_size": 100,
                    "total_pages_path": "$.data.pagination.totalPages"
                },
                "transformations": {
                    "value_transformations": {
                        "principal": {
                            "type": "divide",
                            "divisor": 1000000000
                        },
                        "epochReward": {
                            "type": "divide",
                            "divisor": 1000000000
                        },
                        "totalRewards": {
                            "type": "divide",
                            "divisor": 1000000000
                        }
                    }
                },
                "schema": {
                    "type": "object",
                    "properties": {
                        "date": {"type": "string", "format": "date"},
                        "epoch": {"type": ["integer", "null"]},
                        "principal": {"type": ["number", "null"]},
                        "epochReward": {"type": ["number", "null"]},
                        "totalRewards": {"type": ["number", "null"]}
                    }
                }
            }
        ]
    }
    
    tap = TapRestApiPost(config=config)
    
    # Get the stream
    stream = tap.discover_streams()[0]
    
    # Test payload generation
    print("\n📝 Generated request payload:")
    payload = stream.prepare_request_payload(context=None, next_page_token=None)
    print(json.dumps(payload, indent=2))
    
    # Test URL params
    print("\n🔗 URL parameters:")
    params = stream.get_url_params(context=None, next_page_token=None)
    print(json.dumps(params, indent=2))
    
    # Test actual data extraction
    print("\n📊 Fetching data...")
    try:
        records = []
        for record in stream.get_records(context=None):
            records.append(record)
            if len(records) >= 3:  # Just show first 3 records
                break
        
        print(f"✅ Successfully retrieved {len(records)} records (showing first 3)")
        
        for i, record in enumerate(records):
            print(f"\n📌 Record {i+1}:")
            print(f"  Date: {record.get('date')}")
            print(f"  Principal (SUI): {record.get('principal', 0):.9f}")
            print(f"  Daily Reward (SUI): {record.get('epochReward', 0):.9f}")
            print(f"  Total Rewards (SUI): {record.get('totalRewards', 0):.9f}")
            
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        import traceback
        traceback.print_exc()


def test_solana_api():
    """Test the Solana API configuration (Figment)"""
    print("\n" + "="*50)
    print("🧪 Testing Solana Staking Rewards API (Figment)")
    print("="*50)
    
    api_key = os.getenv("SOL_API_KEY")
    if not api_key:
        print("❌ ERROR: SOL_API_KEY environment variable is not set!")
        print("Please run: export SOL_API_KEY='your-actual-api-key'")
        return
    
    print(f"✅ Using API key from environment (length: {len(api_key)} chars)")
    
    config = {
        "start_date": "2025-06-01",
        "current_date": "2025-07-25",
        "streams": [
            {
                "name": "solana_staking_rewards",
                "api_url": "https://api.figment.io",
                "path": "/solana/rewards",
                "api_key": api_key,
                "api_key_header": "X-API-KEY",
                "primary_keys": ["stake_account", "epoch"],
                "replication_key": "timestamp",
                "records_path": "$.data[*]",
                "body": {
                    "system_accounts": ["A37fo3njrSiXN6vkRTegqUgVaYLzumypeW6TmFXyQHHF"]
                },
                "date_handling": {
                    "type": "date_string",
                    "start_field": "start",
                    "end_field": "end"
                },
                "transformations": {
                    "field_extractions": {
                        "protocol_rewards": {
                            "source_field": "rewards",
                            "type": "nested_array",
                            "filter_type": "protocol"
                        },
                        "mev_rewards": {
                            "source_field": "rewards",
                            "type": "nested_array",
                            "filter_type": "mev"
                        },
                        "balance": {
                            "source_field": "balances",
                            "type": "first_array_item"
                        }
                    }
                },
                "schema": {
                    "type": "object",
                    "properties": {
                        "stake_account": {"type": "string"},
                        "epoch": {"type": "integer"},
                        "validator": {"type": "string"},
                        "system_account": {"type": "string"},
                        "timestamp": {"type": "string", "format": "date-time"},
                        "protocol_rewards": {"type": ["number", "null"]},
                        "mev_rewards": {"type": ["number", "null"]},
                        "balance": {"type": ["number", "null"]},
                        "rewards": {"type": ["array", "null"]},
                        "balances": {"type": ["array", "null"]}
                    }
                }
            }
        ]
    }
    
    tap = TapRestApiPost(config=config)
    
    # Get the stream
    stream = tap.discover_streams()[0]
    
    # Test payload generation
    print("\n📝 Generated request payload:")
    payload = stream.prepare_request_payload(context=None, next_page_token=None)
    print(json.dumps(payload, indent=2))
    
    # Test URL params (should be empty for Figment)
    print("\n🔗 URL parameters:")
    params = stream.get_url_params(context=None, next_page_token=None)
    print(json.dumps(params, indent=2))
    
    # Test actual data extraction
    print("\n📊 Fetching data...")
    try:
        records = []
        for record in stream.get_records(context=None):
            records.append(record)
            if len(records) >= 3:  # Just show first 3 records
                break
        
        print(f"✅ Successfully retrieved {len(records)} records (showing first 3)")
        
        for i, record in enumerate(records):
            print(f"\n📌 Record {i+1}:")
            print(f"  Stake Account: {record.get('stake_account')}")
            print(f"  Epoch: {record.get('epoch')}")
            print(f"  Timestamp: {record.get('timestamp')}")
            print(f"  Protocol Rewards (SOL): {record.get('protocol_rewards', 0):.9f}")
            print(f"  MEV Rewards (SOL): {record.get('mev_rewards', 0):.9f}")
            print(f"  Balance (SOL): {record.get('balance', 0):.9f}")
            
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        import traceback
        traceback.print_exc()


def test_with_singer_format():
    """Test the tap with proper Singer format output"""
    print("\n" + "="*50)
    print("🎵 Testing Singer Format Output")
    print("="*50)
    
    # Test both APIs if keys are available
    sui_key = os.getenv("SUI_API_KEY")
    sol_key = os.getenv("SOL_API_KEY")
    
    if not sui_key and not sol_key:
        print("❌ ERROR: No API keys found!")
        print("Please set at least one of: SUI_API_KEY or SOL_API_KEY")
        return
    
    print("✅ Singer format test ready")
    print("   Run with: poetry run tap-rest-api-post --config config.json")
    
    if sui_key:
        print("   - SUI/Luganodes API key found ✓")
    if sol_key:
        print("   - Solana/Figment API key found ✓")


if __name__ == "__main__":
    print("🚀 TAP-REST-API-POST Local Test Suite")
    print("=====================================")
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test SUI API
    test_sui_api()
    
    # Test Solana API
    test_solana_api()
    
    # Show Singer format info
    test_with_singer_format()
    
    print("\n" + "="*50)
    print("✅ Testing complete!")
    print("="*50)
    