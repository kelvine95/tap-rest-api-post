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
    print("🧪 Testing SUI Staking Rewards API")
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
    """Test the Solana API configuration (placeholder)"""
    print("\n" + "="*50)
    print("🧪 Testing Solana Staking Rewards API")
    print("="*50)
    
    api_key = os.getenv("SOL_API_KEY")
    if not api_key:
        print("⏭️  Skipping Solana test - SOL_API_KEY not set")
        return
    
    config = {
        "start_date": "2025-05-01",
        "current_date": "2025-07-15",
        "streams": [
            {
                "name": "solana_staking_rewards",
                "api_url": "https://api.figment.io",
                "path": "/solana/rewards",
                "api_key": api_key,
                "api_key_header": "x-api-key",
                "primary_keys": ["stake_account", "epoch"],
                "replication_key": "created_at",
                "records_path": "$.data[*]",
                "body": {
                    "system_accounts": ["A37fo3njrSiXN6vkRTegqUgVaYLzumypeW6TmFXyQHHF"]
                },
                "date_handling": {
                    "type": "epoch",
                    "start_field": "start",
                    "end_field": "end"
                },
                "transformations": {
                    "field_mappings": {
                        "created_at": "reward_date"
                    },
                    "value_transformations": {
                        "amount": {
                            "type": "divide",
                            "divisor": 1000000000
                        },
                        "rewards": {
                            "type": "divide", 
                            "divisor": 1000000000
                        }
                    }
                },
                "schema": {
                    "type": "object",
                    "properties": {
                        "account": {"type": "string"},
                        "epoch": {"type": "integer"},
                        "rewards": {"type": ["number", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "percent_change": {"type": ["number", "null"]},
                        "apr": {"type": ["number", "null"]},
                        "reward_date": {"type": "string", "format": "date-time"},
                        "stake_account": {"type": "string"},
                        "created_at": {"type": ["string", "null"]}
                    }
                }
            }
        ]
    }
    
    print("📝 Solana configuration ready")
    print(f"   Epoch range will be calculated from {config['start_date']} to {config['current_date']}")


def test_with_singer_format():
    """Test the tap with proper Singer format output"""
    print("\n" + "="*50)
    print("🎵 Testing Singer Format Output")
    print("="*50)
    
    api_key = os.getenv("SUI_API_KEY")
    if not api_key:
        print("❌ ERROR: SUI_API_KEY environment variable is not set!")
        return
    
    # Create a minimal config
    config = {
        "start_date": "2025-01-14",
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
                    "page_size": 10,
                    "total_pages_path": "$.data.pagination.totalPages"
                },
                "schema": {
                    "type": "object",
                    "properties": {
                        "date": {"type": "string", "format": "date"},
                        "epoch": {"type": ["integer", "null"]},
                        "principal": {"type": ["string", "null"]},
                        "epochReward": {"type": ["string", "null"]},
                        "totalRewards": {"type": ["string", "null"]}
                    }
                }
            }
        ]
    }
    
    # This would normally output Singer format messages
    print("✅ Singer format test ready")
    print("   Run with: poetry run tap-rest-api-post --config config.json")


if __name__ == "__main__":
    print("🚀 TAP-REST-API-POST Local Test Suite")
    print("=====================================")
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test SUI API
    test_sui_api()
    
    # Test Solana API config
    #test_solana_api()
    
    # Show Singer format info
    test_with_singer_format()
    
    print("\n" + "="*50)
    print("✅ Testing complete!")
    print("="*50)
