#!/bin/bash
# Test Railway deployment for nostr-rs-relay
# Replace RAILWAY_URL with your actual Railway deployment URL

RAILWAY_URL="https://your-relay-up.railway.app"

echo "Testing nostr-rs-relay deployment at $RAILWAY_URL"
echo "=================================================="
echo ""

# Test 1: Healthcheck - Root path (should return 200)
echo "1. Testing healthcheck endpoint (GET /)..."
curl -i -s "$RAILWAY_URL/" | head -n 5
echo ""

# Test 2: NIP-11 Relay Information (should return JSON)
echo "2. Testing NIP-11 relay information..."
curl -s -H "Accept: application/nostr+json" "$RAILWAY_URL/" | jq .
echo ""

# Test 3: Metrics endpoint (should return Prometheus metrics)
echo "3. Testing metrics endpoint..."
curl -s "$RAILWAY_URL/metrics" | head -n 10
echo ""

echo "=================================================="
echo "If all tests passed, your relay is working!"
