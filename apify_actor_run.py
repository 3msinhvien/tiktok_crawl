from apify_client import ApifyClient
import json

# You can find your API token at https://console.apify.com/settings/integrations.
client  = ApifyClient()

run_input = {
    "isDownloadVideo": True,
    "isDownloadVideoCover": True,
    "isUnlimited": False,
    "limit": 1,
    "publishTime": "MONTH",
    "region": "JP",
    "sortType": 0,
    "type": "TREND"
}

run = client.actor("novi/fast-tiktok-api").call(run_input=run_input)

# Collect all items into a list first
items = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    items.append(item)

# Write as properly formatted JSON
with open("scan_results_jp.json", "w", encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)
