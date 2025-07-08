import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from datetime import datetime, timedelta

# 关键词和城市设置
keywords = [
    "career planning", #"career development", "professional development",
  #  "leadership", "job fair", "career fair", "networking"
]

locations = [
    "los-angeles",
#    "santa-ana",        # Orange County
 #   "san-diego",
  #  "riverside",
   # "san-bernardino",
#    "ventura",
 #   "santa-barbara",
  #  "el-centro",        # Imperial County
   # "bakersfield",      # Kern County
    #"san-luis-obispo"
]

# 时间范围设定
today = datetime.today().date()
start_date = today.strftime("%Y-%m-%d")
end_date = (today + timedelta(days=2)).strftime("%Y-%m-%d")

# 请求头设定
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 结果集合
event_records = []
seen_ids = set()

# 主循环：关键词 × 城市
for city in locations:
    for keyword in keywords:
        keyword_slug = keyword.replace(" ", "-")
        base_url = f"https://www.eventbrite.com/d/ca--{city}/{keyword_slug}/"
        page = 1

        while page < 3:
            url = f"{base_url}?page={page}&start_date={start_date}&end_date={end_date}"
            print(f"Fetching: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"Request failed: {e}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", href=True)

            new_count = 0
            for a in links:
                href = a["href"]
                if "/e/" in href:
                    match = re.search(r'/e/.+?-([0-9]+)(?:/|$)', href)
                    if match:
                        event_id = match.group(1)
                        if event_id not in seen_ids:
                            seen_ids.add(event_id)
                            full_url = href if href.startswith("http") else f"https://www.eventbrite.com{href}"
                            event_records.append({
                                "event_id": event_id,
                                "event_url": full_url
                            })
                            new_count += 1

            if new_count == 0:
                break  # 没有更多新结果
            page += 1
            time.sleep(1)  # 避免触发反爬限制

# 保存到 CSV
df = pd.DataFrame(event_records)
df.to_csv("event_ids_socal.csv", index=False)
print(f"✅ Saved {len(df)} unique event IDs to event_ids_socal.csv")
