import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from datetime import datetime, timedelta
import csv
import threading
import queue
import pytz

EVENT_ID_RE = re.compile(r'/e/.+-([0-9]+)(?=\D|$)')

start_time = time.time()

keywords = [
    "career planning", "career development", "professional development",
    "leadership", "job fair", "career fair", "networking"
]

locations = {
    "los-angeles": "Los Angeles",
    "santa-ana": "Orange County",
    "san-diego": "San Diego",
    "riverside": "Riverside",
    "san-bernardino": "San Bernardino",
    "ventura": "Ventura",
    "santa-barbara": "Santa Barbara",
    "el-centro": "Imperial",
    "bakersfield": "Kern",
    "san-luis-obispo": "San Luis Obispo",
    "online": "Online"
}

start_day = datetime.today().date()
days_range = 30

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

seen_ids = set()
results = []

rate_limit_errors = []
other_errors = []

for i in range(days_range):
    date = start_day + timedelta(days=i)
    date_str = date.strftime("%Y-%m-%d")
    print(f"\n📅 Searching for events on {date_str}")

    for loc_slug, loc_label in locations.items():
        for keyword in keywords:
            keyword_slug = keyword.replace(" ", "-")
            base_url = f"https://www.eventbrite.com/d/online/{keyword_slug}/" if loc_slug == "online" \
                       else f"https://www.eventbrite.com/d/ca--{loc_slug}/{keyword_slug}/"

            for page in range(1, 51):
                url = f"{base_url}?page={page}&start_date={date_str}&end_date={date_str}"
                print(f"  → Fetching page {page}: {loc_label} - {keyword}")
                try:
                    resp = requests.get(url, headers=headers, timeout=10)
                    if resp.status_code == 429:
                        print(f"    ⚠️  Rate-limit hit: {url}")
                        rate_limit_errors.append(url)
                        break
                    elif resp.status_code != 200:
                        print(f"    ❌ Error {resp.status_code}: {url}")
                        other_errors.append((resp.status_code, url))
                        break
                except Exception as e:
                    print(f"    ❌ Request Exception: {e} - {url}")
                    other_errors.append(("exception", url))
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                anchors = soup.find_all("a", href=True)

                new_found = 0
                for a in anchors:
                    href = a["href"]
                    if "/e/" in href:
                        match = EVENT_ID_RE.search(href)

                        if match:
                            event_id = match.group(1)
                            if event_id not in seen_ids:
                                seen_ids.add(event_id)
                                results.append({
                                    "event_id": event_id,
                                    "event_date": date_str,
                                    "location": loc_label
                                })
                                new_found += 1

                if new_found == 0:
                    break
                time.sleep(3)

df = pd.DataFrame(results)
df.sort_values(by=["event_date", "location", "event_id"], inplace=True)

end_time = time.time()
duration = end_time - start_time
minutes, seconds = divmod(duration, 60)

print("\n✅ DONE")
print(f"🔢 Total unique event IDs: {len(seen_ids)}")
print(f"⏱ Total run time: {int(minutes)} minutes {int(seconds)} seconds")
print(f"⚠️  Rate-limit hits: {len(rate_limit_errors)}")
if rate_limit_errors:
    print("    → URLs with rate-limit:")
    for url in rate_limit_errors:
        print(f"      - {url}")

print(f"❌ Other errors: {len(other_errors)}")
if other_errors:
    print("    → URLs with other errors:")
    for err, url in other_errors:
        print(f"      - [{err}] {url}") 


API_BASE   = "https://www.eventbriteapi.com/v3"
OUTPUT_CSV = "combine2.csv"
OUTPUT_MD  = "combine2.md"

TOKENS = [
    "ADFEE3M6QKP4UHHK4RTJ","AUUSEMT6BFTDLYNMDFPU","7Z7AWALOH2TLA7XQX7OK",
    "YGRRGPXV7F7NQV4HH5RU","4H774ER6HNVINEL627TZ","TGIPUCHUYCVBACWGG6GI",
    "GO62D54NYILEA3AFNH4J","6MVAAMPKWFWNRPXGNODG","BQQLKRU5JPAGIOOJUGVH",
    "LYUIAO4THHDCKM3SNJBE","5FEMNH4LDL4RQQEKEAL4","XT6F7O5DVONZOG7WOTKX",
    "QAOTJ65YVJLDE3RHABAF","CPXY6BZLSP2WCLQ3ZQIS","OOAWYP6D7KIN25F5M6PZ",
    "6AG6WFR46S7SKYU5GY7K","ZA54YJ7YCHEMUBBV7UA6","HVHSEU223FIKILV4D3T5",
    "237RK6M5K54Z7DD5XIHW","HXGETSGAZDES2B5ZNNWF","AQG3T27GPOQVDLUCPU5J",
    "6JUXTETTGHTPR2FUPXOW","KUMPCT5DZILKWCRRRPVP","JMXYZ5ANBW57IO6CBQ7A",
    "7BXG7G6JGBP7UP43PZAP","G6UAN755SVY3VBOCGL6Y","QMMBVEKVMYYQ3ZDA6HQK",
    "INTZNCHDKQWHMPZPX4BU","HNF6UNGUAUQ77C76HNVL","ZHLGC7OBFFPRZ6PZRX5V",
    "RFW5X5UYFUQSOZ2OJMAZ","TSZG4QRXXOA4WYKPULGI","CQOOLXW3DUDCWDCTTFLL",
    "M3CU2VLQ2OSO2MDIVLXC","O3EULVSK6NWIJJ4XSST3","75WESU4HDVHHRQ4IXIYZ",
    "7JWMEX6CPI4RKJYRJS6W","MWCOODQPBRCLX6L5GWCV","22UAT7BSC3MQQSF2VJRD",
    "NRCVYNKLDIM7KHJKNJHL","FVFJFHCXJG6B7EJ6DN26","K32AEV7RONXA7QV5BXRS",
    "YIFSVQXG3AYAYCUDTOSC","6HREUV3Q35K2CKRM5CJQ","MGJFNF7SRKG3MKHDU6G4",
    "SATAELGXQVFICGZ6PXXH","IF3SIR6NVOLQMUJI455K","E524YMHHMNORUOTNXSKS",
    "ECJYLU4WTF3QGPKOHEHS","C3PYJRSQT7DCWRJUGJBK","IZDL5NYVJUOM3DZKMIV7",
    "6YD6RT24JFGEHBNGUOD6","AO5GIQCLCERRTOU4CPTU","7VYNZF74DKHTOZZRLTXM",
    "M3G6KP4UXZ5PFGVNMNZZ","BOSYHVEDOHQJN3SU742M","ICYWMBRWIO726IU5FGPI",
    "PIBEZGMWDR77L2T3UAQZ","RZQC46TK6TVQCFV2FTED","TL4MCD5NJBY5X7S7GRTX",
    "UGQ3E3E63KRL4CLWQTXH","NYMXR3VLCNQO7Q7CIZAI","Y5RIULQGUAKLZGXIFTNW",
    "PKIQ7Z4QFO6XKSED3G2P","FBNGOQ6T7WJ7IS7ROGAN","QVNLNFZTKR5JA2MWTGXZ",
    "NVUE2KM2JSSEYTSESDM6","OJFN4TBC3J3LPSV6AP6M"
]

if not TOKENS:
    raise RuntimeError("Please populate the TOKENS list with at least one OAuth token.")

def fetch_event(session, event_id):
    url = f"{API_BASE}/events/{event_id}/"
    params = {"expand": "organizer,venue,ticket_classes"}
    try:
        resp = session.get(url, params=params)
    except Exception as e:
        return None, 'exception', str(e)
    if resp.status_code == 429:
        return None, 'rate_limit', None
    if not resp.ok:
        return None, 'http_error', f"status {resp.status_code}"
    try:
        return resp.json(), None, None
    except ValueError as e:
        return None, 'parse_error', str(e)

def format_address(venue):
    if not venue or venue.get("online_event"):
        return "Online" if venue and venue.get("online_event") else "Unknown"
    addr = venue.get("address", {}) or {}
    parts = [
        addr.get("address_1",""), addr.get("address_2",""),
        addr.get("city",""), addr.get("region",""),
        addr.get("postal_code",""), addr.get("country","")
    ]
    return ", ".join(p.strip() for p in parts if p and p.strip()) or "Unknown"

def extract_fee(ticket_classes):
    fees = []
    for tc in ticket_classes or []:
        if tc.get("is_sold_out"):
            fees.append("sold out")
        else:
            cost = tc.get("cost")
            if cost:
                val = cost.get("value", 0)/100
                cur = cost.get("currency","").upper()
                fees.append(f"{cur} {val:.2f}")
            elif tc.get("free"):
                fees.append("free")
    return fees[0] if fees else "Unknown"

def to_pt(utc_str):
    dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")
    return pytz.utc.localize(dt).astimezone(pytz.timezone("America/Los_Angeles"))

def clean_desc(text):
    return " ".join(text.split())

def main():
    run_start = time.time()
    now_utc = datetime.now(pytz.utc)
    cutoff  = now_utc + timedelta(days=30)

    event_ids = list(seen_ids)
    total_calls = len(event_ids)

    q = queue.Queue()
    for eid in event_ids:
        q.put(eid)

    records = []
    failure_counts = {'rate_limit': 0, 'http_error': 0, 'exception': 0, 'parse_error': 0}
    failure_details = []
    stats = {'success': 0}
    lock = threading.Lock()

    def worker(token):
        session = requests.Session()
        session.headers.update({
            "Accept":        "application/json",
            "Authorization": f"Bearer {token}"
        })
        while True:
            try:
                eid = q.get_nowait()
            except queue.Empty:
                return
            data, err_type, err_info = fetch_event(session, eid)
            time.sleep(3)

            with lock:
                if err_type:
                    failure_counts[err_type] += 1
                    failure_details.append((eid, err_type, err_info))
                else:
                    stats['success'] += 1
                    start_utc = data.get("start", {}).get("utc")
                    if not start_utc:
                        failure_counts['parse_error'] += 1
                        failure_details.append((eid, 'parse_error', 'missing start.utc'))
                    else:
                        dt_utc = pytz.utc.localize(datetime.strptime(start_utc, "%Y-%m-%dT%H:%M:%SZ"))
                        if now_utc <= dt_utc <= cutoff:
                            name_obj     = data.get("name") or {}
                            desc_obj     = data.get("description") or {}
                            org_obj      = data.get("organizer") or {}
                            venue_obj    = data.get("venue")
                            online_flag  = data.get("online_event", False)

                            title     = (name_obj.get("text") or "").strip() or "No title"
                            organizer = (org_obj.get("name") or "").strip() or "Unknown organizer"
                            raw_desc  = (desc_obj.get("text") or "")
                            desc      = clean_desc(raw_desc) or "No description provided"
                            fee       = extract_fee(data.get("ticket_classes"))
                            address   = "Online" if online_flag else format_address(venue_obj)
                            url_event = data.get("url", "").strip() or "No URL"

                            records.append({
                                "Event ID"   : eid,
                                "Title"      : title,
                                "Organizer"  : organizer,
                                "Start (PT)" : to_pt(start_utc).strftime("%Y-%m-%d %H:%M"),
                                "Address"    : address,
                                "Description": desc,
                                "URL"        : url_event,
                                "Fee"        : fee
                            })
            q.task_done()

    threads = [threading.Thread(target=worker, args=(tkn,), daemon=True) for tkn in TOKENS]
    for t in threads: t.start()
    for t in threads: t.join()

    records.sort(key=lambda r: r["Start (PT)"])
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvf:
        writer = csv.writer(csvf)
        writer.writerow(["Event ID","Title","Organizer","Start (PT)","Address","Description","URL","Fee"])
        for r in records:
            writer.writerow([r[k] for k in ["Event ID","Title","Organizer","Start (PT)","Address","Description","URL","Fee"]])

    with open(OUTPUT_MD, "w", encoding="utf-8") as md:
        md.write("# Upcoming Career & Leadership Events (Next 30 Days, SoCal)\n\n")
        for r in records:
            md.write(f"- **{r['Title']}**\n")
            md.write(f"  - Organizer: {r['Organizer']}\n")
            md.write(f"  - When: {r['Start (PT)']}\n")
            md.write(f"  - Address: {r['Address']}\n")
            md.write(f"  - Description: {r['Description']}\n")
            md.write(f"  - URL: {r['URL']}\n")
            md.write(f"  - Fee: {r['Fee']}\n\n")

    elapsed = time.time() - run_start
    total_failures = sum(failure_counts.values())

    print("\n=== Run Statistics ===")
    print(f"Total API calls attempted: {total_calls}")
    print(f"  Successful fetches:      {stats['success']}")
    for etype, cnt in failure_counts.items():
        print(f"  {etype.replace('_',' ').title():<22}: {cnt}")
    print(f"Total failures:            {total_failures}")
    if failure_details:
        print("\nFailure details (Event ID, Type, Info):")
        for fid, ftype, finfo in failure_details:
            info = finfo or "(no additional info)"
            print(f"  - {fid}: {ftype} — {info}")
    print(f"\nTotal runtime: {elapsed:.2f} seconds")
    print(f"Wrote CSV → {OUTPUT_CSV}")
    print(f"Wrote Markdown → {OUTPUT_MD}")

if __name__ == "__main__":
    main()