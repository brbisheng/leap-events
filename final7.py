
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
final6.py

Eventbrite-based Career & Leadership Event Aggregator for Southern California
— reads a list of venue IDs,
— fetches upcoming events via Eventbrite’s API,
— filters to the next 30 days,
— uses WordNet + spaCy pre-trained embeddings to expand synonyms broadly,
— matches on category/subcategory/description/name for relevant terms,
— extracts per-event matched keywords,
— exports:
  • CSV with columns: Keywords, Event ID, Title, Organizer, Start (PT), Address, Description, URL, Fee
  • Markdown with per-event bullets: Title, Keywords, Organizer, Start, Address, Description, URL, Fee
"""

import time
import csv
import requests
from datetime import datetime, timedelta, timezone
import pytz
import nltk
from nltk.corpus import wordnet
import spacy
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────

TOKENS       = [
    "ADFEE3M6QKP4UHHK4RTJ",
    "AUUSEMT6BFTDLYNMDFPU",
    "7Z7AWALOH2TLA7XQX7OK",
    "YGRRGPXV7F7NQV4HH5RU",
    "4H774ER6HNVINEL627TZ",
    "TGIPUCHUYCVBACWGG6GI",
    "GO62D54NYILEA3AFNH4J",
    "6MVAAMPKWFWNRPXGNODG",
    "BQQLKRU5JPAGIOOJUGVH",
    "LYUIAO4THHDCKM3SNJBE",
    "5FEMNH4LDL4RQQEKEAL4",
    "XT6F7O5DVONZOG7WOTKX",
    "QAOTJ65YVJLDE3RHABAF",
    "CPXY6BZLSP2WCLQ3ZQIS",
    "OOAWYP6D7KIN25F5M6PZ",
    "6AG6WFR46S7SKYU5GY7K",
    "ZA54YJ7YCHEMUBBV7UA6",
    "HVHSEU223FIKILV4D3T5",
    "237RK6M5K54Z7DD5XIHW",
    "HXGETSGAZDES2B5ZNNWF",
    "AQG3T27GPOQVDLUCPU5J",
    "6JUXTETTGHTPR2FUPXOW",
    "KUMPCT5DZILKWCRRRPVP",
    "JMXYZ5ANBW57IO6CBQ7A",
    "7BXG7G6JGBP7UP43PZAP",
    "G6UAN755SVY3VBOCGL6Y",
    "QMMBVEKVMYYQ3ZDA6HQK",
    "INTZNCHDKQWHMPZPX4BU",
    "HNF6UNGUAUQ77C76HNVL",
    "ZHLGC7OBFFPRZ6PZRX5V",
    "RFW5X5UYFUQSOZ2OJMAZ",
    "TSZG4QRXXOA4WYKPULGI",
    "CQOOLXW3DUDCWDCTTFLL",
    "M3CU2VLQ2OSO2MDIVLXC",
    "O3EULVSK6NWIJJ4XSST3",
    "75WESU4HDVHHRQ4IXIYZ",
    "7JWMEX6CPI4RKJYRJS6W",
    "MWCOODQPBRCLX6L5GWCV",
    "22UAT7BSC3MQQSF2VJRD",
    "NRCVYNKLDIM7KHJKNJHL",
    "FVFJFHCXJG6B7EJ6DN26",
    "K32AEV7RONXA7QV5BXRS",
    "YIFSVQXG3AYAYCUDTOSC",
    "6HREUV3Q35K2CKRM5CJQ",
    "MGJFNF7SRKG3MKHDU6G4",
    "SATAELGXQVFICGZ6PXXH",
    "IF3SIR6NVOLQMUJI455K",
    "E524YMHHMNORUOTNXSKS",
    "ECJYLU4WTF3QGPKOHEHS",
    "C3PYJRSQT7DCWRJUGJBK",
    "IZDL5NYVJUOM3DZKMIV7",
    "6YD6RT24JFGEHBNGUOD6",
    "AO5GIQCLCERRTOU4CPTU",
    "7VYNZF74DKHTOZZRLTXM",
    "M3G6KP4UXZ5PFGVNMNZZ",
    "BOSYHVEDOHQJN3SU742M",
    "ICYWMBRWIO726IU5FGPI",
    "PIBEZGMWDR77L2T3UAQZ",
    "RZQC46TK6TVQCFV2FTED",
    "TL4MCD5NJBY5X7S7GRTX",
    "UGQ3E3E63KRL4CLWQTXH",
    "NYMXR3VLCNQO7Q7CIZAI",
    "Y5RIULQGUAKLZGXIFTNW",
    "PKIQ7Z4QFO6XKSED3G2P",
    "FBNGOQ6T7WJ7IS7ROGAN",
    "QVNLNFZTKR5JA2MWTGXZ",
    "NVUE2KM2JSSEYTSESDM6",
    "OJFN4TBC3J3LPSV6AP6M",
    

]
API_BASE     = "https://www.eventbriteapi.com/v3"
INPUT_VENUES  = "/Users/x/Desktop/LEAP/venue_id_test/filtered_venues.txt"
OUTPUT_CSV   = "leap_events_socal7.csv"
OUTPUT_MD    = "leap_events_socal7.md"

CALLS_PER_HOUR = 1000
MIN_INTERVAL   = 3600.0 / CALLS_PER_HOUR

counters     = {"api_calls": 0, "rate_limit_hits": 0, "other_errors": 0}
counter_lock = Lock()
events       = []
events_lock  = Lock()

# ─── DOWNLOAD NLP RESOURCES ─────────────────────────────────────────────────────

nltk.download('wordnet', quiet=True)
# load spaCy medium English model with vectors
nlp = spacy.load('en_core_web_md')

# ─── BASE TERMS & SYNONYMS (WordNet + embeddings) ───────────────────────────────

base_terms = ["career planning", "leadership", "job fair", "professional development"]
synonyms   = set(base_terms)

# WordNet expansions
for term in base_terms:
    lookup = term.replace(" ", "_")
    for syn in wordnet.synsets(lookup):
        for lemma in syn.lemmas():
            synonyms.add(lemma.name().replace("_", " ").lower())

# Embedding-based expansions via spaCy
for term in base_terms:
    doc = nlp(term)
    # find top 10 most similar tokens in vocab
    similar = sorted(
        (w for w in nlp.vocab if w.has_vector and w.is_lower and w.is_alpha),
        key=lambda w: doc.similarity(nlp(w.text)),
        reverse=True
    )[:10]
    for w in similar:
        if doc.similarity(nlp(w.text)) > 0.65:
            synonyms.add(w.text)

# helper to extract matched keywords
def extract_keywords(text: str):
    txt = text.lower()
    return sorted({t for t in synonyms if t in txt})

# ─── RATE-LIMITED REQUEST ───────────────────────────────────────────────────────

def rate_limited_get(session, url, last_call, params=None):
    elapsed = time.time() - last_call
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    resp = session.get(url, params=params)
    return resp, time.time()

# ─── LOAD VENUES ────────────────────────────────────────────────────────────────

def load_ids(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip()]

# ─── FORMAT ADDRESS ────────────────────────────────────────────────────────────

def assemble_address(addr):
    parts = [addr.get(k, '') for k in (
        'address_1','address_2','city','region','postal_code','country'
    )]
    return ", ".join(p.strip() for p in parts if p and p.strip())

# ─── WORKER ────────────────────────────────────────────────────────────────────

def worker(venue_subset, token_index):
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKENS[token_index]}"
    })
    last_call = 0.0
    local = []

    now_utc    = datetime.now(timezone.utc)
    cutoff_utc = now_utc + timedelta(days=30)

    for vid in venue_subset:
        url_list = f"{API_BASE}/venues/{vid}/events/"
        resp, last_call = rate_limited_get(session, url_list, last_call)
        with counter_lock: counters['api_calls'] += 1
        if resp.status_code == 429:
            with counter_lock: counters['rate_limit_hits'] += 1
            continue
        if not resp.ok:
            with counter_lock: counters['other_errors'] += 1
            continue

        for meta in resp.json().get('events', []):
            eid = meta.get('id')
            if not eid: continue

            # fetch full details with category & subcategory
            url_det = f"{API_BASE}/events/{eid}/"
            params  = {"expand": "organizer,venue,ticket_classes,category,subcategory"}
            resp2, last_call = rate_limited_get(session, url_det, last_call, params=params)
            with counter_lock: counters['api_calls'] += 1
            if resp2.status_code == 429:
                with counter_lock: counters['rate_limit_hits'] += 1
                continue
            if not resp2.ok:
                with counter_lock: counters['other_errors'] += 1
                continue

            d = resp2.json()
            utc_start = datetime.fromisoformat(d['start']['utc'].replace('Z','+00:00'))
            if not (now_utc <= utc_start <= cutoff_utc):
                continue

            # combine fields for matching
            combined = " ".join([
                d.get('name',{}).get('text',''),
                d.get('description',{}).get('text',''),
                d.get('organizer',{}).get('name',''),
                d.get('category',{}).get('name',''),
                d.get('subcategory',{}).get('name',''),
            ])
            keywords = extract_keywords(combined)
            if not keywords:
                continue

            # organizer
            org = d.get('organizer',{}).get('name','').strip()

            # address
            if d.get('online_event'):
                address = 'online'
            else:
                address = assemble_address(d.get('venue',{}).get('address',{}))

            # fee
            fees = []
            for tc in d.get('ticket_classes', []):
                if tc.get('is_sold_out'):
                    fees.append('sold out')
                elif tc.get('free'):
                    fees.append('free')
                else:
                    c = tc.get('cost',{})
                    cur = c.get('currency', d.get('currency',''))
                    val = c.get('major_value','')
                    if cur and val:
                        fees.append(f"{cur} {val}")
            fee = fees[0] if len(fees)==1 else ("(" + ", ".join(fees) + ")") if fees else ""

            pt = utc_start.astimezone(pytz.timezone('America/Los_Angeles'))

            local.append({
                'keywords'   : ", ".join(keywords),
                'id'         : eid,
                'name'       : d.get('name',{}).get('text','').strip(),
                'organizer'  : org,
                'start_time' : pt,
                'address'    : address,
                'description': (d.get('description',{}).get('text') or '').strip(),
                'url'        : d.get('url','').strip(),
                'fee'        : fee
            })

    with events_lock:
        events.extend(local)

# ─── MARKDOWN ENTRY ────────────────────────────────────────────────────────────

def md_entry(ev):
    return (
        f"- **{ev['name']}**\n"
        f"  - Keywords: {ev['keywords']}\n"
        f"  - Organizer: {ev['organizer']}\n"
        f"  - When: {ev['start_time'].strftime('%Y-%m-%d %H:%M')}\n"
        f"  - Address: {ev['address']}\n"
        f"  - Description: {ev['description'] or '—'}\n"
        f"  - URL: {ev['url']}\n"
        f"  - Fee: {ev['fee'] or '—'}\n"
    )

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    venue_ids = load_ids(INPUT_VENUES)
    # distribute evenly
    chunks = [venue_ids[i::len(TOKENS)] for i in range(len(TOKENS))]

    with ThreadPoolExecutor(max_workers=len(TOKENS)) as exe:
        for i, chunk in enumerate(chunks):
            exe.submit(worker, chunk, i)
    # wait for completion
    exe.shutdown(wait=True)

    # sort and export
    events.sort(key=lambda e: e['start_time'])

    # CSV
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow([
            'Keywords','Event ID','Title','Organizer',
            'Start (PT)','Address','Description','URL','Fee'
        ])
        for e in events:
            w.writerow([
                e['keywords'], e['id'], e['name'], e['organizer'],
                e['start_time'].strftime('%Y-%m-%d %H:%M'),
                e['address'], e['description'], e['url'], e['fee']
            ])

    # Markdown
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write('# Upcoming Career & Leadership Events (Next 30 Days, SoCal)\n\n')
        for e in events:
            f.write(md_entry(e) + '\n')

    duration = time.time() - start
    print(f"Fetched {len(events)} events in {duration:.2f}s; "
          f"API calls: {counters['api_calls']}, rate-limit hits: {counters['rate_limit_hits']}, "
          f"errors: {counters['other_errors']}")

if __name__ == '__main__':
    main()
