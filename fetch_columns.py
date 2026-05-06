#!/usr/bin/env python3
"""
Fetch RSS via FlareSolverr, keep only entries whose link contains '/columns/',
prepend new items to output XML, keep max N items (default 500).
Full article text is fetched for new items only; existing items are left as-is.
FlareSolverr URL is hardcoded as: http://localhost:8191/v1
"""

import os
import sys
import argparse
import requests
import feedparser
import xml.etree.ElementTree as ET
from xml.dom import minidom
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from datetime import datetime, timezone
import email.utils

# -------------------------
# HARD-CODED FLARESOLVERR URL
# -------------------------
FLARESOLVERR_URL = "http://localhost:8191/v1"


def rfc2822(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return email.utils.format_datetime(dt.astimezone(timezone.utc))


def prettify_xml(elem):
    raw = ET.tostring(elem, encoding='utf-8')
    return minidom.parseString(raw).toprettyxml(indent="  ", encoding='utf-8')


def load_existing_items(path):
    items = []
    if not os.path.exists(path):
        return items

    try:
        tree = ET.parse(path)
        channel = tree.getroot().find('channel')
        if channel is None:
            return items

        for it in channel.findall('item'):
            link = it.findtext('link') or ''
            guid = it.findtext('guid') or link
            pub = it.findtext('pubDate')
            items.append({
                'guid': guid,
                'link': link,
                'pubDate': pub,
                'element': it
            })
    except Exception:
        return items

    return items


def entry_to_item_element(entry):
    item = ET.Element('item')

    t = ET.SubElement(item, 'title')
    t.text = entry.get('title', '')

    l = ET.SubElement(item, 'link')
    l.text = entry.get('link', '')

    g = ET.SubElement(item, 'guid')
    g.text = entry.get('link', entry.get('id', ''))

    d = ET.SubElement(item, 'description')
    if 'content' in entry and entry.content:
        d.text = entry.content[0].value
    else:
        d.text = entry.get('summary', '')

    pub = entry.get('published') or entry.get('updated') or entry.get('pubDate')
    try:
        dt = dtparser.parse(pub) if pub else datetime.now(timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)

    pd = ET.SubElement(item, 'pubDate')
    pd.text = rfc2822(dt)

    return item, pd.text


def fetch_via_flaresolverr(url, target, timeout=60):
    payload = {
        "cmd": "request.get",
        "url": target,
        "maxTimeout": timeout * 1000
    }
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, json=payload, headers=headers, timeout=timeout + 10)
    r.raise_for_status()

    j = r.json()
    sol = j.get("solution") or {}
    html = sol.get("response") or sol.get("body")

    return html if html else r.text


def fetch_full_text(article_url, timeout=60):
    """
    Fetch the full article text from an article page via FlareSolverr.
    Extracts paragraphs from .jw_article_body and returns them joined.
    Returns empty string on failure.
    """
    try:
        html = fetch_via_flaresolverr(FLARESOLVERR_URL, article_url, timeout)
        soup = BeautifulSoup(html, "lxml")
        body = soup.select_one(".jw_article_body")
        if not body:
            print(f"[WARN] .jw_article_body not found in {article_url}", file=sys.stderr)
            return ""
        paragraphs = [p.get_text(separator=" ", strip=True)
                      for p in body.find_all("p")
                      if p.get_text(strip=True)]
        return "\n\n".join(paragraphs)
    except Exception as e:
        print(f"[WARN] Full-text fetch failed for {article_url}: {e}", file=sys.stderr)
        return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--feed', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--max-items', type=int, default=500)
    args = parser.parse_args()

    feed_url = args.feed
    out_path = args.output
    max_items = args.max_items

    body = fetch_via_flaresolverr(FLARESOLVERR_URL, feed_url, timeout=60)
    parsed = feedparser.parse(body)
    entries = parsed.entries or []

    filtered = [e for e in entries if "/columns/" in (e.get("link", "") or "")]

    new_items = []
    for e in filtered:
        el, pub = entry_to_item_element(e)
        new_items.append({
            'guid': e.get('link', ''),
            'link': e.get('link', ''),
            'pubDate': pub,
            'element': el
        })

    existing = load_existing_items(out_path)

    # Determine which items are truly new (not already in the saved XML)
    existing_guids = {e['guid'] for e in existing}
    truly_new = [n for n in new_items if n['guid'] not in existing_guids]

    print(f"[INFO] {len(truly_new)} new article(s) to fetch full text for.")

    # Fetch full article text only for new items
    for n in truly_new:
        url = n['link']
        if not url:
            continue
        print(f"[INFO] Fetching full text: {url}")
        full_text = fetch_full_text(url)
        if full_text:
            # Replace the description element's text with full article content
            desc_el = n['element'].find('description')
            if desc_el is not None:
                desc_el.text = full_text
            else:
                d = ET.SubElement(n['element'], 'description')
                d.text = full_text
        else:
            print(f"[WARN] No full text retrieved for {url}, keeping RSS summary.", file=sys.stderr)

    # Merge: new items take precedence; existing items keep their existing data
    store = {}
    for n in new_items:
        store[n['guid']] = n
    for e in existing:
        if e['guid'] not in store:
            store[e['guid']] = e

    def pd(x):
        try:
            return dtparser.parse(x or "")
        except Exception:
            return datetime.now(timezone.utc)

    final = sorted(store.values(), key=lambda x: pd(x['pubDate']), reverse=True)
    final = final[:max_items]

    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = "BanglaTribune Columns (Filtered)"
    ET.SubElement(channel, "link").text = feed_url
    ET.SubElement(channel, "description").text = "Filtered feed containing only items with /columns/"
    ET.SubElement(channel, "lastBuildDate").text = rfc2822(datetime.now(timezone.utc))

    for it in final:
        channel.append(it['element'])

    xml = prettify_xml(rss)
    with open(out_path, "wb") as f:
        f.write(xml)

    print(f"[OK] {out_path} -> {len(final)} items")


if __name__ == "__main__":
    main()
