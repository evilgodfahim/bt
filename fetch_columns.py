#!/usr/bin/env python3
"""
Fetch RSS via FlareSolverr, keep only entries whose link contains '/columns/',
prepend new items to output XML, keep max N items (default 500).
"""

import os
import sys
import argparse
import json
import time
from urllib.parse import urlparse
import requests
import feedparser
import xml.etree.ElementTree as ET
from xml.dom import minidom
from dateutil import parser as dtparser
from datetime import datetime, timezone
import email.utils

# -------------------------
# Helpers (small definitions)
# -------------------------
def rfc2822(dt):
    """Return RFC-2822 formatted date string for RSS <pubDate>."""
    # dt: aware or naive; convert to aware UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return email.utils.format_datetime(dt_utc)

def prettify_xml(elem):
    raw = ET.tostring(elem, encoding='utf-8')
    parsed = minidom.parseString(raw)
    return parsed.toprettyxml(indent="  ", encoding='utf-8')

def load_existing_items(path):
    items = []
    if not os.path.exists(path):
        return items
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        channel = root.find('channel')
        if channel is None:
            return items
        for it in channel.findall('item'):
            link = it.findtext('link') or ''
            guid = it.findtext('guid') or link
            pubdate = it.findtext('pubDate')
            # keep raw xml element to preserve description etc
            items.append({'guid': guid, 'link': link, 'pubDate': pubdate, 'element': it})
    except Exception:
        return items
    return items

def entry_to_item_element(entry):
    item = ET.Element('item')
    title = ET.SubElement(item, 'title')
    title.text = entry.get('title', '')

    link = ET.SubElement(item, 'link')
    link.text = entry.get('link', '')

    guid = ET.SubElement(item, 'guid')
    guid.text = entry.get('link', entry.get('id', ''))

    # description: prefer content, then summary
    desc_text = ''
    if 'content' in entry and len(entry.content) > 0:
        desc_text = entry.content[0].value
    elif 'summary' in entry:
        desc_text = entry.summary
    desc = ET.SubElement(item, 'description')
    desc.text = desc_text

    # pubDate: parse if available
    pub = entry.get('published') or entry.get('pubDate') or entry.get('updated')
    if pub:
        try:
            dt = dtparser.parse(pub)
            pd = rfc2822(dt)
        except Exception:
            pd = rfc2822(datetime.now(timezone.utc))
    else:
        pd = rfc2822(datetime.now(timezone.utc))
    pd_el = ET.SubElement(item, 'pubDate')
    pd_el.text = pd

    return item, pd

# -------------------------
# Main
# -------------------------
def fetch_via_flaresolverr(flaresolverr_url, target_url, timeout=60):
    """Request FlareSolverr to fetch target_url and return text response.

    Expects FlareSolverr v1-like JSON API:
      POST { "cmd": "request.get", "url": "<target>" }
    Returns response body as text.
    """
    if flaresolverr_url.endswith('/v1'):
        api_url = flaresolverr_url
    else:
        api_url = flaresolverr_url.rstrip('/') + '/v1'

    payload = {
        "cmd": "request.get",
        "url": target_url,
        # increase if needed:
        "maxTimeout": int(timeout * 1000)
    }
    headers = {"Content-Type": "application/json"}
    resp = requests.post(api_url, json=payload, headers=headers, timeout=timeout + 10)
    resp.raise_for_status()
    j = resp.json()
    # typical FlareSolverr shape: {"status":"ok","solution":{"response":"<...>"}}
    if isinstance(j, dict):
        sol = j.get('solution') or {}
        response_html = sol.get('response') or sol.get('body') or j.get('response') or j.get('body')
        if response_html:
            return response_html
    # fallback: if API directly returned text
    return resp.text

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--feed', required=True, help='RSS feed URL')
    p.add_argument('--output', required=True, help='Output XML path (will be created/updated)')
    p.add_argument('--flaresolverr', default=None, help='FlareSolverr base URL (env FLARESOLVERR_URL used if absent)')
    p.add_argument('--max-items', type=int, default=500, help='Maximum items to keep (default 500)')
    args = p.parse_args()

    feed_url = args.feed
    out_path = args.output
    max_items = args.max_items
    fl_url = args.flaresolverr or os.environ.get('FLARESOLVERR_URL')
    if not fl_url:
        print("ERROR: FlareSolverr URL not provided (env FLARESOLVERR_URL or --flaresolverr).", file=sys.stderr)
        sys.exit(2)

    # fetch via flaresolverr
    try:
        body = fetch_via_flaresolverr(fl_url, feed_url, timeout=60)
    except Exception as e:
        print("Fetch error:", e, file=sys.stderr)
        sys.exit(3)

    # parse feed
    parsed = feedparser.parse(body)
    entries = parsed.entries or []

    # filter only entries whose link contains '/columns/'
    filtered = [e for e in entries if '/columns/' in (e.get('link','') or '')]

    # convert to item elements with pubDate
    new_items = []
    for e in filtered:
        try:
            item_el, pubdate = entry_to_item_element(e)
            new_items.append({'element': item_el, 'link': e.get('link',''), 'pubDate': pubdate, 'guid': e.get('link','')})
        except Exception:
            continue

    # load existing items
    existing = load_existing_items(out_path)

    # deduplicate by link/guid: build dict keyed by guid
    keyed = {}
    # start with new items to ensure they are on top
    for it in new_items:
        keyed[it['guid']] = it

    for ex in existing:
        if ex['guid'] not in keyed:
            # keep existing element as-is
            keyed[ex['guid']] = ex

    # sort by pubDate descending; convert pubDate to parseable dt
    def parse_pubdate_string(s):
        try:
            return dtparser.parse(s)
        except Exception:
            return datetime.now(timezone.utc)

    all_items = list(keyed.values())
    all_items_sorted = sorted(all_items, key=lambda x: parse_pubdate_string(x.get('pubDate') or ''), reverse=True)
    # trim
    all_items_sorted = all_items_sorted[:max_items]

    # build RSS XML
    rss = ET.Element('rss', version='2.0')
    channel = ET.SubElement(rss, 'channel')
    title = ET.SubElement(channel, 'title'); title.text = 'BanglaTribune Columns (filtered)'
    link = ET.SubElement(channel, 'link'); link.text = feed_url
    desc = ET.SubElement(channel, 'description'); desc.text = 'Filtered feed: only /columns/ items'
    lastBuild = ET.SubElement(channel, 'lastBuildDate'); lastBuild.text = rfc2822(datetime.now(timezone.utc))

    for it in all_items_sorted:
        el = it.get('element')
        # if element is an Element already, append a copy
        if isinstance(el, ET.Element):
            channel.append(el)
        else:
            # try to parse raw element
            try:
                channel.append(el)
            except Exception:
                continue

    # ensure output dir exists
    outdir = os.path.dirname(out_path)
    if outdir and not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    # write pretty XML
    pretty = prettify_xml(rss)
    with open(out_path, 'wb') as f:
        f.write(pretty)

if __name__ == '__main__':
    main()
