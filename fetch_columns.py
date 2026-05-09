#!/usr/bin/env python3
"""
Multi-source RSS fetcher with full-text enrichment via FlareSolverr.

Each source in SOURCES defines:
  feed_url  – RSS/Atom feed to fetch
  output    – output XML file path
  title     – RSS channel title
  desc      – RSS channel description
  filter    – optional substring; if set, only items whose link contains it
               are kept (e.g. "/columns/").  Set to "" or None for all items.

Full article text is fetched (via FlareSolverr) only for new items not
already present in the saved XML.  Existing items are preserved as-is.

FlareSolverr URL: http://localhost:8191/v1  (hard-coded)
"""

import os
import sys
import requests
import feedparser
import xml.etree.ElementTree as ET
from xml.dom import minidom
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from datetime import datetime, timezone
import email.utils

# ---------------------------------------------------------------------------
# Hard-coded configuration
# ---------------------------------------------------------------------------

FLARESOLVERR_URL = "http://localhost:8191/v1"
MAX_ITEMS        = 500

# Add / remove sources here.  Set "filter" to "" to keep all items.
SOURCES = [
    {
        "feed_url": "https://www.banglatribune.com/feed/columns",
        "output":   "columns.xml",
        "title":    "Bangla Tribune – Columns",
        "desc":     "Column articles from Bangla Tribune",
        "filter":   "/columns/",
    },
    {
        "feed_url": "https://www.banglatribune.com/feed/",
        "output":   "banglatribune.xml",
        "title":    "Bangla Tribune",
        "desc":     "Latest news from Bangla Tribune",
        "filter":   "",          # no filter – keep all items
    },
]

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def rfc2822(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return email.utils.format_datetime(dt.astimezone(timezone.utc))


def prettify_xml(elem: ET.Element) -> bytes:
    raw = ET.tostring(elem, encoding="utf-8")
    return minidom.parseString(raw).toprettyxml(indent="  ", encoding="utf-8")


def parse_pubdate(raw: str | None) -> datetime:
    try:
        return dtparser.parse(raw) if raw else datetime.now(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# FlareSolverr
# ---------------------------------------------------------------------------

def flaresolverr_get(target_url: str, timeout: int = 60) -> str:
    payload = {
        "cmd":        "request.get",
        "url":        target_url,
        "maxTimeout": timeout * 1000,
    }
    r = requests.post(
        FLARESOLVERR_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout + 10,
    )
    r.raise_for_status()
    sol = r.json().get("solution") or {}
    html = sol.get("response") or sol.get("body") or r.text
    return html


# ---------------------------------------------------------------------------
# Full-text extraction
# ---------------------------------------------------------------------------

def fetch_full_text(article_url: str, timeout: int = 60) -> str:
    """
    Fetch an article page via FlareSolverr and extract body paragraphs
    from .jw_article_body.  Returns joined text or "" on failure.
    """
    try:
        html = flaresolverr_get(article_url, timeout)
        soup = BeautifulSoup(html, "lxml")
        body = soup.select_one(".jw_article_body")
        if not body:
            print(f"[WARN] .jw_article_body not found in {article_url}",
                  file=sys.stderr)
            return ""
        paragraphs = [
            p.get_text(separator=" ", strip=True)
            for p in body.find_all("p")
            if p.get_text(strip=True)
        ]
        return "\n\n".join(paragraphs)
    except Exception as e:
        print(f"[WARN] Full-text fetch failed for {article_url}: {e}",
              file=sys.stderr)
        return ""


def fetch_featured_image(article_url: str, html: str | None = None) -> str:
    """
    Extract the high-res featured image URL from a Bangla Tribune article page.
    Looks for span[data-ari] JSON first, then falls back to og:image.
    Pass pre-fetched html to avoid a second FlareSolverr round-trip, or
    leave None to fetch the page here.
    """
    import json as _json

    if html is None:
        try:
            html = flaresolverr_get(article_url)
        except Exception:
            return ""

    soup = BeautifulSoup(html, "lxml")

    IMAGE_CDN = "https://cdn.banglatribune.net/contents/uploads/"

    featured = soup.select_one(".featured_image span[data-ari]")
    if featured:
        try:
            data = _json.loads(featured.get("data-ari", "{}"))
            path = data.get("path", "").split("?")[0]
            if path:
                return IMAGE_CDN + path
        except Exception:
            pass

    og = soup.find("meta", property="og:image")
    if og:
        return og.get("content", "")

    return ""


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def load_existing_items(path: str) -> list[dict]:
    """Load saved items from an existing output XML file."""
    items = []
    if not os.path.exists(path):
        return items
    try:
        channel = ET.parse(path).getroot().find("channel")
        if channel is None:
            return items
        for it in channel.findall("item"):
            link = it.findtext("link") or ""
            guid = it.findtext("guid") or link
            items.append({
                "guid":    guid,
                "link":    link,
                "pubDate": it.findtext("pubDate"),
                "element": it,
            })
    except Exception as e:
        print(f"[WARN] Could not load existing XML ({path}): {e}",
              file=sys.stderr)
    return items


def entry_to_element(entry: dict) -> tuple[ET.Element, str]:
    """
    Convert a feedparser entry to an <item> ET.Element.
    Returns (element, rfc2822_pubdate_string).
    """
    item = ET.Element("item")

    ET.SubElement(item, "title").text = entry.get("title", "")
    ET.SubElement(item, "link").text  = entry.get("link", "")

    g = ET.SubElement(item, "guid")
    g.text = entry.get("link") or entry.get("id", "")

    desc = ET.SubElement(item, "description")
    if entry.get("content"):
        desc.text = entry["content"][0].value
    else:
        desc.text = entry.get("summary", "")

    raw_date = (
        entry.get("published")
        or entry.get("updated")
        or entry.get("pubDate")
    )
    pub_str = rfc2822(parse_pubdate(raw_date))
    ET.SubElement(item, "pubDate").text = pub_str

    return item, pub_str


def save_xml(
    path: str,
    final_items: list[dict],
    feed_url: str,
    title: str,
    desc: str,
) -> None:
    rss     = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text         = title
    ET.SubElement(channel, "link").text          = feed_url
    ET.SubElement(channel, "description").text   = desc
    ET.SubElement(channel, "lastBuildDate").text = rfc2822(
        datetime.now(timezone.utc)
    )

    for it in final_items:
        channel.append(it["element"])

    xml_bytes = prettify_xml(rss)
    with open(path, "wb") as f:
        f.write(xml_bytes)
    print(f"[OK] {path} → {len(final_items)} items")


# ---------------------------------------------------------------------------
# Per-source pipeline
# ---------------------------------------------------------------------------

def process_source(src: dict) -> None:
    feed_url   = src["feed_url"]
    out_path   = src["output"]
    title      = src["title"]
    desc       = src["desc"]
    link_filter = src.get("filter") or ""

    print(f"\n[INFO] Source  : {title}")
    print(f"[INFO] Feed    : {feed_url}")
    print(f"[INFO] Output  : {out_path}")
    if link_filter:
        print(f"[INFO] Filter  : '{link_filter}'")

    # 1. Fetch feed via FlareSolverr
    try:
        raw_feed = flaresolverr_get(feed_url)
    except Exception as e:
        print(f"[SKIP] Cannot fetch feed: {e}", file=sys.stderr)
        return

    # 2. Parse with feedparser
    parsed  = feedparser.parse(raw_feed)
    entries = parsed.entries or []

    # 3. Optional link filter
    if link_filter:
        entries = [e for e in entries if link_filter in (e.get("link") or "")]
    print(f"[INFO] Feed entries after filter: {len(entries)}")

    # 4. Build new item list
    new_items: list[dict] = []
    for entry in entries:
        el, pub = entry_to_element(entry)
        new_items.append({
            "guid":    entry.get("link") or entry.get("id", ""),
            "link":    entry.get("link", ""),
            "pubDate": pub,
            "element": el,
        })

    # 5. Load existing saved items
    existing      = load_existing_items(out_path)
    existing_guids = {e["guid"] for e in existing}

    # 6. Identify truly new entries
    truly_new = [n for n in new_items if n["guid"] not in existing_guids]
    print(f"[INFO] {len(truly_new)} new article(s) to enrich.")

    # 7. Fetch full text (and better image) for each new item
    for n in truly_new:
        url = n["link"]
        if not url:
            continue
        print(f"[INFO] Fetching full text : {url}")
        try:
            html = flaresolverr_get(url)
        except Exception as e:
            print(f"[WARN] Could not fetch {url}: {e}", file=sys.stderr)
            continue

        # --- full text ---
        soup = BeautifulSoup(html, "lxml")
        body = soup.select_one(".jw_article_body")
        if body:
            paragraphs = [
                p.get_text(separator=" ", strip=True)
                for p in body.find_all("p")
                if p.get_text(strip=True)
            ]
            full_text = "\n\n".join(paragraphs)
        else:
            full_text = ""
            print(f"[WARN] .jw_article_body not found in {url}",
                  file=sys.stderr)

        if full_text:
            desc_el = n["element"].find("description")
            if desc_el is not None:
                desc_el.text = full_text
            else:
                ET.SubElement(n["element"], "description").text = full_text
        else:
            print(f"[WARN] No full text for {url}; keeping RSS summary.",
                  file=sys.stderr)

        # --- featured image ---
        image_url = fetch_featured_image(url, html=html)
        if image_url:
            ET.SubElement(n["element"], "enclosure").set("url", image_url)

    # 8. Merge: new_items override existing; unknown existing items appended
    store: dict[str, dict] = {}
    for n in new_items:
        store[n["guid"]] = n
    for e in existing:
        if e["guid"] not in store:
            store[e["guid"]] = e

    final = sorted(
        store.values(),
        key=lambda x: parse_pubdate(x["pubDate"]),
        reverse=True,
    )[:MAX_ITEMS]

    # 9. Write XML
    save_xml(out_path, final, feed_url, title, desc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    for source in SOURCES:
        process_source(source)


if __name__ == "__main__":
    main()
