"""
scrape_truelove.py
Crawl truelove.org, decode UTF-16 HTML, extract clean text,
chunk and save JSONL for later embedding into Aurora.

Usage:
    python3 scripts/scrape_truelove.py

Output:
    output/truelove_chunks/<slug>.jsonl
"""

import hashlib
import json
import os
import re
import time
import urllib.request
import urllib.parse
from collections import deque
from html.parser import HTMLParser

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "http://www.truelove.org"
OUTPUT_DIR = "output/truelove_chunks"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 80
RATE_SLEEP = 0.3   # polite crawl delay

# Only crawl pages under these paths (skip images, PDFs, external links)
ALLOWED_EXTENSIONS = {".html", ".htm", ".txt", ""}
SKIP_PATTERNS = [
    ".jpg", ".jpeg", ".png", ".gif", ".pdf", ".zip",
    "youtube.com", "mailto:", "javascript:", "#",
    ".mp3", ".mp4", ".doc", ".docx",
]

# Seed URLs — known content-rich pages
SEED_URLS = [
    "http://www.truelove.org/",
    "http://www.truelove.org/DP-Japanese.htm",
    "http://www.truelove.org/TF-Auto-Japanese.htm",
    "http://www.truelove.org/PM-Japanese.htm",
    "http://www.truelove.org/purelove-jp/index-j.html",
    "http://www.truelove.org/Korean/gateway.html",
    "http://www.truelove.org/Korean/phg.html",
    "http://www.truelove.org/Korean/Blood.html",
    "http://www.truelove.org/csg/Korean-toc.htm",
    "http://www.truelove.org/ucbooks/Japanese/Owner-Japanese.htm",
]

# ── HTML text extractor ───────────────────────────────────────────────────────

class TextExtractor(HTMLParser):
    """Simple HTML → plain text extractor that skips scripts/styles."""
    def __init__(self):
        super().__init__()
        self.texts = []
        self.links = []
        self._skip = False
        self._current_base = ""

    def set_base(self, base_url):
        self._current_base = base_url

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True
        if tag == "a":
            for name, val in attrs:
                if name == "href" and val:
                    self.links.append(val)

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False
        if tag in ("p", "br", "div", "h1", "h2", "h3", "h4", "li", "tr"):
            self.texts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            stripped = data.strip()
            if stripped:
                self.texts.append(stripped + " ")

    def get_text(self):
        raw = "".join(self.texts)
        # Collapse multiple blank lines
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


# ── Fetch + decode ────────────────────────────────────────────────────────────

def fetch_page(url: str):
    """Fetch URL, return (text, links). Handles UTF-16 and UTF-8."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except Exception as e:
        print(f"  [FETCH ERROR] {url}: {e}")
        return None, []

    # Detect encoding
    try:
        if raw[:2] in (b'\xff\xfe', b'\xfe\xff'):
            text_html = raw.decode("utf-16")
        else:
            # Try charset from meta tag, fallback to utf-8
            sniff = raw[:1000].decode("latin-1", errors="replace").lower()
            if "charset=euc-kr" in sniff or "charset=ks_c" in sniff:
                text_html = raw.decode("euc-kr", errors="replace")
            elif "charset=shift_jis" in sniff:
                text_html = raw.decode("shift_jis", errors="replace")
            else:
                text_html = raw.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [DECODE ERROR] {url}: {e}")
        return None, []

    parser = TextExtractor()
    parser.set_base(url)
    try:
        parser.feed(text_html)
    except Exception:
        pass

    text = parser.get_text()

    # Resolve links
    links = []
    for href in parser.links:
        href = href.strip()
        if not href or any(p in href.lower() for p in SKIP_PATTERNS):
            continue
        if href.startswith("http"):
            full = href
        else:
            full = urllib.parse.urljoin(url, href)
        # Only stay on truelove.org
        if "truelove.org" in full:
            # Normalize: strip fragment
            full = full.split("#")[0].rstrip("/")
            if full:
                links.append(full)

    return text, list(set(links))


# ── Chunking ──────────────────────────────────────────────────────────────────

def chunk_text(text: str, size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        chunk = text[start:end].strip()
        if len(chunk) > 50:  # Skip tiny chunks
            chunks.append(chunk)
        start += size - overlap
    return chunks


# ── Language detection ────────────────────────────────────────────────────────

def detect_language(text: str) -> str:
    ko = len(re.findall(r'[\uac00-\ud7a3]', text))
    ja = len(re.findall(r'[\u3040-\u30ff\u31f0-\u31ff]', text))
    if ko > ja and ko > 20:
        return "ko"
    if ja > 20:
        return "ja"
    return "en"


# ── Main crawler ──────────────────────────────────────────────────────────────

def url_to_slug(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


FRONTIER_FILE  = "output/truelove_frontier.txt"
VISITED_FILE   = "output/truelove_visited.txt"
HARVESTED_FILE = "output/truelove_harvested.txt"  # links already harvested from done pages


def load_set(path: str) -> set:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def append_line(path: str, line: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("output", exist_ok=True)

    # Load persisted state
    visited    = load_set(VISITED_FILE)
    frontier   = load_set(FRONTIER_FILE)
    harvested  = load_set(HARVESTED_FILE)
    done_slugs = {f.replace(".jsonl", "") for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")}

    # Seed queue with unvisited seeds + persisted frontier
    queue = deque((frontier | set(SEED_URLS)) - visited)
    print(f"Resuming: {len(done_slugs)} pages done, {len(queue)} in queue")

    total_chunks = 0
    total_pages  = 0

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        append_line(VISITED_FILE, url)

        slug = url_to_slug(url)

        # Check extension
        path = urllib.parse.urlparse(url).path
        ext  = os.path.splitext(path)[1].lower()
        if ext and ext not in ALLOWED_EXTENSIONS:
            continue

        if slug in done_slugs:
            # Already have chunks — fetch once more just to harvest links
            if url in harvested:
                continue
            print(f"  HARVEST LINKS: {url}")
            _, links = fetch_page(url)
            time.sleep(RATE_SLEEP)
            harvested.add(url)
            append_line(HARVESTED_FILE, url)
            for link in links:
                if link not in visited:
                    queue.append(link)
                    append_line(FRONTIER_FILE, link)
            continue

        print(f"Fetching [{total_pages+1}]: {url}")
        text, links = fetch_page(url)
        time.sleep(RATE_SLEEP)

        # Enqueue + persist discovered links regardless of text quality
        for link in links:
            if link not in visited:
                queue.append(link)
                append_line(FRONTIER_FILE, link)

        if not text or len(text) < 100:
            print(f"  SKIP (too short)")
            continue

        lang        = detect_language(text)
        source_path = urllib.parse.urlparse(url).path.strip("/") or "index"
        chunks      = chunk_text(text)
        if not chunks:
            continue

        out_path = os.path.join(OUTPUT_DIR, f"{slug}.jsonl")
        with open(out_path, "w", encoding="utf-8") as f:
            for i, chunk in enumerate(chunks):
                record = {
                    "text": chunk,
                    "source_url": url,
                    "filename": f"truelove.org/{source_path}",
                    "chunk_index": i,
                    "language": lang,
                    "metadata": {
                        "source_type": "web",
                        "website": "truelove.org",
                        "language": lang,
                        "url": url,
                    },
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        done_slugs.add(slug)
        total_chunks += len(chunks)
        total_pages  += 1
        print(f"  → {len(chunks)} chunks, lang={lang}, total pages={total_pages} chunks={total_chunks}")

    print(f"\nDone: {total_pages} new pages, {total_chunks} chunks saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
