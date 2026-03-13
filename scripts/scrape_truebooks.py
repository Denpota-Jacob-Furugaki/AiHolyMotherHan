"""
scrape_truebooks.py
Scrape truebooks.net (천원사) Korean speech content:
  - 말씀 플러스  (word.php)
  - 청파랑       (cheongpalang.php)
  - 가서전       (gaseojeon.php)
  - 성화랑       (seonghwarang/plus.php)
  - 천일국경전   (service_01.php) — server-rendered content where available

Output: output/truebooks_chunks/<slug>.jsonl
Resume-safe: skips already-written slugs.
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

BASE = "https://truebooks.net/kr"
OUTPUT_DIR = "output/truebooks_chunks"
FRONTIER_FILE  = "output/truebooks_frontier.txt"
VISITED_FILE   = "output/truebooks_visited.txt"
HARVESTED_FILE = "output/truebooks_harvested.txt"
CHUNK_SIZE = 800
CHUNK_OVERLAP = 80
RATE_SLEEP = 0.4

SKIP_PATTERNS = [
    "javascript:", "mailto:", "#",
    ".jpg", ".jpeg", ".png", ".gif", ".pdf",
    ".zip", ".mp3", ".mp4",
    "/intro/", "/etc/", "/notice/", "/purchase/",
    "/member/", "/book/", "/cart/", "/order/",  # skip shop/admin pages
]

# All known entry points
SEED_URLS = [
    f"{BASE}/word/word.php",
    f"{BASE}/word/word.php?startPage=1",
    f"{BASE}/cheongpalang/cheongpalang.php",
    f"{BASE}/gaseojeon/gaseojeon.php",
    f"{BASE}/seonghwarang/plus.php",
    f"{BASE}/service/service_01.php",
    f"{BASE}/service/service_01.php?cate=7",   # 천성경
    f"{BASE}/service/service_01.php?cate=8",   # 평화경
]

# ── HTML extractor ────────────────────────────────────────────────────────────

class TextExtractor(HTMLParser):
    SKIP_TAGS = {"script", "style", "head", "nav", "footer", "header"}
    BLOCK_TAGS = {"p", "br", "div", "h1", "h2", "h3", "h4", "li", "tr", "td"}

    def __init__(self):
        super().__init__()
        self.texts = []
        self.links = []
        self.page_title = ""
        self._in_title = False
        self._depth = {t: 0 for t in self.SKIP_TAGS}

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
        if tag in self.SKIP_TAGS:
            self._depth[tag] = self._depth.get(tag, 0) + 1
        if tag == "a":
            for name, val in attrs:
                if name == "href" and val:
                    self.links.append(val.strip())
        if tag in self.BLOCK_TAGS:
            self.texts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "title":
            self._in_title = False
        if tag in self.SKIP_TAGS:
            self._depth[tag] = max(0, self._depth.get(tag, 0) - 1)

    def handle_data(self, data):
        if self._in_title:
            self.page_title += data.strip()
            return
        if any(self._depth.get(t, 0) > 0 for t in self.SKIP_TAGS):
            return
        s = data.strip()
        if s:
            self.texts.append(s + " ")

    def get_text(self):
        raw = "".join(self.texts)
        raw = re.sub(r"[ \t]{2,}", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch(url: str):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ko,ja;q=0.9,en;q=0.8"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except Exception as e:
        print(f"  [ERR] {url}: {e}")
        return None, []

    try:
        html = raw.decode("utf-8", errors="replace")
    except Exception:
        html = raw.decode("euc-kr", errors="replace")

    parser = TextExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass

    text = parser.get_text()

    # Resolve and filter links
    links = []
    for href in parser.links:
        if not href or any(p in href for p in SKIP_PATTERNS):
            continue
        full = href if href.startswith("http") else urllib.parse.urljoin(url, href)
        full = full.split("#")[0]
        if "truebooks.net/kr" in full and full not in links:
            links.append(full)

    return text, links, parser


# ── Helpers ───────────────────────────────────────────────────────────────────

def slug(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]

def load_set(path: str) -> set:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return {l.strip() for l in f if l.strip()}
    return set()

def append_line(path: str, line: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def chunk_text(text: str):
    chunks, start = [], 0
    while start < len(text):
        c = text[start:start + CHUNK_SIZE].strip()
        if len(c) > 60:
            chunks.append(c)
        start += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks

def detect_language(text: str) -> str:
    ko = len(re.findall(r"[\uac00-\ud7a3]", text))
    ja = len(re.findall(r"[\u3040-\u30ff]", text))
    if ko > ja and ko > 20:
        return "ko"
    if ja > 20:
        return "ja"
    return "en"

def extract_title(parser, url: str) -> str:
    """Extract title from <title> tag, cleaned up."""
    t = parser.page_title
    # HTML title format: "페이지명 - 섹션 - 천원사"
    if t:
        parts = [p.strip() for p in t.split(" - ")]
        # Return the most specific (first) part if meaningful
        if parts[0] and parts[0] != "천원사":
            return parts[0][:100]
    return urllib.parse.urlparse(url).path.strip("/").replace("/", " ")

def is_nav_only(text: str) -> bool:
    """Returns True if the page is essentially just navigation with no speech content."""
    ko = len(re.findall(r"[\uac00-\ud7a3]", text))
    return ko < 150


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("output", exist_ok=True)

    visited   = load_set(VISITED_FILE)
    frontier  = load_set(FRONTIER_FILE)
    harvested = load_set(HARVESTED_FILE)
    done      = {f[:-6] for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")}

    queue = deque((frontier | set(SEED_URLS)) - visited)
    print(f"Start: {len(done)} done, {len(queue)} queued")

    pages = chunks_total = 0

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)
        append_line(VISITED_FILE, url)

        s = slug(url)

        if s in done:
            if url in harvested:
                continue
            # Re-fetch only to harvest links
            print(f"  HARVEST: {url}")
            _, links, _ = fetch(url)
            time.sleep(RATE_SLEEP)
            harvested.add(url)
            append_line(HARVESTED_FILE, url)
            for lnk in links:
                if lnk not in visited:
                    queue.append(lnk)
                    append_line(FRONTIER_FILE, lnk)
            continue

        print(f"Fetch [{pages+1}]: {url}")
        text, links, parser = fetch(url)
        time.sleep(RATE_SLEEP)

        # Enqueue new links
        for lnk in links:
            if lnk not in visited:
                queue.append(lnk)
                append_line(FRONTIER_FILE, lnk)

        if not text or is_nav_only(text):
            print("  SKIP (nav/empty)")
            continue

        lang   = detect_language(text)
        title  = extract_title(parser, url)
        cks    = chunk_text(text)
        if not cks:
            continue

        path_part = urllib.parse.urlparse(url).path.strip("/")
        qs        = urllib.parse.urlparse(url).query

        out = os.path.join(OUTPUT_DIR, f"{s}.jsonl")
        with open(out, "w", encoding="utf-8") as f:
            for i, ck in enumerate(cks):
                record = {
                    "text": ck,
                    "source_url": url,
                    "filename": f"truebooks.net/{path_part}{'?'+qs if qs else ''}",
                    "title": title,
                    "chunk_index": i,
                    "language": lang,
                    "metadata": {
                        "source_type": "web",
                        "website": "truebooks.net",
                        "language": lang,
                        "url": url,
                        "title": title,
                    },
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        done.add(s)
        chunks_total += len(cks)
        pages += 1
        print(f"  → {len(cks)} chunks | lang={lang} | title={title[:40]!r} | total={pages}p/{chunks_total}c")

    print(f"\nFinished: {pages} new pages, {chunks_total} chunks → {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
