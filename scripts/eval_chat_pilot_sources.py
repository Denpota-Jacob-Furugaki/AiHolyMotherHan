#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple


def post_chat(api_endpoint: str, language: str, question: str) -> Dict[str, Any]:
    url = f"{api_endpoint.rstrip('/')}/chat"

    data = json.dumps(
        {
            "message": question,
            "language": language,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def load_queries(path: str) -> List[Tuple[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    out: List[Tuple[str, str]] = []
    for item in data:
        lang = (item.get("language") or "").strip()
        q = (item.get("q") or "").strip()
        if lang and q:
            out.append((lang, q))

    if not out:
        raise RuntimeError("No queries found in queries file")

    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("api_endpoint")
    parser.add_argument("--queries-json", default=None)
    parser.add_argument("--language", choices=["en", "ja"], default=None)
    parser.add_argument("--query", action="append", default=None)
    parser.add_argument("--pilot-prefix", default="pilot/ocr/")
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()

    if args.queries_json:
        queries = load_queries(args.queries_json)
    else:
        if not args.language or not args.query:
            raise RuntimeError("Provide --queries-json OR both --language and one/more --query")
        queries = [(args.language, q) for q in args.query]

    total = 0
    pilot_hit = 0

    for lang, q in queries:
        total += 1
        print("=" * 80)
        print(f"Q[{lang}]: {q}")

        try:
            res = post_chat(args.api_endpoint, lang, q)
        except urllib.error.HTTPError as e:
            print(f"HTTPError: {e.code} {e.reason}")
            continue
        except Exception as e:
            print(f"Error: {e}")
            continue

        sources = res.get("sources") or []
        pilot_sources = [s for s in sources if (s.get("s3_key") or "").startswith(args.pilot_prefix)]

        ok = len(pilot_sources) > 0
        if ok:
            pilot_hit += 1

        print(f"Pilot sources hit: {ok} ({len(pilot_sources)}/{len(sources)} sources)")

        for s in (pilot_sources[: args.top_n] if pilot_sources else sources[: args.top_n]):
            print(
                f"  - idx={s.get('index')} sim={s.get('similarity')} lang={s.get('language')} file={s.get('filename')}"
            )
            print(f"    s3_key={s.get('s3_key')}")

    print("=" * 80)
    if total:
        print(f"Pilot hit-rate: {pilot_hit}/{total} = {pilot_hit/total:.2%}")
    else:
        print("No queries run")


if __name__ == "__main__":
    sys.exit(main())
