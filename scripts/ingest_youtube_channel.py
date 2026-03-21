#!/usr/bin/env python3
"""
YouTube チャンネル → Whisper 文字起こし → DB 格納パイプライン

複数チャンネル対応:
  @UCJAPANch      世界平和統一家庭連合公式チャンネル
  @hjpeacetv8814  HJ PeaceTV
  @onthewayhome3976  帰り道チャンネル

Usage:
    python ingest_youtube_channel.py --channel ucjapan --scan
    python ingest_youtube_channel.py --channel ucjapan --batch 50 --resume
    python ingest_youtube_channel.py --channel hjpeace --batch 50
    python ingest_youtube_channel.py --channel ontheway --batch 50
    python ingest_youtube_channel.py --channel all --fetch   # Fetch video lists for all
"""

import argparse
import glob
import json
import os
import re
import subprocess
import time
from typing import List, Dict, Any, Tuple

import boto3
from openai import OpenAI
from pytubefix import YouTube

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
LAMBDA_NAME = "mini-han-chat"
EMBEDDING_MODEL = "text-embedding-3-large"
WHISPER_MODEL = "whisper-1"
CHUNK_SIZE = 800  # characters per chunk
CHUNK_OVERLAP = 100

# Persistent data directory (in repo, survives reboot, goes to GitHub)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data", "youtube")
os.makedirs(DATA_DIR, exist_ok=True)

# Channel registry
CHANNELS = {
    "ucjapan": {
        "handle": "@UCJAPANch",
        "name": "世界平和統一家庭連合公式チャンネル",
        "url": "https://www.youtube.com/@UCJAPANch",
        "video_list": os.path.join(DATA_DIR, "ucjapan_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_ucjapan_progress.json"),
        "audio_dir": "/tmp/ucjapan_audio",
        "default_lang": "ja",
    },
    "hjpeace": {
        "handle": "@hjpeacetv8814",
        "name": "HJ PeaceTV",
        "url": "https://www.youtube.com/@hjpeacetv8814",
        "video_list": os.path.join(DATA_DIR, "hjpeace_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_hjpeace_progress.json"),
        "audio_dir": "/tmp/hjpeace_audio",
        "default_lang": "ja",
    },
    "ontheway": {
        "handle": "@onthewayhome3976",
        "name": "帰り道チャンネル (On The Way Home)",
        "url": "https://www.youtube.com/@onthewayhome3976",
        "video_list": os.path.join(DATA_DIR, "onthewayhome_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_ontheway_progress.json"),
        "audio_dir": "/tmp/ontheway_audio",
        "default_lang": "ja",
    },
    "adminffwpu": {
        "handle": "@adminffwpu948",
        "name": "FFWPU Australia",
        "url": "https://www.youtube.com/@adminffwpu948",
        "video_list": os.path.join(DATA_DIR, "adminffwpu_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_adminffwpu_progress.json"),
        "audio_dir": "/tmp/adminffwpu_audio",
        "default_lang": "en",
    },
    "ffwpuuk": {
        "handle": "@FFWPUUK",
        "name": "FFWPU UK",
        "url": "https://www.youtube.com/@FFWPUUK",
        "video_list": os.path.join(DATA_DIR, "ffwpuuk_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_ffwpuuk_progress.json"),
        "audio_dir": "/tmp/ffwpuuk_audio",
        "default_lang": "en",
    },
    "njfamily": {
        "handle": "@NJFamilyChurch",
        "name": "NJ Family Church",
        "url": "https://www.youtube.com/@NJFamilyChurch",
        "video_list": os.path.join(DATA_DIR, "njfamily_videos.tsv"),
        "progress_file": os.path.join(DATA_DIR, "yt_njfamily_progress.json"),
        "audio_dir": "/tmp/njfamily_audio",
        "default_lang": "en",
    },
}

YT_DLP = os.environ.get("YT_DLP", "/Users/denpotafurugaki/Library/Python/3.9/bin/yt-dlp")

# Active channel config (set in main)
CHANNEL_CFG = None

# ============================================================
# Music / non-speech filters — skip these
# ============================================================

MUSIC_KEYWORDS = [
    "聖歌", "マリンバ", "合唱曲", "ヒーリング聖歌", "Hymn", "Chorus",
    "Marimba", "hymn", "chorus", "Healing Hymn",
    "オーケストラ伴奏", "混声三部", "混声四部",
    "帝王蝶",
]

def is_music_video(title: str) -> bool:
    for kw in MUSIC_KEYWORDS:
        if kw in title:
            return True
    return False

# ============================================================
# Speaker identification from video title
# ============================================================

SPEAKER_PATTERNS = [
    (r"田中(?:富広)?会長", "田中富広会長 (Chairman Tomihiro Tanaka)"),
    (r"(?:Chairman|President)\s*(?:Tomohiro\s*)?Tanaka", "田中富広会長 (Chairman Tomihiro Tanaka)"),
    (r"中川晴久(?:牧師)?", "中川晴久牧師 (Pastor Haruhisa Nakagawa)"),
    (r"(?:Rev\.\s*)?(?:Haruhisa\s*)?Nakagawa", "中川晴久牧師 (Pastor Haruhisa Nakagawa)"),
    (r"砂川(?:龍一)?牧師", "砂川龍一牧師 (Pastor Ryuichi Sunagawa)"),
    (r"Sunagawa", "砂川龍一牧師 (Pastor Ryuichi Sunagawa)"),
    (r"今中(?:信人|のぶ|ノブ)", "今中信人 (Nobu Imanaka)"),
    (r"(?:Nobu(?:to)?\s*)?Imanaka", "今中信人 (Nobu Imanaka)"),
    (r"今中カナ|(?:Kana\s*)?Imanaka", "今中カナ (Kana Imanaka)"),
    (r"文鮮明(?:師|先生|総裁)?", "文鮮明先生 (Rev. Sun Myung Moon)"),
    (r"(?:Rev\.\s*)?(?:Sun\s*Myung\s*)?Moon", "文鮮明先生 (Rev. Sun Myung Moon)"),
    (r"韓鶴子(?:総裁|女史|先生)?", "韓鶴子総裁 (Dr. Hak Ja Han Moon)"),
    (r"(?:Dr\.\s*)?Hak\s*Ja\s*Han", "韓鶴子総裁 (Dr. Hak Ja Han Moon)"),
    (r"真のお母様|True Mother", "韓鶴子総裁 (Dr. Hak Ja Han Moon)"),
    (r"真のお父様|True Father", "文鮮明先生 (Rev. Sun Myung Moon)"),
    (r"統一原理講座|Unification Principle Lecture", "統一原理講座"),
    (r"教義紹介|Doctrine Introduction", "教義紹介"),
]

def identify_speaker(title: str) -> str:
    for pattern, speaker in SPEAKER_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return speaker
    if CHANNEL_CFG:
        return CHANNEL_CFG["name"]
    return "世界平和統一家庭連合"

def categorize_video(title: str) -> str:
    if is_music_video(title):
        return "music"
    if any(k in title for k in ["講演", "講座", "Lecture", "lecture"]):
        return "lecture"
    if any(k in title for k in ["記者会見", "Press Conference"]):
        return "press_conference"
    if any(k in title for k in ["インタビュー", "interview", "Interview"]):
        return "interview"
    if any(k in title for k in ["ラリー", "Rally", "マーチ"]):
        return "rally"
    if any(k in title for k in ["ドキュメンタリー", "Documentary", "丹心", "天国の扉"]):
        return "documentary"
    if any(k in title for k in ["メッセージ", "Message", "話", "Speech"]):
        return "speech"
    return "general"

# ============================================================
# Clients
# ============================================================

secrets_client = None
lambda_client = None
openai_client = None


def init_clients():
    global secrets_client, lambda_client, openai_client
    session = boto3.Session(region_name=AWS_REGION)
    secrets_client = session.client("secretsmanager")
    lambda_client = session.client("lambda")
    resp = secrets_client.get_secret_value(SecretId="rag/openai-api-key")
    openai_client = OpenAI(api_key=resp["SecretString"])
    print("✓ Clients initialized", flush=True)


# ============================================================
# Audio download
# ============================================================

def download_audio(video_id: str) -> str:
    """Download audio from YouTube video using pytubefix. Returns path to audio file."""
    audio_dir = CHANNEL_CFG["audio_dir"]
    os.makedirs(audio_dir, exist_ok=True)
    out_path = os.path.join(audio_dir, f"{video_id}.mp4")

    if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
        return out_path  # already downloaded

    url = f"https://www.youtube.com/watch?v={video_id}"
    yt = YouTube(url)
    audio = yt.streams.filter(only_audio=True).order_by("abr").first()
    if not audio:
        raise RuntimeError(f"No audio stream for {video_id}")
    audio.download(output_path=audio_dir, filename=f"{video_id}.mp4")
    return out_path


# ============================================================
# Whisper transcription
# ============================================================

WHISPER_MAX_SIZE = 24 * 1024 * 1024  # 24 MB safety margin

def transcribe_audio(audio_path: str) -> str:
    """Transcribe audio file using OpenAI Whisper API."""
    file_size = os.path.getsize(audio_path)

    if file_size > WHISPER_MAX_SIZE:
        # Split into segments using ffmpeg
        return _transcribe_large_file(audio_path)

    lang_hint = CHANNEL_CFG.get("default_lang", "ja") if CHANNEL_CFG else "ja"
    with open(audio_path, "rb") as f:
        response = openai_client.audio.transcriptions.create(
            model=WHISPER_MODEL,
            file=f,
            language=lang_hint,
            response_format="text",
        )
    return response.strip()


def _transcribe_large_file(audio_path: str) -> str:
    """Split large audio into 20-minute segments and transcribe each."""
    lang_hint = CHANNEL_CFG.get("default_lang", "ja") if CHANNEL_CFG else "ja"
    segment_dir = audio_path + "_segments"
    os.makedirs(segment_dir, exist_ok=True)

    ffmpeg_bin = "/tmp/ffmpeg"
    if not os.path.exists(ffmpeg_bin):
        ffmpeg_bin = "ffmpeg"  # fallback to PATH
    cmd = [
        ffmpeg_bin, "-i", audio_path, "-f", "segment",
        "-segment_time", "1200",  # 20 minutes
        "-ac", "1", "-ab", "48k",
        "-y", os.path.join(segment_dir, "seg_%03d.mp3"),
    ]
    subprocess.run(cmd, capture_output=True, timeout=600)

    segments = sorted(glob.glob(os.path.join(segment_dir, "seg_*.mp3")))
    texts = []
    for seg in segments:
        with open(seg, "rb") as f:
            response = openai_client.audio.transcriptions.create(
                model=WHISPER_MODEL, file=f, language=lang_hint, response_format="text",
            )
        texts.append(response.strip())

    for seg in segments:
        os.remove(seg)
    try: os.rmdir(segment_dir)
    except: pass

    return " ".join(texts)


# ============================================================
# Chunking
# ============================================================

def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end < len(text):
            for sep in ["。", ".", "！", "!", "？", "?", "、", ",", " "]:
                last_sep = text[start:end].rfind(sep)
                if last_sep > chunk_size * 0.5:
                    end = start + last_sep + 1
                    break
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start = end - overlap
    return chunks


# ============================================================
# Lambda helpers
# ============================================================

def store_chunks(chunks: List[Dict]) -> int:
    payload = json.dumps({"body": json.dumps({"message": "__insert_chunks__", "chunks": chunks})})
    r = lambda_client.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=payload)
    result = json.loads(r["Payload"].read())
    body = json.loads(result.get("body", "{}"))
    return body.get("inserted", 0)


def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    try:
        response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
        return [d.embedding for d in response.data]
    except Exception as e:
        print(f"    Embedding error: {e}")
        return [[0.0] * 3072 for _ in texts]


# ============================================================
# Progress
# ============================================================

def load_progress() -> Dict[str, Any]:
    pf = CHANNEL_CFG["progress_file"]
    if os.path.exists(pf):
        with open(pf, "r") as f:
            return json.load(f)
    return {"processed_ids": [], "success": 0, "skipped": 0, "errors": 0, "chunks_stored": 0}


def save_progress(progress: Dict[str, Any]):
    progress["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(CHANNEL_CFG["progress_file"], "w") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


# ============================================================
# Main
# ============================================================

def process_video(video: Dict, progress: Dict) -> int:
    vid_id = video["id"]
    title = video["title"]
    duration = video.get("duration", 0)

    # Skip music
    if is_music_video(title):
        print(f"  SKIP (music): {title[:50]}", flush=True)
        progress["skipped"] += 1
        progress["processed_ids"].append(vid_id)
        return 0

    # Skip very short videos (< 30s)
    if duration and duration < 30:
        print(f"  SKIP (short): {title[:50]}", flush=True)
        progress["skipped"] += 1
        progress["processed_ids"].append(vid_id)
        return 0

    # 1) Download audio
    print(f"  DL...", end="", flush=True)
    audio_path = download_audio(vid_id)
    audio_mb = os.path.getsize(audio_path) / (1024 * 1024)
    print(f" {audio_mb:.1f}MB", end="", flush=True)

    # 2) Whisper transcription
    print(f" -> Whisper...", end="", flush=True)
    text = transcribe_audio(audio_path)

    if not text or len(text.strip()) < 30:
        print(f" (empty transcript)", flush=True)
        progress["errors"] += 1
        progress["processed_ids"].append(vid_id)
        return 0

    # 3) Identify speaker and category
    speaker = identify_speaker(title)
    category = categorize_video(title)
    video_url = f"https://www.youtube.com/watch?v={vid_id}"

    # 4) Chunk
    chunks = chunk_text(text)
    print(f" {len(text)}ch/{len(chunks)}chunks", end="", flush=True)

    # 5) Build metadata
    ch_handle = CHANNEL_CFG["handle"]
    ch_name = CHANNEL_CFG["name"]
    metadata = {
        "source_type": "youtube",
        "channel": ch_name,
        "channel_handle": ch_handle,
        "video_id": vid_id,
        "video_url": video_url,
        "video_title": title,
        "speaker": speaker,
        "category": category,
        "language": CHANNEL_CFG.get("default_lang", "ja"),
        "duration_seconds": int(duration) if duration else None,
    }

    chunk_data = []
    for i, ct in enumerate(chunks):
        chunk_data.append({
            "s3_key": f"youtube/{ch_handle}/{vid_id}",
            "filename": f"[YT] {title}",
            "chunk_index": i,
            "chunk_text": ct,
            "metadata": metadata,
        })

    # 6) Embed
    print(f" -> Embed", end="", flush=True)
    texts = [c["chunk_text"] for c in chunk_data]
    embeddings = get_embeddings_batch(texts)
    for c, emb in zip(chunk_data, embeddings):
        c["embedding"] = emb

    # 7) Store
    print(f" -> Store", end="", flush=True)
    stored = store_chunks(chunk_data)
    progress["chunks_stored"] += stored
    progress["success"] += 1
    progress["processed_ids"].append(vid_id)

    # Clean up audio to save disk
    try:
        os.remove(audio_path)
    except:
        pass

    print(f" OK [{speaker[:20]}] {stored}stored", flush=True)
    return stored


def load_video_list() -> List[Dict]:
    vf = CHANNEL_CFG["video_list"]
    if not os.path.exists(vf):
        print(f"Video list not found: {vf}", flush=True)
        print(f"Run: --fetch to download it first", flush=True)
        return []
    videos = []
    with open(vf) as f:
        for line in f:
            parts = line.strip().split("\\t")
            if len(parts) >= 2:
                videos.append({
                    "id": parts[0],
                    "title": parts[1],
                    "duration": float(parts[3]) if len(parts) > 3 and parts[3] != "NA" else 0,
                })
    return videos


def fetch_video_list(channel_key: str):
    """Download video list from YouTube using yt-dlp."""
    cfg = CHANNELS[channel_key]
    url = cfg["url"] + "/videos"
    out = cfg["video_list"]
    print(f"Fetching video list for {cfg['name']}...", flush=True)
    cmd = [
        YT_DLP, "--flat-playlist",
        "--print", "%(id)s\\t%(title)s\\t%(upload_date)s\\t%(duration)s",
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    with open(out, "w") as f:
        f.write(result.stdout)
    count = len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0
    print(f"  Saved {count} videos to {out}", flush=True)
    return count


def main():
    global CHANNEL_CFG
    parser = argparse.ArgumentParser(description="Ingest YouTube channel via Whisper")
    parser.add_argument("--channel", required=True, help="Channel key: ucjapan, hjpeace, ontheway, or all")
    parser.add_argument("--batch", type=int, default=20, help="Videos per batch")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    parser.add_argument("--scan", action="store_true", help="Show categorization summary")
    parser.add_argument("--fetch", action="store_true", help="Fetch video list from YouTube")
    parser.add_argument("--update", action="store_true", help="Check for new videos and ingest them (auto-mode)")
    args = parser.parse_args()

    # Handle --fetch for all channels
    if args.fetch:
        targets = list(CHANNELS.keys()) if args.channel == "all" else [args.channel]
        for key in targets:
            if key not in CHANNELS:
                print(f"Unknown channel: {key}")
                continue
            fetch_video_list(key)
        return

    # Handle --update: re-fetch list, find new videos, process all new ones
    if args.update:
        targets = list(CHANNELS.keys()) if args.channel == "all" else [args.channel]
        init_clients()
        for key in targets:
            if key not in CHANNELS:
                continue
            CHANNEL_CFG = CHANNELS[key]
            print(f"\n{'='*60}", flush=True)
            print(f"UPDATE CHECK: {CHANNEL_CFG['name']}", flush=True)
            print(f"{'='*60}", flush=True)

            # Re-fetch video list
            fetch_video_list(key)

            all_videos = load_video_list()
            if not all_videos:
                continue

            progress = load_progress()
            processed_set = set(progress["processed_ids"])
            new_videos = [v for v in all_videos if v["id"] not in processed_set]

            if not new_videos:
                print(f"  No new videos found.", flush=True)
                continue

            print(f"  Found {len(new_videos)} new video(s)!", flush=True)

            for i, video in enumerate(new_videos):
                print(f"  [{i+1}/{len(new_videos)}] {video['title'][:45]}...", end="", flush=True)
                try:
                    process_video(video, progress)
                except Exception as e:
                    print(f" ERROR: {e}", flush=True)
                    progress["errors"] += 1
                    progress["processed_ids"].append(video["id"])
                save_progress(progress)
                time.sleep(0.3)

            print(f"  Update done: {len(new_videos)} processed, {progress['chunks_stored']} total chunks", flush=True)

        print(f"\nAll channels updated at {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        return

    if args.channel not in CHANNELS:
        print(f"Unknown channel: {args.channel}. Use: {', '.join(CHANNELS.keys())}")
        return

    CHANNEL_CFG = CHANNELS[args.channel]

    # Migrate old progress file for ucjapan
    if args.channel == "ucjapan" and not os.path.exists(CHANNEL_CFG["progress_file"]):
        old = "/tmp/youtube_ingest_progress.json"
        if os.path.exists(old):
            import shutil
            shutil.copy2(old, CHANNEL_CFG["progress_file"])
            print(f"Migrated old progress -> {CHANNEL_CFG['progress_file']}", flush=True)

    print("=" * 60, flush=True)
    print(f"YouTube → Whisper → DB: {CHANNEL_CFG['name']}", flush=True)
    print(f"Handle: {CHANNEL_CFG['handle']}", flush=True)
    print("=" * 60, flush=True)

    all_videos = load_video_list()
    if not all_videos:
        return
    print(f"Total videos: {len(all_videos)}", flush=True)

    # Categorize
    categories = {}
    speakers = {}
    for v in all_videos:
        cat = categorize_video(v["title"])
        categories[cat] = categories.get(cat, 0) + 1
        spk = identify_speaker(v["title"])
        speakers[spk] = speakers.get(spk, 0) + 1

    music_count = categories.get("music", 0)
    processable = len(all_videos) - music_count

    print(f"Music (skip): {music_count}")
    print(f"Processable: {processable}", flush=True)

    if args.scan:
        print(f"\nCategories:")
        for cat, cnt in sorted(categories.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {cnt}")
        print(f"\nSpeakers:")
        for spk, cnt in sorted(speakers.items(), key=lambda x: -x[1]):
            print(f"  {spk}: {cnt}")
        return

    init_clients()

    progress = load_progress() if args.resume else {
        "processed_ids": [], "success": 0, "skipped": 0, "errors": 0, "chunks_stored": 0
    }

    to_process = [v for v in all_videos if v["id"] not in progress["processed_ids"]]
    to_process = to_process[:args.batch]

    print(f"\nBatch: {len(to_process)} videos")
    print(f"Already done: {len(progress['processed_ids'])} videos, {progress['chunks_stored']} chunks\n", flush=True)

    for i, video in enumerate(to_process):
        print(f"[{i+1}/{len(to_process)}] {video['title'][:45]}...", end="", flush=True)
        try:
            process_video(video, progress)
        except Exception as e:
            print(f" ERROR: {e}", flush=True)
            progress["errors"] += 1
            progress["processed_ids"].append(video["id"])
        save_progress(progress)
        time.sleep(0.3)

    print("\n" + "=" * 60, flush=True)
    print(f"Done: {progress['success']} ok, {progress['skipped']} skip, {progress['errors']} err", flush=True)
    print(f"Chunks stored: {progress['chunks_stored']}", flush=True)
    print(f"Remaining: {len(all_videos) - len(progress['processed_ids'])}", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
