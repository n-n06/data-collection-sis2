from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import time

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import (
    ARTIST_MBID,
    ARTIST_NAME,
    DATA_DIR,
    MUSICBRAINZ_URL,
    USER_AGENT
)

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)


def classify_format(format: str) -> str | None:
    value = format.casefold()
    if not value or "digital media" in value or "streaming" in value:
        return None
    if "cassette" in value:
        return "Cassette"
    if "8-track" in value or "8 track" in value:
        return "8-Track"
    if "minidisc" in value:
        return "MiniDisc"
    if "vinyl" in value:
        return "Vinyl"
    if "cd" in value or "sacd" in value:
        return "CD"
    if "dvd" in value or "blu-ray" in value or "bluray" in value:
        return "DVD/Blu-ray"
    return "Other physical"


def normalize_release(release: dict[str, Any], collected_at: str) -> dict[str, Any] | None:
    media = release.get("media") or []

    # format of releast
    formats = [str(item.get("format") or "").strip() for item in media]
    formats = [item for item in formats if item]
    format_detail = " + ".join(formats)
    format_category = classify_format(format_detail)
    if format_category is None:
        return None
    

    # dates
    release_date = str(release.get("date") or "").strip()
    year_match = re.match(r"^(\d{4})", release_date)
    if not year_match:
        return None
    release_year = int(year_match.group(1))
    date_precision = {4: "year", 7: "month", 10: "day"}.get(len(release_date), "partial")


    # num number of tracks
    track_count = sum(int(item.get("track-count") or 0) for item in media)

    # labl info
    labels = release.get("label-info") or []
    label_names = [str((item.get("label") or {}).get("name") or "").strip() for item in labels]

    # catalog num - str
    catalog_numbers = [str(item.get("catalog-number") or "").strip() for item in labels]

    # release group and artist credits
    group = release.get("release-group") or {}
    artist_credit = release.get("artist-credit") or []
    credit_name = "".join(
        str(item.get("name") or (item.get("artist") or {}).get("name") or "") + str(item.get("joinphrase") or "")
        for item in artist_credit
    ).strip()


    # relese UUID
    release_id = str(release.get("id") or "")

    if not UUID_RE.match(release_id):
        return None


    return {
        "release_id": release_id,
        "artist_mbid": ARTIST_MBID,
        "artist_name": ARTIST_NAME,
        "artist_credit_display": credit_name or ARTIST_NAME,
        "release_group_id": str(group.get("id") or ""),
        "release_group_title": str(group.get("title") or release.get("title") or "").strip(),
        "title": str(release.get("title") or "").strip(),
        "status": str(release.get("status") or "Unknown").strip(),
        "format_detail": format_detail,
        "format_category": format_category,
        "media_count": len(media),
        "track_count": track_count,
        "country_code": str(release.get("country") or "").strip() or None,
        "release_date": release_date,
        "release_year": release_year,
        "date_precision": date_precision,
        "label": "; ".join(item for item in label_names if item) or None,
        "catalog_number": "; ".join(item for item in catalog_numbers if item) or None,
        "barcode": str(release.get("barcode") or "").strip() or None,
        "primary_type": str(group.get("primary-type") or "Unknown"),
        "collected_at": collected_at,
        "source_endpoint": MUSICBRAINZ_URL,
        "is_test_invalid": False,
    }



def request_page(session: requests.Session, offset: int, limit: int = 100, retries: int = 4) -> dict[str, Any]:
    params = {
        "artist": ARTIST_MBID,
        "inc": "media+release-groups+labels+artist-credits",
        "fmt": "json",
        "limit": limit,
        "offset": offset,
    }
    for attempt in range(1, retries + 1):
        try:
            response = session.get(MUSICBRAINZ_URL, params=params, timeout=45)
            if response.status_code == 503 or response.status_code == 429:
                raise requests.HTTPError(f"temporary HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries:
                raise RuntimeError(f"MusicBrainz request failed after {retries} attempts at offset {offset}: {exc}") from exc
            time.sleep(2 ** (attempt - 1))
    raise AssertionError("unreachable")


def collect_from_api(target: int, raw_pages_dir: Path) -> list[dict[str, Any]]:
    if target < 200:
        raise ValueError("target must be at least 200 records")
    raw_pages_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    collected_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset = 0
    total_available: int | None = None

    while len(records) < target and (total_available is None or offset < total_available):
        page = request_page(session, offset=offset)
        total_available = int(page.get("release-count") or 0)
        (raw_pages_dir / f"page_{offset:05d}.json").write_text(
            json.dumps(page, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        releases = page.get("releases") or []
        if not releases:
            break
        for release in releases:
            record = normalize_release(release, collected_at)
            if record and record["release_id"] not in seen:
                records.append(record)
                seen.add(record["release_id"])
                if len(records) >= target:
                    break
        offset += len(releases)
        time.sleep(1.1)  # MusicBrainz asks clients to stay near one request per second.

    if len(records) < target:
        raise RuntimeError(f"Only {len(records)} dated physical releases were collected; target was {target}.")
    return records


def load_snapshot(snapshot_path: Path, target: int | None = None) -> list[dict[str, Any]]:
    records = [json.loads(line) for line in snapshot_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if target is not None:
        if target < 200:
            raise ValueError("target must be at least 200 records")
        if len(records) < target:
            raise RuntimeError(f"Snapshot contains {len(records)} records, fewer than target {target}.")
        records = records[:target]
    return records


def write_jsonl(records: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_csv(records: list[dict[str, Any]], output_path: Path) -> None:
    fieldnames = list(dict.fromkeys(key for record in records for key in record))
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def collect(source: str = "api", target: int = 250, output: Path | None = None) -> list[dict[str, Any]]:
    output = output or DATA_DIR / "raw_data.jsonl"
    if source == "api":
        records = collect_from_api(target, DATA_DIR / "raw_api_pages")
    elif source == "snapshot":
        snapshot = DATA_DIR / "source_snapshot.jsonl"
        records = load_snapshot(snapshot, target=target)
    else:
        raise ValueError("source must be 'api' or 'snapshot'")
    write_jsonl(records, output)
    write_csv(records, output.with_suffix(".csv"))
    summary = {
        "source": source,
        "artist": ARTIST_NAME,
        "artist_mbid": ARTIST_MBID,
        "records_collected": len(records),
        "output": str(output.relative_to(DATA_DIR.parent)),
    }
    (DATA_DIR / "collection_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Collected {len(records)} dated physical Pink Floyd releases -> {output}")
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect physical Pink Floyd releases from MusicBrainz.")
    parser.add_argument("--source", choices=["api", "snapshot"], default="api")
    parser.add_argument("--target", type=int, default=250)
    parser.add_argument("--output", type=Path, default=DATA_DIR / "raw_data.jsonl")
    args = parser.parse_args()
    collect(args.source, args.target, args.output)


if __name__ == "__main__":
    main()
