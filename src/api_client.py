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
