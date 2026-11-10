import os

from pathlib import Path
from __future__ import annotations

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
FIGURES_DIR = ROOT / "figures"
DOCS_DIR = ROOT / "docs"
REPORT_DIR = ROOT / "report"
SQL_DIR = ROOT / "sql"

ARTIST_NAME = "Pink Floyd"
ARTIST_MBID = "83d91898-7763-47d7-b03b-b92132375c47" # looked it up manually
MUSICBRAINZ_URL = "https://musicbrainz.org/ws/2/release"
USER_AGENT = os.getenv(
    "MUSICBRAINZ_USER_AGENT",
    "SIS2NursultanZhantuar - educatoinal use",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pink-floyd-physical-releases")
CURRENT_YEAR = 2026
PHYSICAL_FORMATS = {"CD", "Vinyl", "Cassette", "MiniDisc", "DVD/Blu-ray", "8-Track", "Other physical"}