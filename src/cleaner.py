import argparse
import json
import re

from pathlib import Path
from __future__ import annotations

import numpy as np
import pandas as pd

from .config import CURRENT_YEAR, DATA_DIR


def clean_and_transform(
    input_path: Path, 
    output_path: Path | None = None
) -> tuple[pd.DataFrame, dict]:
    
    output_path = output_path or DATA_DIR / "processed_data.csv"
    frame = pd.read_csv(input_path, dtype={"release_id": "string", "barcode": "string", "catalog_number": "string"})


    # for copmarison
    input_rows = len(frame)
    missing_before = frame.isna().sum().to_dict()


    # nullable strings are imputed using Unknown
    optional_text = ["country_code", "label", "catalog_number", "barcode", "primary_type"]
    for column in optional_text:
        frame[column] = frame[column].fillna("Unknown").astype(str).str.strip()


    # remove extra spaces
    for column in ["title", "release_group_title", "label", "format_detail"]:
        frame[column] = frame[column].fillna("").astype(str).map(lambda value: re.sub(r"\s+", " ", value).strip())

    # strip cols
    frame["artist_name"] = frame["artist_name"].str.strip()
    frame["status"] = frame["status"].str.strip().str.title()
    frame["format_category"] = frame["format_category"].str.strip()


    # Type conversion and dedup by UID
    frame["release_date_parsed"] = pd.to_datetime(frame["release_date"], errors="coerce")
    frame["release_year"] = pd.to_numeric(frame["release_year"], errors="raise").astype("int64")
    frame["track_count"] = pd.to_numeric(frame["track_count"], errors="raise").astype("int64")
    frame["media_count"] = pd.to_numeric(frame["media_count"], errors="raise").astype("int64")
    duplicate_rows = int(frame.duplicated(subset=["release_id"]).sum())
    frame = frame.drop_duplicates(subset=["release_id"], keep="first").copy()


    # nump transformations used by analysis
    frame["release_age_years"] = np.maximum(0, CURRENT_YEAR - frame["release_year"].to_numpy())
    frame["release_decade"] = (frame["release_year"] // 10 * 10).astype("int64")
    frame["era"] = np.select(
        [frame["release_year"] < 1980, frame["release_year"] < 2000],
        ["Classic 1960s-1970s", "Catalog 1980s-1990s"],
        default="Modern 2000s+",
    )
    frame["is_multi_disc"] = (frame["media_count"] > 1).astype("int64")
    track_std = float(frame["track_count"].std(ddof=0))
    frame["track_count_zscore"] = (
        (frame["track_count"] - frame["track_count"].mean()) / track_std if track_std else 0.0
    )


    # multi-condition filter, group aggregation and pivot-table reshaping
    filtered = frame.loc[
        (frame["release_year"] >= 2000)
        & (frame["media_count"] > 1)
        & (frame["track_count"] >= 10)
    ].copy() # special releases with more than 1 disc after 2000 (disbandment)

    # save to csv
    filtered.to_csv(DATA_DIR / "filtered_recent_multidisc.csv", index=False)


    grouped = (
        frame.groupby("format_category", as_index=False)
        .agg(release_count=("release_id", "count"), average_tracks=("track_count", "mean"), median_age=("release_age_years", "median"))
        .sort_values("release_count", ascending=False)
    )
    # svae to csv
    grouped.to_csv(DATA_DIR / "groupby_format_summary.csv", index=False)


    pivot = pd.pivot_table(
        frame,
        index="release_decade",
        columns="format_category",
        values="release_id",
        aggfunc="count",
        fill_value=0,
    )
    # save to csv
    pivot.to_csv(DATA_DIR / "pivot_decade_format.csv")


    frame["release_date_parsed"] = frame["release_date_parsed"].dt.strftime("%Y-%m-%d")
    frame.to_csv(output_path, index=False)


    summary = {
        "input_valid_records": int(input_rows),
        "duplicates_removed": duplicate_rows,
        "records_after_cleaning": int(len(frame)),
        "missing_optional_values_before_fill": {key: int(value) for key, value in missing_before.items() if value},
        "multi_condition_filter_records": int(len(filtered)),
        "groupby_rows": int(len(grouped)),
        "pivot_shape": [int(pivot.shape[0]), int(pivot.shape[1])],
        "numpy_operations": ["release age", "decade", "era selection", "multi-disc flag", "track-count z-score"],
    }
    (DATA_DIR / "cleaning_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Cleaning: {input_rows} valid -> {len(frame)} processed; removed {duplicate_rows} duplicates")
    
    return frame, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and transform validated records with pandas and NumPy.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "valid_records.csv")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "processed_data.csv")
    args = parser.parse_args()
    clean_and_transform(args.input, args.output)


if __name__ == "__main__":
    main()
