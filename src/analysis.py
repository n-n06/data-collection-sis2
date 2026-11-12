from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from .config import DATA_DIR, DOCS_DIR, FIGURES_DIR

sns.set_theme(style="whitegrid", context="notebook")
PALETTE = ["#6A3D9A", "#1F78B4", "#33A02C", "#FF7F00", "#E31A1C", "#A6CEE3", "#B2DF8A"]


def _save(fig: plt.Figure, name: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / name, dpi=180, bbox_inches="tight")
    plt.close(fig)


def architecture_diagram() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(14, 3.2))
    ax.axis("off")
    steps = ["MusicBrainz\nAPI", "Raw JSONL\npreservation", "Kafka\nProducer", "Kafka broker\n+ topic", "Kafka\nConsumer", "Pandera\nvalidation", "pandas + NumPy\nclean/transform", "SQLite3\nUPSERT", "EDA +\nfigures"]
    x_values = list(range(len(steps)))
    for x, text in zip(x_values, steps):
        ax.text(x, 0.5, text, ha="center", va="center", fontsize=9, color="white",
                bbox=dict(boxstyle="round,pad=0.55", facecolor="#3B276B", edgecolor="#F3C623", linewidth=1.5))
        if x < len(steps) - 1:
            ax.annotate("", xy=(x + 0.64, 0.5), xytext=(x + 0.36, 0.5), arrowprops=dict(arrowstyle="->", color="#444", lw=1.5))
    ax.set_xlim(-0.6, len(steps) - 0.4)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(DOCS_DIR / "pipeline_architecture.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def analyze(input_path: Path) -> dict:
    frame = pd.read_csv(input_path)
    frame["release_year"] = pd.to_numeric(frame["release_year"])
    frame["track_count"] = pd.to_numeric(frame["track_count"])
    frame["media_count"] = pd.to_numeric(frame["media_count"])

    format_counts = frame["format_category"].value_counts()
    fig, ax = plt.subplots(figsize=(9, 5.4))
    sns.barplot(x=format_counts.values, y=format_counts.index, hue=format_counts.index, palette=PALETTE[:len(format_counts)], legend=False, ax=ax)
    ax.set(title="Pink Floyd physical releases by format", xlabel="Number of release editions", ylabel="Physical format")
    for container in ax.containers:
        ax.bar_label(container, padding=3, fontsize=9)
    _save(fig, "01_format_distribution.png")

    decade_counts = frame.groupby("release_decade").size().reset_index(name="release_count")
    fig, ax = plt.subplots(figsize=(9, 5.4))
    sns.lineplot(data=decade_counts, x="release_decade", y="release_count", marker="o", linewidth=2.5, color="#6A3D9A", ax=ax)
    ax.set(title="Release editions represented by decade", xlabel="Release decade", ylabel="Number of editions")
    ax.set_xticks(decade_counts["release_decade"])
    _save(fig, "02_releases_by_decade.png")

    country_counts = frame["country_code"].replace("Unknown", pd.NA).dropna().value_counts().head(12)
    fig, ax = plt.subplots(figsize=(9, 5.4))
    sns.barplot(x=country_counts.index, y=country_counts.values, hue=country_counts.index, palette="mako", legend=False, ax=ax)
    ax.set(title="Top release countries/areas", xlabel="MusicBrainz area code", ylabel="Number of editions")
    ax.tick_params(axis="x", rotation=45)
    _save(fig, "03_top_countries.png")

    pivot = pd.pivot_table(frame, index="release_decade", columns="format_category", values="release_id", aggfunc="count", fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 5.6))
    sns.heatmap(pivot, annot=True, fmt="g", cmap="Purples", linewidths=0.5, ax=ax)
    ax.set(title="Format mix by release decade", xlabel="Physical format", ylabel="Release decade")
    _save(fig, "04_decade_format_heatmap.png")

    architecture_diagram()
    stats = {
        "dimensions": [int(frame.shape[0]), int(frame.shape[1])],
        "release_year_min": int(frame["release_year"].min()),
        "release_year_max": int(frame["release_year"].max()),
        "track_count_mean": round(float(frame["track_count"].mean()), 2),
        "track_count_median": round(float(frame["track_count"].median()), 2),
        "track_count_min": int(frame["track_count"].min()),
        "track_count_max": int(frame["track_count"].max()),
        "unique_countries_or_areas": int(frame["country_code"].replace("Unknown", pd.NA).nunique()),
        "unique_release_groups": int(frame["release_group_id"].nunique()),
        "format_counts": {str(key): int(value) for key, value in format_counts.items()},
        "top_countries": {str(key): int(value) for key, value in country_counts.items()},
        "peak_decade": int(decade_counts.loc[decade_counts["release_count"].idxmax(), "release_decade"]),
        "peak_decade_count": int(decade_counts["release_count"].max()),
        "multi_disc_releases": int((frame["media_count"] > 1).sum()),
        "figures": sorted(path.name for path in FIGURES_DIR.glob("*.png")),
    }
    (DATA_DIR / "eda_statistics.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")
    findings = [
        "# EDA findings",
        f"- The processed dataset has {stats['dimensions'][0]} rows and {stats['dimensions'][1]} columns.",
        f"- Editions span {stats['release_year_min']} to {stats['release_year_max']}.",
        f"- The most frequent physical format is {format_counts.index[0]} ({int(format_counts.iloc[0])} editions).",
        f"- The peak decade is the {stats['peak_decade']}s ({stats['peak_decade_count']} editions).",
        f"- The data contains {stats['unique_countries_or_areas']} distinct country/area codes and {stats['multi_disc_releases']} multi-disc editions.",
        "- The heatmap demonstrates the historical shift from vinyl/cassette toward CD and later multi-format reissues.",
    ]
    (DATA_DIR / "eda_findings.md").write_text("\n".join(findings) + "\n", encoding="utf-8")
    print(f"EDA: generated {len(stats['figures'])} figures; peak decade={stats['peak_decade']}s")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EDA and generate Matplotlib/Seaborn figures.")
    parser.add_argument("--input", type=Path, default=DATA_DIR / "processed_data.csv")
    args = parser.parse_args()
    analyze(args.input)


if __name__ == "__main__":
    main()
