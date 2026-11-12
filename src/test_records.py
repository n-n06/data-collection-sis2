from __future__ import annotations
from copy import deepcopy


def deliberate_invalid_records(template: dict) -> list[dict]:
    """Return exactly three clearly marked records that violate different schema rules."""
    bad_id = deepcopy(template)
    bad_id.update({
        "release_id": "not-a-musicbrainz-uuid",
        "title": "",
        "is_test_invalid": True,
        "invalid_reason": "invalid UUID and empty required title",
    })
    bad_range = deepcopy(template)
    bad_range.update({
        "release_id": "00000000-0000-4000-8000-000000000001",
        "track_count": 0,
        "release_date": "2099-01-01",
        "release_year": 2099,
        "is_test_invalid": True,
        "invalid_reason": "track count below range and future year/date",
    })
    bad_category = deepcopy(template)
    bad_category.update({
        "release_id": "00000000-0000-4000-8000-000000000002",
        "status": "Mystery",
        "format_category": "Digital",
        "format_detail": "Digital Media",
        "is_test_invalid": True,
        "invalid_reason": "disallowed status and non-physical format",
    })
    return [bad_id, bad_range, bad_category]
