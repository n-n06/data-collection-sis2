from src.api_client import classify_format


def test_physical_format_classification():
    assert classify_format('12" Vinyl') == "Vinyl"
    assert classify_format("2xCD") == "CD"
    assert classify_format("Cassette") == "Cassette"
    assert classify_format("Digital Media") is None
