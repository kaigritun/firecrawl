import pytest

from firecrawl.v2.types import Document
from firecrawl.v2.utils.normalize import normalize_document_input, _map_change_tracking_keys


class TestChangeTrackingNormalization:
    def test_change_tracking_keys_normalized_to_snake_case(self):
        """Test that changeTracking inner keys are converted to snake_case."""
        raw = {
            "markdown": "# Hello",
            "changeTracking": {
                "changeStatus": "new",
                "previousScrapeAt": "2024-01-01T00:00:00Z",
                "visibility": "visible",
            },
        }

        normalized = normalize_document_input(raw)
        ct = normalized["change_tracking"]

        assert ct["change_status"] == "new"
        assert ct["previous_scrape_at"] == "2024-01-01T00:00:00Z"
        assert ct["visibility"] == "visible"
        # Original camelCase keys should not exist
        assert "changeStatus" not in ct
        assert "previousScrapeAt" not in ct

    def test_change_tracking_with_none_previous_scrape_at(self):
        """Test change_tracking with null previousScrapeAt (first scrape)."""
        raw = {
            "markdown": "# Hello",
            "changeTracking": {
                "changeStatus": "new",
                "previousScrapeAt": None,
                "visibility": "visible",
            },
        }

        normalized = normalize_document_input(raw)
        ct = normalized["change_tracking"]

        assert ct["change_status"] == "new"
        assert ct["previous_scrape_at"] is None
        assert ct["visibility"] == "visible"

    def test_change_tracking_all_status_values(self):
        """Test all possible changeStatus values are preserved."""
        for status in ["new", "same", "changed", "removed"]:
            raw = {
                "markdown": "# Hello",
                "changeTracking": {
                    "changeStatus": status,
                    "previousScrapeAt": None,
                    "visibility": "visible",
                },
            }

            normalized = normalize_document_input(raw)
            assert normalized["change_tracking"]["change_status"] == status

    def test_change_tracking_with_diff(self):
        """Test change_tracking with diff data is preserved."""
        raw = {
            "markdown": "# Hello",
            "changeTracking": {
                "changeStatus": "changed",
                "previousScrapeAt": "2024-01-01T00:00:00Z",
                "visibility": "visible",
                "diff": {
                    "text": "- old line\n+ new line",
                    "json": {"files": []},
                },
            },
        }

        normalized = normalize_document_input(raw)
        ct = normalized["change_tracking"]

        assert ct["change_status"] == "changed"
        assert ct["diff"]["text"] == "- old line\n+ new line"
        assert ct["diff"]["json"] == {"files": []}

    def test_change_tracking_with_json(self):
        """Test change_tracking with json comparison data is preserved."""
        raw = {
            "markdown": "# Hello",
            "changeTracking": {
                "changeStatus": "changed",
                "previousScrapeAt": "2024-01-01T00:00:00Z",
                "visibility": "visible",
                "json": {
                    "price": {"previous": 100, "current": 120},
                },
            },
        }

        normalized = normalize_document_input(raw)
        ct = normalized["change_tracking"]

        assert ct["change_status"] == "changed"
        assert ct["json"]["price"]["previous"] == 100
        assert ct["json"]["price"]["current"] == 120

    def test_document_with_change_tracking(self):
        """Test that Document can be constructed with normalized change_tracking."""
        raw = {
            "markdown": "# Hello",
            "changeTracking": {
                "changeStatus": "changed",
                "previousScrapeAt": "2024-01-01T00:00:00Z",
                "visibility": "hidden",
            },
        }

        doc = Document(**normalize_document_input(raw))

        assert doc.change_tracking is not None
        assert doc.change_tracking["change_status"] == "changed"
        assert doc.change_tracking["previous_scrape_at"] == "2024-01-01T00:00:00Z"
        assert doc.change_tracking["visibility"] == "hidden"

    def test_no_change_tracking(self):
        """Test document without changeTracking field."""
        raw = {
            "markdown": "# Hello",
        }

        normalized = normalize_document_input(raw)
        assert "change_tracking" not in normalized

    def test_map_change_tracking_keys_helper(self):
        """Test the _map_change_tracking_keys helper directly."""
        ct = {
            "changeStatus": "new",
            "previousScrapeAt": None,
            "visibility": "visible",
            "customField": "preserved",
        }

        mapped = _map_change_tracking_keys(ct)

        assert mapped["change_status"] == "new"
        assert mapped["previous_scrape_at"] is None
        assert mapped["visibility"] == "visible"
        # Unknown keys should be preserved as-is
        assert mapped["customField"] == "preserved"
