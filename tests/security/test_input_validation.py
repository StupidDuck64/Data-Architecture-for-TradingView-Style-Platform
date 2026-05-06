"""
Security validation tests — verifies input sanitization and abuse prevention.
"""

import pytest
from fastapi import HTTPException

from backend.services.candle_service import validate_symbol, validate_interval


class TestSymbolInjection:
    """Test that malicious symbol inputs are rejected."""

    @pytest.mark.security
    def test_sql_injection_attempt(self):
        with pytest.raises(HTTPException):
            validate_symbol("'; DROP TABLE--")

    @pytest.mark.security
    def test_xss_attempt(self):
        with pytest.raises(HTTPException):
            validate_symbol("<script>alert(1)</script>")

    @pytest.mark.security
    def test_path_traversal(self):
        with pytest.raises(HTTPException):
            validate_symbol("../../../etc/passwd")

    @pytest.mark.security
    def test_null_bytes(self):
        with pytest.raises(HTTPException):
            validate_symbol("BTC\x00USDT")

    @pytest.mark.security
    def test_unicode_abuse(self):
        with pytest.raises(HTTPException):
            validate_symbol("BTC\u200bUSDT")  # zero-width space

    @pytest.mark.security
    def test_extremely_long_input(self):
        with pytest.raises(HTTPException):
            validate_symbol("A" * 1000)


class TestIntervalInjection:
    """Test that malicious interval inputs are rejected."""

    @pytest.mark.security
    def test_sql_in_interval(self):
        with pytest.raises(HTTPException):
            validate_interval("1m; DROP TABLE")

    @pytest.mark.security
    def test_arbitrary_string(self):
        with pytest.raises(HTTPException):
            validate_interval("anything")

    @pytest.mark.security
    def test_empty_interval(self):
        with pytest.raises(HTTPException):
            validate_interval("")
