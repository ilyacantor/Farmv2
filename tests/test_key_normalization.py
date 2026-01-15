"""Tests for key normalization functions.

These tests verify that domain extraction and normalization work correctly,
especially avoiding the creation of phantom domains from subdomain stripping.
"""

import pytest
from src.services.key_normalization import (
    extract_domain,
    extract_registered_domain,
    to_domain_key,
    normalize_name,
    is_external_domain,
    is_valid_fqdn,
)


class TestExtractDomain:
    """Test extract_domain() for proper eTLD+1 extraction."""

    def test_simple_domain(self):
        """Simple domains should pass through unchanged."""
        assert extract_domain("slack.com") == "slack.com"
        assert extract_domain("calendly.com") == "calendly.com"
        assert extract_domain("pagerduty.com") == "pagerduty.com"

    def test_subdomain_extraction(self):
        """Subdomains should be stripped to get registered domain."""
        assert extract_domain("app.slack.com") == "slack.com"
        assert extract_domain("www.google.com") == "google.com"
        assert extract_domain("mail.google.com") == "google.com"

    def test_multi_level_subdomain(self):
        """Multi-level subdomains should extract to registered domain."""
        assert extract_domain("static.cdn.cloudflare.com") == "cloudflare.com"
        assert extract_domain("api.v2.example.com") == "example.com"
        assert extract_domain("deep.nested.sub.domain.com") == "domain.com"

    def test_no_phantom_domains(self):
        """Should NOT create phantom domains from subdomain stripping.
        
        This was the bug: static.cdn.cloudflare.com was being stripped to cdn.com
        instead of cloudflare.com.
        """
        # These should NOT return cdn.com, edge.com, etc.
        assert extract_domain("static.cdn.cloudflare.com") != "cdn.com"
        assert extract_domain("app.edge.fastly.net") != "edge.fastly.net"
        
        # They should return the actual registered domain
        assert extract_domain("static.cdn.cloudflare.com") == "cloudflare.com"
        assert extract_domain("app.edge.fastly.net") == "fastly.net"

    def test_compound_tld(self):
        """Compound TLDs like .co.uk should be handled correctly."""
        assert extract_domain("example.co.uk") == "example.co.uk"
        assert extract_domain("app.example.co.uk") == "example.co.uk"
        assert extract_domain("cdn.static.example.co.uk") == "example.co.uk"

    def test_url_with_protocol(self):
        """URLs with protocol should have it stripped."""
        assert extract_domain("https://slack.com") == "slack.com"
        assert extract_domain("http://app.slack.com/path") == "slack.com"
        assert extract_domain("https://www.google.com/search?q=test") == "google.com"

    def test_url_with_port(self):
        """URLs with port should have port stripped."""
        assert extract_domain("localhost:8080") is None  # No TLD
        assert extract_domain("example.com:443") == "example.com"

    def test_empty_and_none(self):
        """Empty and None inputs should return None."""
        assert extract_domain("") is None
        assert extract_domain(None) is None

    def test_no_tld(self):
        """Strings without dots should return None."""
        assert extract_domain("localhost") is None
        assert extract_domain("authservice") is None


class TestExtractRegisteredDomain:
    """Test extract_registered_domain() for eTLD+1 extraction."""

    def test_simple_domain(self):
        assert extract_registered_domain("slack.com") == "slack.com"

    def test_subdomain(self):
        assert extract_registered_domain("app.slack.com") == "slack.com"
        assert extract_registered_domain("www.redis.com") == "redis.com"

    def test_compound_tld(self):
        assert extract_registered_domain("example.co.uk") == "example.co.uk"
        assert extract_registered_domain("cdn.static.example.co.uk") == "example.co.uk"


class TestToDomainKey:
    """Test to_domain_key() for entity key conversion."""

    def test_domain_passthrough(self):
        """Domain-like keys should be converted to domain keys."""
        assert to_domain_key("slack.com") == "slack.com"
        assert to_domain_key("app.slack.com") == "slack.com"

    def test_name_normalization(self):
        """Non-domain keys should be normalized."""
        assert to_domain_key("Slack") == "slack"
        assert to_domain_key("Microsoft 365") == "microsoft365"

    def test_url_handling(self):
        """URLs should be converted to domain keys."""
        assert to_domain_key("https://slack.com/path") == "slack.com"


class TestIsValidFqdn:
    """Test is_valid_fqdn() for FQDN validation."""

    def test_valid_fqdns(self):
        assert is_valid_fqdn("google.com") == True
        assert is_valid_fqdn("mail.google.com") == True
        assert is_valid_fqdn("example.co.uk") == True

    def test_invalid_fqdns(self):
        assert is_valid_fqdn("paymentgateway") == False
        assert is_valid_fqdn("auth-service") == False
        assert is_valid_fqdn("images694") == False
        assert is_valid_fqdn("") == False


class TestNormalizeName:
    """Test normalize_name() for name normalization."""

    def test_lowercase(self):
        assert normalize_name("SLACK") == "slack"
        assert normalize_name("Microsoft") == "microsoft"

    def test_strip_special_chars(self):
        assert normalize_name("Microsoft 365") == "microsoft365"
        assert normalize_name("Zoom-Video") == "zoomvideo"

    def test_empty(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""


class TestInfrastructureDomainPreservation:
    """Test infrastructure domain preservation per AOD Stage 4 fix.
    
    Infrastructure domains (googleapis.com, gstatic.com, office.com, etc.)
    should be preserved as standalone keys and NOT collapsed to their parent domain.
    """

    def test_google_infrastructure_preserved(self):
        """Google infrastructure domains should NOT collapse to google.com.
        
        Note: google.com itself is NOT an infrastructure domain - it's a regular
        SaaS domain. So mail.google.com correctly collapses to google.com.
        """
        assert extract_domain("api.googleapis.com") == "googleapis.com"
        assert extract_domain("www.gstatic.com") == "gstatic.com"
        assert extract_domain("storage.googleusercontent.com") == "googleusercontent.com"
        assert extract_domain("video.googlevideo.com") == "googlevideo.com"
        
        # google.com is NOT infrastructure - regular subdomains collapse normally
        assert extract_domain("mail.google.com") == "google.com"
        assert extract_domain("www.google.com") == "google.com"
        assert extract_domain("drive.google.com") == "google.com"

    def test_microsoft_infrastructure_preserved(self):
        """Microsoft infrastructure domains should NOT collapse to microsoft.com."""
        assert extract_domain("login.microsoftonline.com") == "microsoftonline.com"
        assert extract_domain("outlook.office.com") == "office.com"
        assert extract_domain("teams.office365.com") == "office365.com"
        assert extract_domain("portal.sharepoint.com") == "sharepoint.com"
        assert extract_domain("mail.outlook.com") == "outlook.com"
        
        # Regular microsoft.com subdomains still collapse
        assert extract_domain("www.microsoft.com") == "microsoft.com"

    def test_aws_infrastructure_preserved(self):
        """AWS infrastructure domains should be preserved."""
        assert extract_domain("d1234.cloudfront.net") == "cloudfront.net"
        assert extract_domain("static.awsstatic.com") == "awsstatic.com"
        assert extract_domain("s3.amazonaws.com") == "amazonaws.com"

    def test_cdn_infrastructure_preserved(self):
        """CDN infrastructure domains should be preserved."""
        assert extract_domain("edge.akamaihd.net") == "akamaihd.net"
        assert extract_domain("cdn.cloudflare.com") == "cloudflare.com"

    def test_regular_domains_still_collapse(self):
        """Regular (non-infrastructure) domains should still collapse normally."""
        assert extract_domain("app.slack.com") == "slack.com"
        assert extract_domain("api.calendly.com") == "calendly.com"
        assert extract_domain("www.dropbox.com") == "dropbox.com"
