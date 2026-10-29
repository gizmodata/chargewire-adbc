"""Live test: browser sign-in (Chargewire spec §4.3) through the native driver. The whole
flow, against a mock identity provider, is the Go driver's integration suite; here the
options reach the driver and its refusal reaches Python."""

from __future__ import annotations

import pytest

from adbc_driver_chargewire import dbapi

pytestmark = pytest.mark.integration


def test_browser_sign_in_needs_the_servers_capability(chargewire_uri):
    # The shared server has no browser sign-in configured: the driver refuses before
    # creating a flow, saying why, and never opens a browser.
    with pytest.raises(expected_exception=dbapi.Error) as caught:
        dbapi.connect(uri=chargewire_uri, auth_type="oauth_browser", open_browser=False)
    assert "does not offer browser sign-in" in str(caught.value)


def test_browser_sign_in_takes_no_password(chargewire_uri):
    with pytest.raises(expected_exception=dbapi.Error) as caught:
        dbapi.connect(uri=chargewire_uri, auth_type="oauth_browser", username="u", password="p")
    assert "signs in through the browser" in str(caught.value)
