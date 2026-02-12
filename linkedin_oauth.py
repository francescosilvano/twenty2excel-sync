"""
LinkedIn OAuth 2.0 Authorization Code Flow.

Handles the 3-legged OAuth dance:
  1. Open the consent URL in the user's browser.
  2. Spin up a tiny local HTTP server to capture the redirect callback.
  3. Exchange the authorization code for an access token.
  4. Persist / load tokens from disk.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs

import requests

from config import (
    LINKEDIN_CLIENT_ID,
    LINKEDIN_CLIENT_SECRET,
    LINKEDIN_REDIRECT_URI,
    LINKEDIN_SCOPE,
    LINKEDIN_TOKEN_PATH,
)

logger = logging.getLogger(__name__)

# ── Token persistence ────────────────────────────────────────────────


def _save_token(token: dict) -> None:
    Path(LINKEDIN_TOKEN_PATH).write_text(json.dumps(token, indent=2))
    logger.info("LinkedIn token saved to %s", LINKEDIN_TOKEN_PATH)


def load_token() -> dict | None:
    p = Path(LINKEDIN_TOKEN_PATH)
    if not p.exists():
        return None
    token = json.loads(p.read_text())
    # Check expiry
    if token.get("expires_at", 0) < time.time():
        logger.warning("LinkedIn token has expired – re-authenticate with `linkedin-auth`")
        return None
    return token


# ── Tiny callback server ─────────────────────────────────────────────

_auth_result: dict[str, str] = {}


class _CallbackHandler(BaseHTTPRequestHandler):
    """Handles the OAuth redirect from LinkedIn."""

    def do_GET(self):  # noqa: N802
        global _auth_result
        qs = parse_qs(urlparse(self.path).query)
        _auth_result = {k: v[0] for k, v in qs.items()}

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        if "code" in _auth_result:
            self.wfile.write(
                b"<h2>LinkedIn authorisation successful!</h2>"
                b"<p>You can close this tab and return to the terminal.</p>"
            )
        else:
            msg = _auth_result.get("error_description", "Unknown error")
            self.wfile.write(
                f"<h2>Authorisation failed</h2><p>{msg}</p>".encode()
            )

    def log_message(self, fmt, *args):  # silence default logging
        logger.debug(fmt, *args)


# ── Public API ────────────────────────────────────────────────────────


def authenticate() -> dict:
    """
    Run the full OAuth 2.0 Authorization Code Flow.

    1. Opens the consent URL in the default browser.
    2. Waits for LinkedIn to redirect back to localhost.
    3. Exchanges the code for an access token.
    4. Saves and returns the token dict.
    """
    if not LINKEDIN_CLIENT_ID or not LINKEDIN_CLIENT_SECRET:
        raise RuntimeError(
            "LINKEDIN_CLIENT_ID and LINKEDIN_CLIENT_SECRET must be set in .env"
        )

    state = secrets.token_urlsafe(24)

    auth_url = "https://www.linkedin.com/oauth/v2/authorization?" + urlencode(
        {
            "response_type": "code",
            "client_id": LINKEDIN_CLIENT_ID,
            "redirect_uri": LINKEDIN_REDIRECT_URI,
            "state": state,
            "scope": LINKEDIN_SCOPE,
        }
    )

    # Parse redirect URI to find host / port for the callback server
    parsed = urlparse(LINKEDIN_REDIRECT_URI)
    host = "127.0.0.1"
    port = parsed.port or 8787

    logger.info("Opening browser for LinkedIn authorisation…")
    webbrowser.open(auth_url)

    # Start the callback server
    global _auth_result
    _auth_result = {}
    server = HTTPServer((host, port), _CallbackHandler)
    server.timeout = 120  # 2 minutes max wait
    logger.info("Waiting for callback on %s:%d …", host, port)

    while "code" not in _auth_result and "error" not in _auth_result:
        server.handle_request()

    server.server_close()

    if "error" in _auth_result:
        raise RuntimeError(
            f"LinkedIn auth failed: {_auth_result.get('error_description', _auth_result['error'])}"
        )

    # Verify state
    if _auth_result.get("state") != state:
        raise RuntimeError("OAuth state mismatch – possible CSRF attack")

    code = _auth_result["code"]
    logger.info("Received authorisation code – exchanging for token…")

    # Exchange code for access token
    resp = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "client_id": LINKEDIN_CLIENT_ID,
            "client_secret": LINKEDIN_CLIENT_SECRET,
            "redirect_uri": LINKEDIN_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )

    if not resp.ok:
        raise RuntimeError(f"Token exchange failed: {resp.status_code} {resp.text}")

    token = resp.json()
    # Store absolute expiry for easy checking
    token["expires_at"] = time.time() + token.get("expires_in", 5184000)
    _save_token(token)
    logger.info("LinkedIn authentication complete ✓")
    return token


def get_access_token() -> str:
    """Return a valid access token or raise."""
    # 1. Check .env first
    from config import LINKEDIN_ACCESS_TOKEN
    if LINKEDIN_ACCESS_TOKEN:
        return LINKEDIN_ACCESS_TOKEN
    # 2. Fall back to saved token file
    token = load_token()
    if token is None:
        raise RuntimeError(
            "No valid LinkedIn token found. Set LINKEDIN_ACCESS_TOKEN in .env "
            "or run `python main.py linkedin-auth`."
        )
    return token["access_token"]


def save_manual_token(access_token: str, expires_in: int = 5184000) -> dict:
    """
    Save a token that was manually generated via LinkedIn's
    OAuth Token Generator Tool.
    """
    token = {
        "access_token": access_token.strip(),
        "expires_in": expires_in,
        "expires_at": time.time() + expires_in,
        "scope": LINKEDIN_SCOPE,
    }
    _save_token(token)
    logger.info("Manual LinkedIn token saved ✓  (expires in %d days)", expires_in // 86400)
    return token
