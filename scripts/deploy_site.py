"""Deploy the built public site (site/dist) to Netlify.

Usage (run from the repo root, after `npm run build` in site/):

    NETLIFY_AUTH_TOKEN=... python scripts/deploy_site.py

The token comes from the NETLIFY_AUTH_TOKEN environment variable (or --token) and
is never written to disk by this script. The site id is not a secret.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

SITE_ID = "ce5f05ad-a7ab-4886-83a2-c63b072aad2b"  # mapletreesoftball.netlify.app
DIST = Path(__file__).resolve().parents[1] / "site" / "dist"
API = "https://api.netlify.com/api/v1"


def zip_dist() -> bytes:
    buffer = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(DIST.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(DIST).as_posix())
                count += 1
    print(f"zipped {count} files ({buffer.tell():,} bytes)")
    return buffer.getvalue()


def request(method: str, url: str, token: str, body: bytes | None = None,
            content_type: str | None = None) -> dict:
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if content_type:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.environ.get("NETLIFY_AUTH_TOKEN"))
    args = parser.parse_args()
    if not args.token:
        print("error: pass --token or set NETLIFY_AUTH_TOKEN", file=sys.stderr)
        return 2
    if not (DIST / "index.html").exists():
        print(f"error: {DIST} has no index.html — run `npm run build` in site/ first",
              file=sys.stderr)
        return 2

    deploy = request("POST", f"{API}/sites/{SITE_ID}/deploys", args.token,
                     body=zip_dist(), content_type="application/zip")
    deploy_id = deploy["id"]
    print(f"deploy {deploy_id}: {deploy.get('state')}")

    for _ in range(40):
        status = request("GET", f"{API}/sites/{SITE_ID}/deploys/{deploy_id}", args.token)
        state = status.get("state")
        if state == "ready":
            print(f"LIVE: {status.get('ssl_url') or status.get('url')}")
            return 0
        if state == "error":
            print(f"deploy failed: {json.dumps(status.get('error_message'))}", file=sys.stderr)
            return 1
        time.sleep(3)
    print("timed out waiting for deploy to become ready", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
