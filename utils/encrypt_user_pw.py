#!/usr/bin/env python3
"""
encrypt_user_pw.py - Bcrypt a password for use in dim_user.passwd.

Usage:
    python3 utils/encrypt_user_pw.py <password>
    python3 utils/encrypt_user_pw.py <password> -u <email>

Without -u: prints the bcrypt hash to stdout.
With -u:    updates dim_user.passwd for the matching email and confirms the row count.
"""

import argparse
import os
import sys
from urllib.parse import urlparse, unquote


def load_env(path=".env"):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass


def parse_pg_url(url):
    p = urlparse(url)
    kw = dict(host=p.hostname, port=p.port or 5432,
              dbname=p.path.lstrip("/"), user=p.username,
              password=unquote(p.password) if p.password else None)
    return {k: v for k, v in kw.items() if v is not None}


def main():
    parser = argparse.ArgumentParser(description="Bcrypt a password for dim_user.passwd")
    parser.add_argument("password", help="Plaintext password to hash")
    parser.add_argument("-u", "--username", metavar="EMAIL",
                        help="Email of user to update in dim_user (optional)")
    args = parser.parse_args()

    try:
        import bcrypt
    except ImportError:
        sys.exit("ERROR: bcrypt not installed. Run: pip install bcrypt")

    hashed = bcrypt.hashpw(args.password.encode(), bcrypt.gensalt()).decode()

    if not args.username:
        print(hashed)
        return

    # Locate .env relative to this script's repo root (one level up from utils/)
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_env(os.path.join(repo_root, ".env"))

    pg_url = os.environ.get("PG_URL")
    if not pg_url:
        sys.exit("ERROR: PG_URL not set. Add it to .env or export it.")

    try:
        import psycopg2
    except ImportError:
        sys.exit("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")

    conn = psycopg2.connect(**parse_pg_url(pg_url))
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dim_user SET passwd = %s, updated_at = NOW() WHERE email = %s",
                (hashed, args.username),
            )
            if cur.rowcount == 0:
                conn.rollback()
                sys.exit(f"ERROR: no user found with email '{args.username}'")
            conn.commit()
        print(f"Updated passwd for '{args.username}' ({cur.rowcount} row).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
