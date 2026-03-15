#!/usr/bin/env python3
"""
manage_users.py — CLI for managing Hansard Search user accounts.

Usage:
    python manage_users.py add email@example.com --days 7 --label "Guardian journalist"
    python manage_users.py add email@example.com --days 7 --password mypassword
    python manage_users.py list
    python manage_users.py delete email@example.com
    python manage_users.py extend email@example.com --days 7
    python manage_users.py reset email@example.com
"""

import sys
import sqlite3
import secrets
import argparse
from datetime import datetime, timedelta
from pathlib import Path

from werkzeug.security import generate_password_hash

DB_PATH = Path(__file__).parent / "users.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_add(args):
    if not DB_PATH.exists():
        print("Error: users.db not found. Start the app once first to initialise it.")
        sys.exit(1)

    password = args.password or secrets.token_urlsafe(12)
    expires_at = None
    if args.days:
        expires_at = (datetime.utcnow() + timedelta(days=args.days)).strftime("%Y-%m-%dT%H:%M:%S")

    with get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO users (email, password_hash, created_at, expires_at, label) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    args.email.lower(),
                    generate_password_hash(password),
                    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                    expires_at,
                    args.label,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            print(f"Error: {args.email} already exists. Use 'extend' or 'reset' instead.")
            sys.exit(1)

    print(f"  Created : {args.email}")
    print(f"  Password: {password}")
    print(f"  Expires : {expires_at[:10] if expires_at else 'never'}")
    if args.label:
        print(f"  Label   : {args.label}")


def cmd_list(args):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT email, created_at, expires_at, label FROM users ORDER BY created_at"
        ).fetchall()

    if not rows:
        print("No users.")
        return

    now = datetime.utcnow()
    for row in rows:
        expires = row["expires_at"]
        if expires:
            exp_dt = datetime.fromisoformat(expires)
            status = "EXPIRED" if now > exp_dt else f"expires {expires[:10]}"
        else:
            status = "no expiry"
        label = f"  [{row['label']}]" if row["label"] else ""
        print(f"  {row['email']}{label}  created={row['created_at'][:10]}  {status}")


def cmd_delete(args):
    with get_conn() as conn:
        n = conn.execute(
            "DELETE FROM users WHERE email = ?", (args.email.lower(),)
        ).rowcount
        conn.commit()
    if n:
        print(f"  Deleted {args.email}")
    else:
        print(f"  User not found: {args.email}")


def cmd_extend(args):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT expires_at FROM users WHERE email = ?", (args.email.lower(),)
        ).fetchone()
        if not row:
            print(f"  User not found: {args.email}")
            sys.exit(1)
        base = datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else datetime.utcnow()
        # Extend from expiry date (or now if expired) — whichever is later
        base = max(base, datetime.utcnow())
        new_expiry = (base + timedelta(days=args.days)).strftime("%Y-%m-%dT%H:%M:%S")
        conn.execute(
            "UPDATE users SET expires_at = ? WHERE email = ?",
            (new_expiry, args.email.lower()),
        )
        conn.commit()
    print(f"  Extended {args.email} → expires {new_expiry[:10]}")


def cmd_reset(args):
    password = args.password or secrets.token_urlsafe(12)
    with get_conn() as conn:
        n = conn.execute(
            "UPDATE users SET password_hash = ? WHERE email = ?",
            (generate_password_hash(password), args.email.lower()),
        ).rowcount
        conn.commit()
    if n:
        print(f"  Reset password for {args.email}")
        print(f"  New password: {password}")
    else:
        print(f"  User not found: {args.email}")


def cmd_requests(args):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, name, email, organisation, reason, requested_at, status "
            "FROM access_requests ORDER BY requested_at DESC"
        ).fetchall()
    if not rows:
        print("No access requests.")
        return
    for row in rows:
        org = f"  [{row['organisation']}]" if row['organisation'] else ""
        print(f"  [{row['id']}] {row['name']}{org} <{row['email']}>  {row['requested_at'][:10]}  ({row['status']})")
        if row['reason']:
            print(f"       {row['reason']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Hansard Search user accounts")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="Create a new user")
    p_add.add_argument("email")
    p_add.add_argument("--password", help="Password (auto-generated if omitted)")
    p_add.add_argument("--days", type=int, help="Days until access expires")
    p_add.add_argument("--label", help="Label e.g. 'Guardian journalist'")

    sub.add_parser("list", help="List all users and their expiry status")

    sub.add_parser("requests", help="List access requests")

    p_del = sub.add_parser("delete", help="Delete a user")
    p_del.add_argument("email")

    p_ext = sub.add_parser("extend", help="Extend a user's access by N days from their current expiry")
    p_ext.add_argument("email")
    p_ext.add_argument("--days", type=int, required=True)

    p_rst = sub.add_parser("reset", help="Reset a user's password")
    p_rst.add_argument("email")
    p_rst.add_argument("--password", help="New password (auto-generated if omitted)")

    args = parser.parse_args()
    {
        "add":    cmd_add,
        "list":   cmd_list,
        "delete": cmd_delete,
        "extend": cmd_extend,
        "reset":    cmd_reset,
        "requests": cmd_requests,
    }[args.cmd](args)
