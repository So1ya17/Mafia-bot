#!/usr/bin/env python3
"""Edit user balances in the game database.

Usage:
  python manage.py coins <tg_id_or_username> <amount>
  python manage.py diamonds <tg_id_or_username> <amount>
  python manage.py add_item <tg_id_or_username> <item_name> [quantity]

Amount can be positive (add) or negative (subtract), e.g. +/-500.
If tg_id_or_username starts with @, it searches by username.
"""
import sys
import os
import sqlite3
from pathlib import Path

# load DB_PATH from .env if present
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            if k == 'DB_PATH':
                DB_PATH = v.strip()
                break
else:
    DB_PATH = 'mafia.db'

DB_PATH = str(Path(DB_PATH).expanduser().resolve())


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=wal')
    return conn


def find_user(conn, identifier: str) -> dict | None:
    identifier = identifier.lstrip('@')
    try:
        tg_id = int(identifier)
        cur = conn.execute('SELECT id, tg_id, username, coins, diamonds FROM users WHERE tg_id = ?', (tg_id,))
    except ValueError:
        cur = conn.execute('SELECT id, tg_id, username, coins, diamonds FROM users WHERE username = ?', (identifier,))
    row = cur.fetchone()
    return dict(row) if row else None


def cmd_coins(identifier: str, amount_str: str):
    conn = get_conn()
    user = find_user(conn, identifier)
    if not user:
        print(f'❌ User not found: {identifier}')
        return 1
    amount = int(amount_str)
    conn.execute('UPDATE users SET coins = MAX(0, coins + ?) WHERE id = ?', (amount, user['id']))
    conn.commit()
    cur = conn.execute('SELECT coins FROM users WHERE id = ?', (user['id'],))
    new_balance = cur.fetchone()['coins']
    sign = '+' if amount >= 0 else ''
    print(f'✅ {user["username"] or user["tg_id"]}: coins {sign}{amount} → {new_balance}')
    conn.close()
    return 0


def cmd_diamonds(identifier: str, amount_str: str):
    conn = get_conn()
    user = find_user(conn, identifier)
    if not user:
        print(f'❌ User not found: {identifier}')
        return 1
    amount = int(amount_str)
    conn.execute('UPDATE users SET diamonds = MAX(0, diamonds + ?) WHERE id = ?', (amount, user['id']))
    conn.commit()
    cur = conn.execute('SELECT diamonds FROM users WHERE id = ?', (user['id'],))
    new_balance = cur.fetchone()['diamonds']
    sign = '+' if amount >= 0 else ''
    print(f'✅ {user["username"] or user["tg_id"]}: diamonds {sign}{amount} → {new_balance}')
    conn.close()
    return 0


def cmd_add_item(identifier: str, item_name: str, qty_str: str = '1'):
    conn = get_conn()
    user = find_user(conn, identifier)
    if not user:
        print(f'❌ User not found: {identifier}')
        return 1
    qty = int(qty_str)
    cur = conn.execute('SELECT quantity FROM user_items WHERE user_id = ? AND item_name = ?', (user['id'], item_name))
    row = cur.fetchone()
    if row:
        conn.execute('UPDATE user_items SET quantity = quantity + ? WHERE user_id = ? AND item_name = ?',
                     (qty, user['id'], item_name))
    else:
        conn.execute('INSERT INTO user_items (user_id, item_name, quantity) VALUES (?, ?, ?)',
                     (user['id'], item_name, qty))
    conn.commit()
    print(f'✅ {user["username"] or user["tg_id"]}: +{qty} × {item_name}')
    conn.close()
    return 0


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 1

    cmd = sys.argv[1]
    identifier = sys.argv[2]
    arg = sys.argv[3]
    extra = sys.argv[4] if len(sys.argv) > 4 else None

    if cmd == 'coins':
        return cmd_coins(identifier, arg)
    elif cmd == 'diamonds':
        return cmd_diamonds(identifier, arg)
    elif cmd == 'add_item':
        return cmd_add_item(identifier, arg, extra)
    else:
        print(f'❌ Unknown command: {cmd}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
