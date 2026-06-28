import aiosqlite
import random
from datetime import datetime
from config import DB_PATH

_db: aiosqlite.Connection | None = None

async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        _db = await aiosqlite.connect(DB_PATH)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL")
    return _db

async def close_db():
    global _db
    if _db:
        await _db.close()
        _db = None

async def init_db():
    db = await get_db()
    await db.executescript('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER UNIQUE,
        username TEXT,
        coins INTEGER DEFAULT 0,
        diamonds INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_daily DATETIME DEFAULT NULL
    );
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        thread_id INTEGER,
        state TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        user_id INTEGER,
        role TEXT,
        alive INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS shop_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price INTEGER,
        data TEXT
    );
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount INTEGER,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS user_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        item_name TEXT,
        quantity INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS night_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        actor_user_id INTEGER,
        role TEXT,
        target_user_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        voter_user_id INTEGER,
        target_user_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS lynch_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        candidate_user_id INTEGER,
        voter_user_id INTEGER,
        choice INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS lynch_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        candidate_user_id INTEGER,
        chat_id INTEGER,
        message_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    # ensure legacy DBs get chat_id column if missing
    cur = await db.execute("PRAGMA table_info(games)")
    cols = await cur.fetchall()
    col_names = [c[1] for c in cols]
    if 'chat_id' not in col_names:
        await db.execute('ALTER TABLE games ADD COLUMN chat_id INTEGER')
    if 'lobby_message_id' not in col_names:
        await db.execute('ALTER TABLE games ADD COLUMN lobby_message_id INTEGER')
    # games table: add phase column
    if 'phase' not in col_names:
        await db.execute('ALTER TABLE games ADD COLUMN phase TEXT DEFAULT "lobby"')
    # games table: add phase_deadline column to persist timers (epoch seconds)
    if 'phase_deadline' not in col_names:
        await db.execute('ALTER TABLE games ADD COLUMN phase_deadline INTEGER')
    # create unique indexes to avoid duplicate votes in race conditions
    await db.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_lynch_vote_unique ON lynch_votes (game_id, candidate_user_id, voter_user_id)')
    await db.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_vote_unique ON votes (game_id, voter_user_id)')
    await db.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_user_item ON user_items (user_id, item_name)')
    await db.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_shop_item_name ON shop_items (name)')
    # add framed, blackmailed, disguised columns if missing
    cur = await db.execute("PRAGMA table_info(players)")
    prow = await cur.fetchall()
    pcol_names = [c[1] for c in prow]
    for col in ('framed', 'blackmailed', 'disguised', 'doctor_last_heal', 'protection_used', 'masked'):
        if col not in pcol_names:
            await db.execute(f'ALTER TABLE players ADD COLUMN {col} INTEGER DEFAULT 0')
    await db.commit()
    # add created_at column if missing (migration for existing DBs)
    try:
        await db.execute('ALTER TABLE users ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP')
    except Exception:
        pass
    # add last_daily column if missing (migration for existing DBs)
    try:
        await db.execute('ALTER TABLE users ADD COLUMN last_daily DATETIME DEFAULT NULL')
    except Exception:
        pass
    # add diamonds column if missing (migration for existing DBs)
    try:
        await db.execute('ALTER TABLE users ADD COLUMN diamonds INTEGER DEFAULT 0')
    except Exception:
        pass
    # add daily_count column if missing (migration for existing DBs)
    try:
        await db.execute('ALTER TABLE users ADD COLUMN daily_count INTEGER DEFAULT 0')
    except Exception:
        pass
    # add crazy_mode column for Безумный режим
    try:
        await db.execute('ALTER TABLE games ADD COLUMN crazy_mode INTEGER DEFAULT 0')
    except Exception:
        pass

    # last_words table for persistent last word collection across restarts
    await db.execute('''
        CREATE TABLE IF NOT EXISTS last_words (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            user_id INTEGER,
            tg_id INTEGER,
            chat_id INTEGER,
            thread_id INTEGER,
            name TEXT,
            text TEXT,
            collected INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # game_item_usage table for once-per-game item tracking
    await db.execute('''
        CREATE TABLE IF NOT EXISTS game_item_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            item_name TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    await db.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_item_usage ON game_item_usage (game_id, user_id, item_name)')
    # pending_poison_kills table for slow poison delayed kills
    await db.execute('''
        CREATE TABLE IF NOT EXISTS pending_poison_kills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # role_stats table for per-role win/loss tracking
    await db.execute('''
        CREATE TABLE IF NOT EXISTS role_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            role_name TEXT NOT NULL,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            games INTEGER DEFAULT 0,
            UNIQUE(user_id, role_name)
        )
    ''')
    # add role_revealed column for Gray Cardinal tracking
    try:
        await db.execute('ALTER TABLE players ADD COLUMN role_revealed INTEGER DEFAULT 0')
    except Exception:
        pass
    # add started_at column for game duration tracking
    try:
        await db.execute('ALTER TABLE games ADD COLUMN started_at DATETIME')
    except Exception:
        pass
    # add total_play_seconds column for player play time
    try:
        await db.execute('ALTER TABLE users ADD COLUMN total_play_seconds INTEGER DEFAULT 0')
    except Exception:
        pass
    # add tg_username column for @username lookups (give command)
    try:
        await db.execute('ALTER TABLE users ADD COLUMN tg_username TEXT')
    except Exception:
        pass
    # achievement_progress table — tracks raw progress per achievement
    await db.execute('''
        CREATE TABLE IF NOT EXISTS achievement_progress (
            user_id INTEGER NOT NULL,
            achievement_name TEXT NOT NULL,
            progress INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, achievement_name)
        )
    ''')
    # user_achievements table — tiered unlocks (1=bronze, 2=silver, 3=gold)
    # migrate from old schema if needed
    cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_achievements'")
    old_exists = await cur.fetchone()
    if old_exists:
        cur = await db.execute("PRAGMA table_info(user_achievements)")
        cols = [c[1] for c in await cur.fetchall()]
        if 'tier' not in cols:
            # migrate old table: add tier column via recreate
            await db.execute('''
                CREATE TABLE user_achievements_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    achievement_name TEXT NOT NULL,
                    tier INTEGER DEFAULT 1,
                    unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, achievement_name, tier)
                )
            ''')
            await db.execute('INSERT INTO user_achievements_new (id, user_id, achievement_name, tier, unlocked_at) SELECT id, user_id, achievement_name, 1, unlocked_at FROM user_achievements')
            await db.execute('DROP TABLE user_achievements')
            await db.execute('ALTER TABLE user_achievements_new RENAME TO user_achievements')
    else:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS user_achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                achievement_name TEXT NOT NULL,
                tier INTEGER DEFAULT 1,
                unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, achievement_name, tier)
            )
        ''')
    await db.commit()

    # seed shop items
    await seed_shop()


async def get_or_create_user(tg_id: int, username: str | None, tg_username: str | None = None):
    db = await get_db()
    cur = await db.execute('SELECT id FROM users WHERE tg_id = ?', (tg_id,))
    row = await cur.fetchone()
    if row:
        await db.execute('UPDATE users SET username = ?, tg_username = ? WHERE id = ?',
                         (username, tg_username, row['id']))
        await db.commit()
        return row['id']
    cur = await db.execute('INSERT INTO users (tg_id, username, tg_username, coins) VALUES (?, ?, ?, 100)',
                           (tg_id, username, tg_username))
    await db.commit()
    return cur.lastrowid


async def get_user_by_dbid(user_db_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM users WHERE id = ?', (user_db_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def get_game_by_thread(chat_id: int, thread_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM games WHERE chat_id = ? AND thread_id = ? ORDER BY created_at DESC LIMIT 1', (chat_id, thread_id))
    row = await cur.fetchone()
    return dict(row) if row else None


async def get_game_by_id(game_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM games WHERE id = ? LIMIT 1', (game_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def set_game_lobby_message(game_id: int, message_id: int):
    db = await get_db()
    await db.execute('UPDATE games SET lobby_message_id = ? WHERE id = ?', (message_id, game_id))
    await db.commit()


async def create_game(chat_id: int, thread_id: int):
    db = await get_db()
    cur = await db.execute('INSERT INTO games (chat_id, thread_id, state) VALUES (?, ?, ?)', (chat_id, thread_id, 'lobby'))
    await db.commit()
    return cur.lastrowid


async def update_game_state(game_id: int, state: str):
    db = await get_db()
    await db.execute('UPDATE games SET state = ? WHERE id = ?', (state, game_id))
    await db.commit()


async def try_start_game(game_id: int) -> bool:
    """Atomically transition game from lobby to running. Returns True if succeeded."""
    db = await get_db()
    cur = await db.execute('UPDATE games SET state = ? WHERE id = ? AND state = ?', ('running', game_id, 'lobby'))
    await db.commit()
    return cur.rowcount > 0


async def set_crazy_mode(game_id: int, enabled: bool):
    db = await get_db()
    await db.execute('UPDATE games SET crazy_mode = ? WHERE id = ?', (1 if enabled else 0, game_id))
    await db.commit()


async def add_player(game_id: int, user_id: int, role: str = ''):
    db = await get_db()
    cur = await db.execute('SELECT id FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    if row:
        return False
    await db.execute('INSERT INTO players (game_id, user_id, role) VALUES (?, ?, ?)', (game_id, user_id, role))
    await db.commit()
    return True


async def set_player_role(game_id: int, user_id: int, role: str):
    db = await get_db()
    await db.execute('UPDATE players SET role = ? WHERE game_id = ? AND user_id = ?', (role, game_id, user_id))
    await db.commit()


async def remove_player(game_id: int, user_id: int):
    db = await get_db()
    await db.execute('DELETE FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    await db.commit()


async def list_players(game_id: int):
    db = await get_db()
    cur = await db.execute('''
        SELECT p.user_id, u.tg_id, u.username, p.role, p.alive
        FROM players p
        JOIN users u ON p.user_id = u.id
        WHERE p.game_id = ?
    ''', (game_id,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_players_for_game(game_id: int):
    """Return players with db id, tg_id, username and alive flag"""
    return await list_players(game_id)


async def get_player_role(game_id: int, user_id: int):
    db = await get_db()
    cur = await db.execute('SELECT role FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    return row['role'] if row else None


async def record_night_action(game_id: int, actor_user_id: int, role: str, target_user_id: int):
    db = await get_db()
    await db.execute('DELETE FROM night_actions WHERE game_id = ? AND actor_user_id = ?', (game_id, actor_user_id))
    await db.execute('INSERT INTO night_actions (game_id, actor_user_id, role, target_user_id) VALUES (?, ?, ?, ?)', (game_id, actor_user_id, role, target_user_id))
    await db.commit()


async def fetch_night_actions(game_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM night_actions WHERE game_id = ?', (game_id,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def clear_night_actions(game_id: int):
    db = await get_db()
    await db.execute('DELETE FROM night_actions WHERE game_id = ?', (game_id,))
    await db.commit()


async def is_player_alive(game_id: int, user_id: int) -> bool:
    db = await get_db()
    cur = await db.execute('SELECT alive FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    return row and row['alive'] == 1 if row else False


async def set_player_alive(game_id: int, user_id: int, alive: bool):
    db = await get_db()
    await db.execute('UPDATE players SET alive = ? WHERE game_id = ? AND user_id = ?', (1 if alive else 0, game_id, user_id))
    await db.commit()


async def set_player_framed(game_id: int, user_id: int, framed: int = 1):
    db = await get_db()
    await db.execute('UPDATE players SET framed = ? WHERE game_id = ? AND user_id = ?', (1 if framed else 0, game_id, user_id))
    await db.commit()


async def set_player_blackmailed(game_id: int, user_id: int, blackmailed: int = 1):
    db = await get_db()
    await db.execute('UPDATE players SET blackmailed = ? WHERE game_id = ? AND user_id = ?', (1 if blackmailed else 0, game_id, user_id))
    await db.commit()


async def get_player_flags(game_id: int, user_id: int):
    db = await get_db()
    cur = await db.execute('SELECT framed, blackmailed, disguised FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    return dict(row) if row else {'framed':0,'blackmailed':0,'disguised':0}


async def set_player_disguised(game_id: int, user_id: int, disguised: int = 1):
    db = await get_db()
    await db.execute('UPDATE players SET disguised = ? WHERE game_id = ? AND user_id = ?', (1 if disguised else 0, game_id, user_id))
    await db.commit()


async def set_protection_used(game_id: int, user_id: int):
    """Mark that protection item has been used this game for this player."""
    db = await get_db()
    await db.execute('UPDATE players SET protection_used = 1 WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    await db.commit()


async def get_protection_used(game_id: int, user_id: int) -> bool:
    db = await get_db()
    cur = await db.execute('SELECT protection_used FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    return bool(row and row['protection_used']) if row else False


async def reset_protection_used(game_id: int):
    """Reset protection_used flag for all players in a game (game finished)."""
    db = await get_db()
    await db.execute('UPDATE players SET protection_used = 0 WHERE game_id = ?', (game_id,))
    await db.commit()


async def set_doctor_last_heal(game_id: int, doctor_user_id: int, target_user_id: int):
    db = await get_db()
    await db.execute('UPDATE players SET doctor_last_heal = ? WHERE game_id = ? AND user_id = ?',
                     (target_user_id, game_id, doctor_user_id))
    await db.commit()


async def get_doctor_last_heal(game_id: int, doctor_user_id: int) -> int | None:
    db = await get_db()
    cur = await db.execute('SELECT doctor_last_heal FROM players WHERE game_id = ? AND user_id = ?',
                           (game_id, doctor_user_id))
    row = await cur.fetchone()
    return row['doctor_last_heal'] if row and row['doctor_last_heal'] else None


async def clear_doctor_last_heal(game_id: int):
    db = await get_db()
    await db.execute('UPDATE players SET doctor_last_heal = 0 WHERE game_id = ?', (game_id,))
    await db.commit()


async def get_alive_players(game_id: int):
    db = await get_db()
    cur = await db.execute('''
        SELECT p.user_id, u.tg_id, u.username
        FROM players p
        JOIN users u ON p.user_id = u.id
        WHERE p.game_id = ? AND p.alive = 1
    ''', (game_id,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def record_vote(game_id: int, voter_user_id: int, target_user_id: int):
    db = await get_db()
    await db.execute('DELETE FROM votes WHERE game_id = ? AND voter_user_id = ?', (game_id, voter_user_id))
    await db.execute('INSERT INTO votes (game_id, voter_user_id, target_user_id) VALUES (?, ?, ?)', (game_id, voter_user_id, target_user_id))
    await db.commit()


async def get_votes_for_game(game_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM votes WHERE game_id = ?', (game_id,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def clear_votes(game_id: int):
    db = await get_db()
    await db.execute('DELETE FROM votes WHERE game_id = ?', (game_id,))
    await db.commit()


async def record_lynch_vote(game_id: int, candidate_user_id: int, voter_user_id: int, choice: int):
    db = await get_db()
    await db.execute('DELETE FROM lynch_votes WHERE game_id = ? AND candidate_user_id = ? AND voter_user_id = ?', (game_id, candidate_user_id, voter_user_id))
    await db.execute('INSERT INTO lynch_votes (game_id, candidate_user_id, voter_user_id, choice) VALUES (?, ?, ?, ?)', (game_id, candidate_user_id, voter_user_id, choice))
    await db.commit()


async def get_lynch_vote_choice(game_id: int, candidate_user_id: int, voter_user_id: int):
    """Return the existing choice value for a voter's lynch vote or None."""
    db = await get_db()
    cur = await db.execute('SELECT choice FROM lynch_votes WHERE game_id = ? AND candidate_user_id = ? AND voter_user_id = ? LIMIT 1', (game_id, candidate_user_id, voter_user_id))
    row = await cur.fetchone()
    return int(row['choice']) if row else None


async def get_lynch_vote_counts(game_id: int, candidate_user_id: int):
    db = await get_db()
    cur = await db.execute('SELECT choice, COUNT(*) as cnt FROM lynch_votes WHERE game_id = ? AND candidate_user_id = ? GROUP BY choice', (game_id, candidate_user_id))
    rows = await cur.fetchall()
    counts = {int(r['choice']): r['cnt'] for r in rows}
    return counts


async def get_lynch_votes_details(game_id: int, candidate_user_id: int):
    """Return list of {voter_user_id, choice} for lynch votes for this candidate."""
    db = await get_db()
    cur = await db.execute('SELECT voter_user_id, choice FROM lynch_votes WHERE game_id = ? AND candidate_user_id = ?', (game_id, candidate_user_id))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def clear_lynch_votes(game_id: int, candidate_user_id: int = None):
    db = await get_db()
    if candidate_user_id:
        await db.execute('DELETE FROM lynch_votes WHERE game_id = ? AND candidate_user_id = ?', (game_id, candidate_user_id))
    else:
        await db.execute('DELETE FROM lynch_votes WHERE game_id = ?', (game_id,))
    await db.commit()


async def set_lynch_message(game_id: int, candidate_user_id: int, chat_id: int, message_id: int):
    db = await get_db()
    await db.execute('DELETE FROM lynch_messages WHERE game_id = ? AND candidate_user_id = ?', (game_id, candidate_user_id))
    await db.execute('INSERT INTO lynch_messages (game_id, candidate_user_id, chat_id, message_id) VALUES (?, ?, ?, ?)', (game_id, candidate_user_id, chat_id, message_id))
    await db.commit()


async def get_lynch_message(game_id: int, candidate_user_id: int):
    db = await get_db()
    cur = await db.execute('SELECT * FROM lynch_messages WHERE game_id = ? AND candidate_user_id = ? LIMIT 1', (game_id, candidate_user_id))
    row = await cur.fetchone()
    return dict(row) if row else None


async def clear_lynch_message(game_id: int, candidate_user_id: int):
    db = await get_db()
    await db.execute('DELETE FROM lynch_messages WHERE game_id = ? AND candidate_user_id = ?', (game_id, candidate_user_id))
    await db.commit()


async def clear_player_flags(game_id: int):
    """Clear framed/blackmailed/disguised flags for all players in the game."""
    db = await get_db()
    await db.execute('UPDATE players SET framed = 0, blackmailed = 0, disguised = 0 WHERE game_id = ?', (game_id,))
    await db.commit()


async def game_has_player(game_id: int, user_id: int):
    db = await get_db()
    cur = await db.execute('SELECT 1 FROM players WHERE game_id = ? AND user_id = ? LIMIT 1', (game_id, user_id))
    return await cur.fetchone() is not None


async def set_game_phase_deadline(game_id: int, deadline_ts: int | None):
    """Set the absolute epoch deadline (seconds) for the game's current phase. Use None to clear."""
    db = await get_db()
    if deadline_ts is None:
        await db.execute('UPDATE games SET phase_deadline = NULL WHERE id = ?', (game_id,))
    else:
        await db.execute('UPDATE games SET phase_deadline = ? WHERE id = ?', (int(deadline_ts), game_id))
    await db.commit()


async def get_games_by_state(state: str):
    db = await get_db()
    cur = await db.execute('SELECT * FROM games WHERE state = ?', (state,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]

SHOP_ITEMS = [
    {'name': 'documents', 'display': '📄 Документ', 'price': 150,
     'description': 'Скрывает вашу роль от проверки Комиссара (однократно).'},
    {'name': 'protection', 'display': '🛡️ Защита', 'price': 100,
     'description': 'Защищает от смерти ночью (1 раз за игру). Не работает против повешения.'},
    {'name': 'disguise', 'display': '🕶️ Маскировка', 'price': 150,
     'description': 'Советник и Слежка увидят вас как Неизвестного одну ночь (однократно).'},
    {'name': 'anonymous', 'display': '📱 Анонимка', 'price': 50,
     'description': 'Отправляет анонимное сообщение в игровой чат (однократно).'},
    {'name': 'lottery', 'display': '🎲 Счастливый билет', 'price': 50,
     'description': '50% шанс выиграть 100 🪙, 50% — ничего.'},
    {'name': 'poisoned_dagger', 'display': '🔪 Отравленный кинжал', 'price': 100,
     'description': 'Если вас повесят — один из голосовавших «За» тоже умрёт (1 раз за игру).'},
    {'name': 'antidote', 'display': '💊 Антидот', 'price': 200,
     'description': 'Спасает от Отравленного кинжала (1 раз за игру).'},
    {'name': 'alibi', 'display': '🔗 Алиби', 'price': 100,
     'description': 'Автоматически +1 голос «Против» при вашем линчевании (1 раз за игру).'},
    {'name': 'slow_poison', 'display': '🧪 Яд медленного действия', 'price': 300,
     'description': 'Если вас убили ночью — ваш убийца умрёт следующей ночью (1 раз за игру).'},
]

DIAMOND_SHOP_ITEMS = [
    {'name': 'active_role', 'display': '🎯 Активная роль', 'price': 1,
     'description': '99% шанс получить активную роль в следующей игре.'},
    {'name': 'allseeing', 'display': '👁️ Всевидящий', 'price': 150,
     'description': 'Ночью может раскрыть роль любого игрока (1 раз за игру).'},
    {'name': 'double', 'display': '👤 Двойник', 'price': 100,
     'description': 'Если вас должны убить ночью — вместо вас умирает случайный живой (1 раз за игру).'},
    {'name': 'carnival_mask', 'display': '🎪 Карнавальная маска', 'price': 20,
     'description': 'На один день ваше имя в списке живых сменится на «Неизвестный» (1 раз за игру).'},
]

# Packages buyable with Telegram Stars
COIN_PACKAGES = [
    {'stars': 10,  'coins': 100,  'label': '100 🪙'},
    {'stars': 20,  'coins': 250,  'label': '250 🪙'},
    {'stars': 40,  'coins': 500,  'label': '500 🪙'},
    {'stars': 70,  'coins': 1000, 'label': '1 000 🪙'},
    {'stars': 150, 'coins': 2500, 'label': '2 500 🪙'},
    {'stars': 250, 'coins': 5000, 'label': '5 000 🪙'},
]

DIAMOND_PACKAGES = [
    {'stars': 15,  'diamonds': 5,   'label': '5 💎'},
    {'stars': 25,  'diamonds': 10,  'label': '10 💎'},
    {'stars': 40,  'diamonds': 20,  'label': '20 💎'},
    {'stars': 80,  'diamonds': 50,  'label': '50 💎'},
    {'stars': 140, 'diamonds': 100, 'label': '100 💎'},
]

ACHIEVEMENTS = [
    {'name': 'first_lynch', 'display': '🔗 Первый линч', 'desc': 'Быть повешенным', 'tiers': [1, 5, 20], 'coins': [10, 25, 50]},
    {'name': 'jester_win', 'display': '🤡 Шут-трик', 'desc': 'Победить как Шут', 'tiers': [1, 3, 10], 'coins': [30, 60, 100]},
    {'name': 'vigilante_godfather', 'display': '🦸 Охотник на Дона', 'desc': 'Убить Дона будучи Охотником', 'tiers': [1, 3, 10], 'coins': [25, 50, 100]},
    {'name': 'gray_cardinal_win', 'display': '🃏 Инкогнито', 'desc': 'Победить как СК', 'tiers': [1, 3, 10], 'coins': [30, 60, 100]},
    {'name': 'survivor_win', 'display': '🏃 Живучий', 'desc': 'Выжить как Выживший', 'tiers': [1, 3, 10], 'coins': [20, 40, 80]},
    {'name': 'serial_killer_3', 'display': '🔪 Жажда крови', 'desc': '3+ убийств за игру (СК)', 'tiers': [1, 3, 10], 'coins': [50, 100, 200]},
    {'name': 'doctor_3_saves', 'display': '💉 Ангел-хранитель', 'desc': '3+ спасений за игру', 'tiers': [1, 3, 10], 'coins': [30, 60, 100]},
    {'name': 'mafia_win_3', 'display': '😈 Мафиозный авторитет', 'desc': 'Победить как Мафия', 'tiers': [3, 15, 50], 'coins': [20, 50, 100]},
    {'name': 'town_win_5', 'display': '👤 Ветеран города', 'desc': 'Победить как Мирный', 'tiers': [5, 20, 50], 'coins': [25, 60, 120]},
    {'name': 'double_save', 'display': '🔄 Фальшивая смерть', 'desc': 'Выжить с Двойником', 'tiers': [1, 3, 10], 'coins': [15, 30, 60]},
    {'name': 'poison_revenge', 'display': '🧪 Ядовитая месть', 'desc': 'Отомстить Ядом', 'tiers': [1, 3, 10], 'coins': [35, 70, 150]},
    {'name': 'first_win', 'display': '🏆 Первая победа', 'desc': 'Всего побед', 'tiers': [1, 10, 50], 'coins': [10, 30, 60]},
    {'name': 'detective_godfather', 'display': '🔍 Раскрытие Дона', 'desc': 'Найти Дона детективом', 'tiers': [1, 3, 10], 'coins': [25, 50, 100]},
    {'name': 'triple_kill_night', 'display': '⚡ Ночная резня', 'desc': '3+ убийств за ночь', 'tiers': [1, 3, 10], 'coins': [40, 80, 160]},
    {'name': 'game_50', 'display': '🎲 Ветеран', 'desc': 'Всего сыграно игр', 'tiers': [50, 200, 500], 'coins': [15, 35, 70]},
    {'name': 'item_collector_5', 'display': '🎒 Коллекционер', 'desc': 'Разных предметов сразу', 'tiers': [5, 15, 30], 'coins': [10, 25, 50]},
]

TIER_EMOJIS = ['', '🥉', '🥈', '🥇']

async def seed_shop():
    db = await get_db()
    await db.executemany(
        '''INSERT INTO shop_items (name, price, data) VALUES (?, ?, ?)
           ON CONFLICT(name) DO UPDATE SET price = excluded.price, data = excluded.data''',
        [(i['name'], i['price'], i['display']) for i in SHOP_ITEMS]
    )
    await db.commit()

async def get_shop_items():
    db = await get_db()
    cur = await db.execute('SELECT * FROM shop_items')
    rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def get_user_item_count(user_id: int, item_name: str) -> int:
    db = await get_db()
    cur = await db.execute('SELECT COALESCE(SUM(quantity), 0) FROM user_items WHERE user_id = ? AND item_name = ?',
                          (user_id, item_name))
    row = await cur.fetchone()
    return row[0] if row else 0

async def add_user_item(user_id: int, item_name: str, quantity: int = 1):
    db = await get_db()
    cur = await db.execute('SELECT id FROM user_items WHERE user_id = ? AND item_name = ?',
                          (user_id, item_name))
    row = await cur.fetchone()
    if row:
        await db.execute('UPDATE user_items SET quantity = quantity + ? WHERE id = ?', (quantity, row['id']))
    else:
        await db.execute('INSERT INTO user_items (user_id, item_name, quantity) VALUES (?, ?, ?)',
                        (user_id, item_name, quantity))
    await db.commit()

async def use_user_item(user_id: int, item_name: str) -> bool:
    """Consume one item. Returns True if item was available and consumed."""
    db = await get_db()
    cur = await db.execute('SELECT id, quantity FROM user_items WHERE user_id = ? AND item_name = ?',
                          (user_id, item_name))
    row = await cur.fetchone()
    if not row:
        return False
    if row['quantity'] > 1:
        await db.execute('UPDATE user_items SET quantity = quantity - 1 WHERE id = ?', (row['id'],))
    else:
        await db.execute('DELETE FROM user_items WHERE id = ?', (row['id'],))
    await db.commit()
    return True

async def get_user_items_with_details(user_id: int) -> list[dict]:
    """Return list of {name, display, quantity} for user's items."""
    db = await get_db()
    cur = await db.execute('SELECT item_name, quantity FROM user_items WHERE user_id = ?', (user_id,))
    rows = await cur.fetchall()
    shop_map = {item['name']: item['display'] for item in SHOP_ITEMS}
    shop_map.update({item['name']: item['display'] for item in DIAMOND_SHOP_ITEMS})
    result = []
    for r in rows:
        name = r['item_name']
        result.append({'name': name, 'display': shop_map.get(name, name), 'quantity': r['quantity']})
    return result

async def add_coins(user_id: int, amount: int, reason: str = ''):
    db = await get_db()
    await db.execute('UPDATE users SET coins = coins + ? WHERE id = ?', (amount, user_id))
    await db.execute('INSERT INTO transactions (user_id, amount, reason) VALUES (?, ?, ?)',
                    (user_id, amount, reason or None))
    await db.commit()

async def add_diamonds(user_id: int, amount: int, reason: str = ''):
    db = await get_db()
    await db.execute('UPDATE users SET diamonds = diamonds + ? WHERE id = ?', (amount, user_id))
    await db.execute('INSERT INTO transactions (user_id, amount, reason) VALUES (?, ?, ?)',
                    (user_id, amount, reason or None))
    await db.commit()

async def add_win(user_id: int):
    db = await get_db()
    await db.execute('UPDATE users SET wins = wins + 1 WHERE id = ?', (user_id,))
    await db.commit()

async def add_loss(user_id: int):
    db = await get_db()
    await db.execute('UPDATE users SET losses = losses + 1 WHERE id = ?', (user_id,))
    await db.commit()

DAILY_COOLDOWN = 86400  # 24 hours

async def claim_daily(user_id: int) -> tuple[bool, str]:
    """Claim daily reward. Random 25-75 coins, with milestone bonuses at 100, 500, 1000. Returns (success, message)."""
    db = await get_db()
    cur = await db.execute('SELECT last_daily, daily_count FROM users WHERE id = ?', (user_id,))
    row = await cur.fetchone()
    if not row:
        return False, '❌ Пользователь не найден.'
    last_raw = row['last_daily']
    daily_count = row['daily_count'] or 0
    now = datetime.now()
    if last_raw:
        try:
            last_dt = datetime.fromisoformat(last_raw) if isinstance(last_raw, str) else last_raw
            elapsed = (now - last_dt).total_seconds()
            if elapsed < DAILY_COOLDOWN:
                remaining = int(DAILY_COOLDOWN - elapsed)
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                return False, f'⏳ Бонус уже получен. Следующий через {hours} ч {minutes} мин.'
        except Exception:
            pass

    base = random.randint(25, 75)
    total_coins = base
    milestone_msg = ''

    new_count = daily_count + 1
    if new_count == 100:
        bonus = random.randint(100, 250)
        total_coins += bonus
        milestone_msg = f'\n🎉 <b>100-й день!</b> Бонус +{bonus} 🪙'
    elif new_count == 500:
        bonus = random.randint(500, 1000)
        total_coins += bonus
        milestone_msg = f'\n🎊 <b>500-й день!</b> Бонус +{bonus} 🪙'
    elif new_count == 1000:
        bonus_diamonds = random.randint(1, 5)
        await db.execute('UPDATE users SET diamonds = diamonds + ? WHERE id = ?',
                         (bonus_diamonds, user_id))
        milestone_msg = f'\n💎 <b>1000-й день!</b> Бонус +{bonus_diamonds} 💎'
        await db.execute('INSERT INTO transactions (user_id, amount, reason) VALUES (?, ?, ?)',
                         (user_id, bonus_diamonds, 'Ежедневный бонус 1000 дней (💎)'))

    if new_count == 1000:
        await db.execute('UPDATE users SET coins = coins + ?, daily_count = 0, last_daily = ? WHERE id = ?',
                         (total_coins, now.isoformat(), user_id))
    else:
        await db.execute('UPDATE users SET coins = coins + ?, daily_count = daily_count + 1, last_daily = ? WHERE id = ?',
                         (total_coins, now.isoformat(), user_id))
    await db.execute('INSERT INTO transactions (user_id, amount, reason) VALUES (?, ?, ?)',
                     (user_id, total_coins, 'Ежедневный бонус'))
    await db.commit()

    text = f'🎉 Вы получили <b>+{base}</b> 🪙 за ежедневный вход!{milestone_msg}'
    return True, text


# ── Last words persistence ──────────────────────────────────────────────────

async def create_last_word(game_id: int, user_id: int, tg_id: int, chat_id: int, thread_id: int, name: str):
    db = await get_db()
    await db.execute(
        'INSERT INTO last_words (game_id, user_id, tg_id, chat_id, thread_id, name) VALUES (?, ?, ?, ?, ?, ?)',
        (game_id, user_id, tg_id, chat_id, thread_id, name)
    )
    await db.commit()


async def get_pending_last_words() -> list[dict]:
    """Return all uncollected last words (for restoring after restart)."""
    db = await get_db()
    cur = await db.execute('SELECT * FROM last_words WHERE collected = 0')
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def submit_last_word(tg_id: int, text: str):
    """Mark the last word as collected with the given text."""
    db = await get_db()
    await db.execute('UPDATE last_words SET text = ?, collected = 1 WHERE tg_id = ? AND collected = 0',
                     (text, tg_id))
    await db.commit()


async def get_collected_last_words(game_id: int) -> list[dict]:
    db = await get_db()
    cur = await db.execute('SELECT * FROM last_words WHERE game_id = ? AND collected = 1 ORDER BY created_at ASC',
                           (game_id,))
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def delete_last_word(lw_id: int):
    db = await get_db()
    await db.execute('DELETE FROM last_words WHERE id = ?', (lw_id,))
    await db.commit()


async def delete_last_words_by_game(game_id: int):
    db = await get_db()
    await db.execute('DELETE FROM last_words WHERE game_id = ?', (game_id,))
    await db.commit()


# ── Once-per-game item usage tracking ───────────────────────────────────────

async def mark_item_used(game_id: int, user_id: int, item_name: str):
    db = await get_db()
    await db.execute(
        'INSERT OR IGNORE INTO game_item_usage (game_id, user_id, item_name) VALUES (?, ?, ?)',
        (game_id, user_id, item_name)
    )
    await db.commit()


async def is_item_used(game_id: int, user_id: int, item_name: str) -> bool:
    db = await get_db()
    cur = await db.execute(
        'SELECT 1 FROM game_item_usage WHERE game_id = ? AND user_id = ? AND item_name = ?',
        (game_id, user_id, item_name)
    )
    return await cur.fetchone() is not None


async def clear_item_usage(game_id: int):
    db = await get_db()
    await db.execute('DELETE FROM game_item_usage WHERE game_id = ?', (game_id,))
    await db.commit()


# ── Carnival mask ────────────────────────────────────────────────────────────

async def set_player_masked(game_id: int, user_id: int):
    """Активировать маску — действует 2 ночи."""
    db = await get_db()
    await db.execute('UPDATE players SET masked = 2 WHERE game_id = ? AND user_id = ?',
                     (game_id, user_id))
    await db.commit()


async def get_player_masked(game_id: int, user_id: int) -> bool:
    db = await get_db()
    cur = await db.execute('SELECT masked FROM players WHERE game_id = ? AND user_id = ?', (game_id, user_id))
    row = await cur.fetchone()
    return row and row['masked'] > 0 if row else False


async def tick_masked_flags(game_id: int):
    """Уменьшить счётчик маски на 1 для всех замаскированных."""
    db = await get_db()
    await db.execute('UPDATE players SET masked = masked - 1 WHERE game_id = ? AND masked > 0', (game_id,))
    await db.commit()


async def clear_masked_flags(game_id: int):
    """Полный сброс маски (при завершении игры)."""
    db = await get_db()
    await db.execute('UPDATE players SET masked = 0 WHERE game_id = ?', (game_id,))
    await db.commit()


# ── Slow poison (отложенный яд) ─────────────────────────────────────────────

async def add_pending_poison_kill(game_id: int, target_user_id: int):
    db = await get_db()
    await db.execute('INSERT INTO pending_poison_kills (game_id, target_user_id) VALUES (?, ?)',
                     (game_id, target_user_id))
    await db.commit()


async def get_pending_poison_kills(game_id: int) -> list[int]:
    db = await get_db()
    cur = await db.execute('SELECT target_user_id FROM pending_poison_kills WHERE game_id = ?', (game_id,))
    rows = await cur.fetchall()
    return [r['target_user_id'] for r in rows]


async def clear_pending_poison_kills(game_id: int):
    db = await get_db()
    await db.execute('DELETE FROM pending_poison_kills WHERE game_id = ?', (game_id,))
    await db.commit()


# ── Role stats (статистика по ролям) ─────────────────────────────────────────

async def record_role_stat(user_id: int, role_name: str, won: bool):
    db = await get_db()
    role_name = role_name or 'Unknown'
    await db.execute('''
        INSERT INTO role_stats (user_id, role_name, wins, losses, games)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(user_id, role_name) DO UPDATE SET
            wins = wins + ?,
            losses = losses + ?,
            games = games + 1
    ''', (user_id, role_name, 1 if won else 0, 1 if not won else 0,
          1 if won else 0, 1 if not won else 0))
    await db.commit()


async def get_role_stats(user_id: int) -> list[dict]:
    db = await get_db()
    cur = await db.execute(
        'SELECT role_name, wins, losses, games FROM role_stats WHERE user_id = ? ORDER BY games DESC',
        (user_id,)
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_total_play_seconds(user_id: int) -> int:
    db = await get_db()
    cur = await db.execute('SELECT total_play_seconds FROM users WHERE id = ?', (user_id,))
    row = await cur.fetchone()
    return row['total_play_seconds'] if row else 0


async def add_play_seconds(user_id: int, seconds: int):
    db = await get_db()
    await db.execute('UPDATE users SET total_play_seconds = total_play_seconds + ? WHERE id = ?',
                     (seconds, user_id))
    await db.commit()


async def set_game_started(game_id: int):
    db = await get_db()
    await db.execute('UPDATE games SET started_at = ? WHERE id = ?',
                     (datetime.now().isoformat(), game_id))
    await db.commit()


async def get_game_started(game_id: int) -> str | None:
    db = await get_db()
    cur = await db.execute('SELECT started_at FROM games WHERE id = ?', (game_id,))
    row = await cur.fetchone()
    return row['started_at'] if row else None


# ── Gray Cardinal (запись раскрытия роли) ────────────────────────────────────

async def set_role_revealed(game_id: int, user_id: int, revealed: int = 1):
    db = await get_db()
    await db.execute('UPDATE players SET role_revealed = ? WHERE game_id = ? AND user_id = ?',
                     (revealed, game_id, user_id))
    await db.commit()


async def get_role_revealed(game_id: int, user_id: int) -> bool:
    db = await get_db()
    cur = await db.execute('SELECT role_revealed FROM players WHERE game_id = ? AND user_id = ?',
                           (game_id, user_id))
    row = await cur.fetchone()
    return bool(row and row['role_revealed']) if row else False


# ── Achievements (ачивки) ────────────────────────────────────────────────────

async def add_progress(user_id: int, achievement_name: str, amount: int = 1) -> int | None:
    """Increment progress for an achievement and unlock any new tiers.
    Returns the newly unlocked tier (1-3) or None if no new tier was unlocked."""
    db = await get_db()
    await db.execute('''
        INSERT INTO achievement_progress (user_id, achievement_name, progress)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, achievement_name) DO UPDATE SET progress = progress + ?
    ''', (user_id, achievement_name, amount, amount))
    cur = await db.execute('SELECT progress FROM achievement_progress WHERE user_id = ? AND achievement_name = ?',
                           (user_id, achievement_name))
    row = await cur.fetchone()
    if not row:
        return None
    progress = row[0]
    meta = next((a for a in ACHIEVEMENTS if a['name'] == achievement_name), None)
    if not meta:
        return None
    unlocked_tier = None
    for i, threshold in enumerate(meta['tiers']):
        tier = i + 1
        if progress >= threshold:
            try:
                await db.execute('INSERT INTO user_achievements (user_id, achievement_name, tier) VALUES (?, ?, ?)',
                                 (user_id, achievement_name, tier))
                unlocked_tier = tier
            except Exception:
                pass  # already unlocked
    await db.commit()
    return unlocked_tier


async def unlock_achievement(user_id: int, achievement_name: str) -> bool:
    """Legacy wrapper — unlocks bronze tier (tier=1). Returns True if newly unlocked."""
    return (await add_progress(user_id, achievement_name, 1)) is not None


async def get_user_achievements(user_id: int) -> list[dict]:
    db = await get_db()
    cur = await db.execute(
        'SELECT achievement_name, tier, unlocked_at FROM user_achievements WHERE user_id = ? ORDER BY unlocked_at',
        (user_id,)
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_achievement_count(user_id: int) -> int:
    """Count total unique achievement types unlocked (at any tier)."""
    db = await get_db()
    cur = await db.execute('SELECT COUNT(DISTINCT achievement_name) FROM user_achievements WHERE user_id = ?', (user_id,))
    row = await cur.fetchone()
    return row[0] if row else 0


async def get_distinct_item_count(user_id: int) -> int:
    """Count how many distinct item types the user owns."""
    db = await get_db()
    cur = await db.execute('SELECT COUNT(*) FROM user_items WHERE user_id = ? AND quantity > 0', (user_id,))
    row = await cur.fetchone()
    return row[0] if row else 0


# ── Item transfer (передача предметов) ───────────────────────────────────────

async def get_user_by_username(username: str) -> dict | None:
    """Find user by @username (tg_username) or display name (username)."""
    db = await get_db()
    cur = await db.execute('SELECT * FROM users WHERE tg_username = ? OR username = ?',
                           (username, username))
    row = await cur.fetchone()
    return dict(row) if row else None


async def transfer_item(from_user_id: int, to_user_id: int, item_name: str, quantity: int = 1) -> tuple[bool, str]:
    """Transfer item from one user to another. Returns (success, message)."""
    db = await get_db()
    cur = await db.execute('SELECT id, quantity FROM user_items WHERE user_id = ? AND item_name = ?',
                           (from_user_id, item_name))
    row = await cur.fetchone()
    if not row or row['quantity'] < quantity:
        return False, f'У вас нет {quantity} шт. этого предмета.'
    # deduct from sender
    if row['quantity'] > quantity:
        await db.execute('UPDATE user_items SET quantity = quantity - ? WHERE id = ?', (quantity, row['id']))
    else:
        await db.execute('DELETE FROM user_items WHERE id = ?', (row['id'],))
    # add to receiver
    cur2 = await db.execute('SELECT id FROM user_items WHERE user_id = ? AND item_name = ?',
                            (to_user_id, item_name))
    existing = await cur2.fetchone()
    if existing:
        await db.execute('UPDATE user_items SET quantity = quantity + ? WHERE id = ?', (quantity, existing['id']))
    else:
        await db.execute('INSERT INTO user_items (user_id, item_name, quantity) VALUES (?, ?, ?)',
                         (to_user_id, item_name, quantity))
    await db.commit()
    return True, '✅ Предмет(ы) успешно переданы!'


# ── Batch game-end update (одна транзакция на всех игроков) ──────────────────

async def batch_game_end(results: list[dict]):
    """results: [{user_id, coins, won, role_name, play_seconds, reason}, ...]"""
    db = await get_db()
    for r in results:
        uid = r['user_id']
        won = r['won']
        coins = r.get('coins', 0) if won else 0
        secs = r.get('play_seconds', 0)
        await db.execute(
            'UPDATE users SET coins = coins + ?, wins = wins + ?, losses = losses + ?, '
            'total_play_seconds = total_play_seconds + ? WHERE id = ?',
            (coins, 1 if won else 0, 1 if not won else 0, secs, uid)
        )
        await db.execute('INSERT INTO transactions (user_id, amount, reason) VALUES (?, ?, ?)',
                         (uid, coins, r.get('reason', '')))
        await db.execute('''
            INSERT INTO role_stats (user_id, role_name, wins, losses, games)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(user_id, role_name) DO UPDATE SET
                wins = wins + ?, losses = losses + ?, games = games + 1
        ''', (uid, r['role_name'], 1 if won else 0, 1 if not won else 0,
              1 if won else 0, 1 if not won else 0))
    await db.commit()
