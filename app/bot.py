import asyncio
import logging
import random
import time
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardButton, CallbackQuery, LabeledPrice, PreCheckoutQuery, InlineKeyboardMarkup
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
import os
from config import BOT_TOKEN, DB_PATH
from .db import (
    init_db, close_db, get_game_by_thread, create_game, get_or_create_user, add_player, list_players,
    remove_player, update_game_state, try_start_game, set_crazy_mode, game_has_player, set_player_role, get_user_by_dbid, record_night_action, get_players_for_game, get_player_role,
    fetch_night_actions, clear_night_actions, is_player_alive, set_player_alive, get_alive_players, record_vote, get_votes_for_game, clear_votes, get_game_by_id, set_game_lobby_message,
    record_lynch_vote, get_lynch_vote_counts, get_lynch_votes_details, get_lynch_vote_choice, clear_lynch_votes, set_player_framed, get_player_flags, set_player_blackmailed, clear_player_flags,
    set_player_disguised, set_game_phase_deadline, get_games_by_state, set_lynch_message, get_lynch_message, clear_lynch_message,
    get_shop_items, get_user_item_count, add_user_item, use_user_item, add_coins, add_diamonds, add_win, add_loss,
    get_user_items_with_details, claim_daily, set_doctor_last_heal, get_doctor_last_heal, clear_doctor_last_heal, SHOP_ITEMS, DIAMOND_SHOP_ITEMS, COIN_PACKAGES, DIAMOND_PACKAGES,
    create_last_word, get_pending_last_words, submit_last_word, get_collected_last_words, delete_last_word, delete_last_words_by_game,
    set_protection_used, get_protection_used, reset_protection_used,
    mark_item_used, is_item_used, clear_item_usage,
    set_player_masked, get_player_masked, tick_masked_flags, clear_masked_flags,
    add_pending_poison_kill, get_pending_poison_kills, clear_pending_poison_kills,
    record_role_stat, get_role_stats, get_total_play_seconds, add_play_seconds,
    set_game_started, get_game_started, batch_game_end,
    set_role_revealed, get_role_revealed,
    add_progress, get_user_achievements, get_achievement_count, get_distinct_item_count,
    get_user_by_username, transfer_item, ACHIEVEMENTS, TIER_EMOJIS,
)
from . import roles as roles_module
import sqlite3

# Logging to console and file
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('mafia_bot')
file_handler = logging.FileHandler('mafia_bot.log')
file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s'))
logger.addHandler(file_handler)

MIN_PLAYERS = 4
MAX_PLAYERS = roles_module.MAX_PLAYERS
# Lobby / phase timing (from .env via config.py, which calls load_dotenv)
LOBBY_INITIAL = int(os.getenv('LOBBY_INITIAL', '180'))
LOBBY_SHORT = int(os.getenv('LOBBY_SHORT', '90'))
LOBBY_JOIN_EXTENSION = int(os.getenv('LOBBY_JOIN_EXTENSION', '30'))
NIGHT_DURATION = int(os.getenv('NIGHT_DURATION', '60'))
DISCUSSION_DURATION = int(os.getenv('DISCUSSION_DURATION', '60'))
VOTING_DURATION = int(os.getenv('VOTING_DURATION', '45'))
LYNCH_DECISION_DURATION = int(os.getenv('LYNCH_DECISION_DURATION', '45'))

# In-memory lobby timer tasks (game_id -> asyncio.Task)
LOBBY_TASKS: dict = {}

# In-memory set for pending last word tg_ids (populated from DB at startup, used for fast lookup)
last_words_pending_tg_ids: set[int] = set()

# In-memory night action tracker — avoids 60 DB polls per night cycle
_night_actors: dict[int, set[int]] = {}
_night_actors_lock = asyncio.Lock()

# Per-game achievement tracking
_game_sk_kills: dict[int, int] = {}  # game_id -> SK kill count
_game_vigilante_gf: dict[int, bool] = {}  # game_id -> True if Vigilante killed Godfather
_game_doctor_saves: dict[int, dict[int, set[int]]] = {}  # game_id -> {doctor_uid: {saved_uid, ...}}
_game_night_kills: dict[int, dict[int, int]] = {}  # game_id -> {actor_uid: kill_count_this_night}

def _ru_plural(n: int, words: tuple[str, str, str]) -> str:
    """Russian pluralization helper. words = (nom_sg, gen_sg, gen_pl).
    e.g. _ru_plural(3, ('минута', 'минуты', 'минут')) -> '3 минуты'
    """
    if n % 10 == 1 and n % 100 != 11:
        return f'{n} {words[0]}'
    if 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return f'{n} {words[1]}'
    return f'{n} {words[2]}'


async def lobby_countdown(game_id: int, chat_id: int, thread_id: int, bot: Bot):
    """Manage lobby timers: initial wait for MIN_PLAYERS, then short countdown with extensions on joins.

    Behavior:
    - Initial window: LOBBY_INITIAL seconds to gather MIN_PLAYERS. If MIN not reached -> cancel lobby.
    - Once MIN_PLAYERS reached: set short timer LOBBY_SHORT.
      - If a new player joins during short timer, extend remaining by LOBBY_JOIN_EXTENSION and announce.
      - When remaining <= 60s announce "1 minute left" once.
    - When timer expires and MIN_PLAYERS met -> start game (set state running and launch run_game_loop).
    Persist deadlines to DB so restarts can resume.
    """
    try:
        prev_count = 0
        phase = 'initial'
        one_min_notified = False

        # attempt to restore deadline from DB
        try:
            g = await get_game_by_id(game_id)
            db_deadline = g.get('phase_deadline') if g else None
        except Exception:
            db_deadline = None

        now_epoch = int(time.time())
        loop_now = asyncio.get_event_loop().time()
        last_announced_min = None
        if db_deadline:
            remaining = int(db_deadline) - now_epoch
            if remaining <= 0:
                # deadline already passed; set loop deadline to now so it will trigger
                deadline = loop_now
            else:
                deadline = loop_now + remaining
        else:
            # set initial deadline and persist
            deadline = loop_now + LOBBY_INITIAL
            try:
                await set_game_phase_deadline(game_id, int(time.time()) + LOBBY_INITIAL)
            except Exception:
                logger.exception('Failed to persist initial lobby deadline for %s', game_id)

        # announce initial lobby timer to thread
        try:
            remaining = int(deadline - asyncio.get_event_loop().time())
            mins = remaining // 60
            secs = remaining % 60
            await bot.send_message(chat_id, f'⏳ Таймер набора игроков запущен: {mins}:{secs:02d}. Нужны минимум {MIN_PLAYERS} игрока.', message_thread_id=thread_id)
            last_announced_min = mins
        except Exception:
            logger.exception('Failed to announce initial lobby timer for %s', game_id)

        # initial prev_count
        try:
            players = await list_players(game_id)
            prev_count = len(players)
        except Exception:
            prev_count = 0

        while True:
            # if game state changed, stop
            game = await get_game_by_id(game_id)
            if not game or game.get('state') != 'lobby':
                break

            # fetch current players
            players = await list_players(game_id)
            cnt = len(players)

            if phase == 'initial' and cnt >= MIN_PLAYERS:
                phase = 'short'
                # set short deadline
                deadline = asyncio.get_event_loop().time() + LOBBY_SHORT
                try:
                    await set_game_phase_deadline(game_id, int(time.time()) + LOBBY_SHORT)
                except Exception:
                    logger.exception('Failed to persist short lobby deadline for %s', game_id)
                one_min_notified = False
                last_announced_min = LOBBY_SHORT // 60
                try:
                    await bot.send_message(chat_id, f'✅ Набрано минимум игроков ({cnt}). Таймер запуска игры — {_ru_plural(LOBBY_SHORT // 60, ('минута', 'минуты', 'минут'))}.', message_thread_id=thread_id)
                except Exception:
                    logger.exception('Failed to announce short lobby timer for %s', game_id)
                prev_count = cnt

            elif phase == 'short':
                # detect joins
                if cnt > prev_count:
                    # extend deadline
                    # compute remaining in epoch seconds
                    try:
                        cur_db = await get_game_by_id(game_id)
                        cur_deadline = cur_db.get('phase_deadline') if cur_db else None
                        if cur_deadline:
                            new_deadline_ts = int(cur_deadline) + LOBBY_JOIN_EXTENSION
                        else:
                            new_deadline_ts = int(time.time()) + LOBBY_JOIN_EXTENSION
                        await set_game_phase_deadline(game_id, new_deadline_ts)
                        # update local loop deadline
                        deadline = asyncio.get_event_loop().time() + (new_deadline_ts - int(time.time()))
                    except Exception:
                        logger.exception('Failed to extend/persist lobby deadline for %s', game_id)
                    try:
                        await bot.send_message(chat_id, f'➕ Игрок присоединился — +{LOBBY_JOIN_EXTENSION} сек к таймеру.', message_thread_id=thread_id)
                    except Exception:
                        logger.exception('Failed to announce join extension for %s', game_id)
                    prev_count = cnt

                remaining = int(deadline - asyncio.get_event_loop().time())
                if remaining <= 60 and not one_min_notified:
                    try:
                        await bot.send_message(chat_id, '⏱ Осталась 1 минута до начала игры.', message_thread_id=thread_id)
                    except Exception:
                        logger.exception('Failed to announce one-minute warning for %s', game_id)
                    one_min_notified = True
                    last_announced_min = remaining // 60  # 1 when 60s, 0 when <60

            # periodic minute announcements (only in short phase — game is starting)
            if phase == 'short':
                try:
                    rem = int(deadline - asyncio.get_event_loop().time())
                    if rem > 0:
                        mins = rem // 60
                        secs = rem % 60
                        if mins != last_announced_min:
                            # don't repeat the sub-60 warning if already sent
                            if mins == 0 and one_min_notified:
                                last_announced_min = mins
                            else:
                                try:
                                    if mins > 0:
                                        verb = 'Осталась' if mins == 1 else 'Осталось'
                                        await bot.send_message(chat_id, f'⏳ {verb} {_ru_plural(mins, ("минута", "минуты", "минут"))} до старта игры.', message_thread_id=thread_id)
                                    elif not one_min_notified:
                                        verb = 'Осталась' if secs == 1 else 'Осталось'
                                        await bot.send_message(chat_id, f'⏳ {verb} {_ru_plural(secs, ("секунда", "секунды", "секунд"))} до старта игры.', message_thread_id=thread_id)
                                except Exception:
                                    logger.exception('Failed to send periodic lobby timer update for %s', game_id)
                                last_announced_min = mins
                except Exception:
                    logger.exception('Failed to compute/send periodic lobby timer update for %s', game_id)

            # check deadline
            now = asyncio.get_event_loop().time()
            if now >= deadline:
                # check players count
                players = await list_players(game_id)
                cnt = len(players)
                if cnt >= MIN_PLAYERS:
                    # start game atomically
                    try:
                        started = await try_start_game(game_id)
                        if not started:
                            # another process/thread already started the game
                            break
                        await set_game_started(game_id)
                        await bot.send_message(chat_id, '🚀 Время истекло — начинается игра!', message_thread_id=thread_id)
                        # clear persisted deadline
                        try:
                            await set_game_phase_deadline(game_id, None)
                        except Exception:
                            logger.exception('Failed to clear phase_deadline for %s', game_id)
                        # remove timer entry
                        if game_id in LOBBY_TASKS:
                            try:
                                del LOBBY_TASKS[game_id]
                            except Exception:
                                pass
                        await distribute_roles_and_notify(game_id, chat_id, thread_id, bot, cnt)
                        try:
                            g_check = await get_game_by_id(game_id)
                            if g_check and g_check.get('crazy_mode'):
                                await bot.send_message(chat_id, '🌀 <b>Безумный режим!</b> Роли будут перемешаны после каждого дня.', message_thread_id=thread_id, parse_mode='HTML')
                        except Exception:
                            pass
                        asyncio.create_task(run_game_loop(game_id, chat_id, thread_id, bot))
                    except Exception:
                        logger.exception('Failed to start game from lobby countdown for %s', game_id)
                else:
                    try:
                        await bot.send_message(chat_id, '⛔ Минимальное количество игроков не набрано. Лобби закрыто.')
                        await update_game_state(game_id, 'finished')
                        # clear persisted deadline
                        try:
                            await set_game_phase_deadline(game_id, None)
                        except Exception:
                            logger.exception('Failed to clear phase_deadline on lobby close for %s', game_id)
                    except Exception:
                        logger.exception('Failed to handle lobby timeout (not enough players) for %s', game_id)
                break

            await asyncio.sleep(2)
    except asyncio.CancelledError:
        # expected on cancellation
        return
    except Exception:
        logger.exception('Lobby countdown error for %s', game_id)
    finally:
        # cleanup
        if game_id in LOBBY_TASKS:
            try:
                del LOBBY_TASKS[game_id]
            except Exception:
                pass

async def _build_lobby_text(game_id: int) -> str:
    """Build the lobby message text including players list and crazy mode indicator."""
    players = await list_players(game_id)
    header = (f'🎮 <b>Лобби</b> — <b>{_ru_plural(len(players), ("игрок", "игрока", "игроков"))}/{MAX_PLAYERS}</b>\n\n'
              f'👥 <b>Игроки:</b>\n')
    entries = []
    for i, p in enumerate(players):
        disp = p.get('username') or str(p.get('tg_id'))
        entries.append(f'{i+1}. <a href="tg://user?id={p.get("tg_id")}">{disp}</a>')
    text = header + '\n'.join(entries)
    try:
        game = await get_game_by_id(game_id)
        if game and game.get('crazy_mode'):
            text += '\n\n🌀 <b>Безумный режим</b> — роли будут перемешиваться каждый ход!'
    except Exception:
        pass
    text += '\n\n➡️ Нажмите кнопку, чтобы присоединиться к лобби.'
    return text


_ROLE_META_CACHE: dict[str, dict] | None = None

def _role_meta_by_name(name: str):
    global _ROLE_META_CACHE
    if _ROLE_META_CACHE is None:
        _ROLE_META_CACHE = {}
        for r in roles_module.get_all_roles():
            _ROLE_META_CACHE[r['name']] = r
    return _ROLE_META_CACHE.get(name)


async def distribute_roles_and_notify(game_id: int, chat_id: int, thread_id: int, bot: Bot, n_players: int = None):
    """Generate roles, assign to players, DM role cards, notify mafia/mason groups, send composition card to thread."""
    players = await list_players(game_id)
    if n_players is None:
        n_players = len(players)
    player_db_ids = [p['user_id'] for p in players]
    player_map = {p['user_id']: p for p in players}
    role_names = roles_module.generate_roles(n_players)
    assignments = roles_module.assign_roles_to_players(player_db_ids, role_names)

    # apply active_role items — re-roll Vanilla Town for item holders (99% chance)
    # Detective/Doctor excluded (already guaranteed by generation).
    # Priority: town actives → neutrals. Never mafia.
    all_roles = roles_module.get_all_roles()
    current_counts = {name: 0 for name in [r['name'] for r in all_roles]}
    for rn in assignments.values():
        current_counts[rn] = current_counts.get(rn, 0) + 1

    _excluded = {'Vanilla Town', 'Detective', 'Doctor', 'Mafia', 'Godfather', 'Consigliere', 'Framer', 'Blackmailer'}
    primary_pool = [
        r for r in all_roles
        if r['team'] == 'town' and r['name'] not in _excluded
    ]
    fallback_pool = [
        r for r in all_roles
        if r['team'] == 'neutral'
    ]

    for dbid, role in list(assignments.items()):
        if role != 'Vanilla Town':
            continue
        if await get_user_item_count(dbid, 'active_role') < 1:
            continue
        available = [
            r['name'] for r in primary_pool
            if r['min_players'] <= n_players
            and current_counts.get(r['name'], 0) < r.get('max_instances', 999)
        ]
        if not available:
            available = [
                r['name'] for r in fallback_pool
                if r['min_players'] <= n_players
                and current_counts.get(r['name'], 0) < r.get('max_instances', 999)
            ]
        if not available:
            continue
        if random.random() < 0.99:
            await use_user_item(dbid, 'active_role')
            new_role = random.choice(available)
            assignments[dbid] = new_role
            current_counts[new_role] = current_counts.get(new_role, 0) + 1
            logger.info('Active role item consumed for user %s: Vanilla Town -> %s', dbid, new_role)

    bot_username = None
    try:
        me = await bot.get_me()
        bot_username = getattr(me, 'username', None)
    except Exception:
        logger.exception('Failed to fetch bot info')

    unreachable = []

    async def _send_role_card(actor_db_id, role_name):
        await set_player_role(game_id, actor_db_id, role_name)
        meta = _role_meta_by_name(role_name)
        display = meta.get('display_ru', role_name) if meta else role_name
        desc = meta.get('description', '') if meta else ''
        tg_id = player_map[actor_db_id]['tg_id']
        icon = meta.get('icon', '') if meta else ''
        team = meta.get('team', 'town') if meta else 'town'
        team_label = 'Мафия' if team == 'mafia' else 'Нейтрал' if team == 'neutral' else 'Город'
        text = (f'{icon} <b>Ваша роль: {display}</b>\n'
                f'<b>{team_label}</b>\n\n'
                f'{desc}\n\n'
                f'🔐 <i>Держите роль в секрете.</i>\n'
                f'⏳ <i>Ночные действия придут отдельным сообщением.</i>')
        try:
            await bot.send_message(tg_id, text, parse_mode='HTML')
        except TelegramForbiddenError:
            logger.error('Failed to send DM to user %s: Telegram forbidden', tg_id)
            unreachable.append(actor_db_id)
        except Exception as e:
            logger.exception('Failed to send DM to user %s: %s', tg_id, e)

    await asyncio.gather(*[_send_role_card(aid, rn) for aid, rn in assignments.items()], return_exceptions=True)

    # notify mafia team members of each other and masons
    try:
        mafia_members = []
        mason_members = []
        for dbid, role in assignments.items():
            meta = _role_meta_by_name(role) or {}
            team = meta.get('team')
            if team == 'mafia':
                user = player_map.get(dbid)
                if user:
                    mafia_members.append(user)
            if role == 'Mason':
                user = player_map.get(dbid)
                if user:
                    mason_members.append(user)

        if mafia_members:
            lines = []
            for m in mafia_members:
                disp = m.get('username') or str(m.get('tg_id'))
                lines.append(f'<a href="tg://user?id={m.get("tg_id")}">{disp}</a>')
            mafia_text = '📣 Ваша команда (мафия):\n' + '\n'.join(lines)
            await asyncio.gather(*[
                bot.send_message(m.get('tg_id'), mafia_text, parse_mode='HTML')
                for m in mafia_members
            ], return_exceptions=True)

        if len(mason_members) > 1:
            lines = []
            for m in mason_members:
                disp = m.get('username') or str(m.get('tg_id'))
                lines.append(f'<a href="tg://user?id={m.get("tg_id")}">{disp}</a>')
            mason_text = '🧭 Вы — масоны. Другие масоны:\n' + '\n'.join(lines)
            await asyncio.gather(*[
                bot.send_message(m.get('tg_id'), mason_text, parse_mode='HTML')
                for m in mason_members
            ], return_exceptions=True)
    except Exception:
        logger.exception('Failed to notify mafia/mason groups')

    # send role composition card to the thread
    try:
        role_counts = {}
        for dbid, rname in assignments.items():
            role_counts[rname] = role_counts.get(rname, 0) + 1
        team_labels = {'mafia': 'Мафия', 'town': 'Город', 'neutral': 'Нейтралы'}
        team_order = ['mafia', 'town', 'neutral']
        team_count_labels = {'mafia': 'маф', 'town': 'мирных', 'neutral': 'нейтралов'}
        lines = [f'🎮 <b>Состав игры ({_ru_plural(n_players, ("игрок", "игрока", "игроков"))})</b>']
        lines.append(f'{"─" * 24}')
        for team in team_order:
            team_lines = []
            total = 0
            for rname, cnt in sorted(role_counts.items()):
                meta = _role_meta_by_name(rname) or {}
                if meta.get('team') == team:
                    icon = meta.get('icon', '❓')
                    display = meta.get('display_ru', rname)
                    if rname == 'Vanilla Town':
                        display = 'Мирный житель'
                    team_lines.append(f'  {icon} {display} ×{cnt}')
                    total += cnt
            if team_lines:
                lines.append(f'\n<b>{team_labels[team]} ({total} {team_count_labels[team]})</b>')
                lines.extend(team_lines)
        role_card = '\n'.join(lines)
        await bot.send_message(chat_id, role_card, message_thread_id=thread_id, parse_mode='HTML')
    except Exception:
        logger.exception('Failed to send role composition card')

    # notify thread about unreachable players
    if unreachable and bot_username:
        names = []
        for uid in unreachable:
            u = await get_user_by_dbid(uid)
            names.append(u.get('username') or str(u.get('tg_id')))
        text = 'Следующие игроки не открыли диалог с ботом. Пожалуйста, откройте личный чат с ботом и нажмите "Start":\n' + ', '.join(names)
        url = f'https://t.me/{bot_username}?start=game_{game_id}'
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text='Открыть чат с ботом и начать', url=url))
        kb = builder.as_markup()
        try:
            await bot.send_message(chat_id, text, reply_markup=kb, message_thread_id=thread_id)
        except Exception:
            logger.exception('Failed to send start-chat prompt in thread %s:%s', chat_id, thread_id)

    return assignments


async def _shuffle_roles_crazy(game_id: int, chat_id: int, thread_id: int, bot: Bot):
    """Shuffle roles among alive players (crazy/безумный режим)."""
    players = await list_players(game_id)
    alive = [p for p in players if p['alive'] == 1]
    if len(alive) < 2:
        return
    alive_roles = [p['role'] for p in alive]
    random.shuffle(alive_roles)
    for p, new_role in zip(alive, alive_roles):
        await set_player_role(game_id, p['user_id'], new_role)
        p['role'] = new_role  # update in-memory dict for subsequent use
    await clear_player_flags(game_id)
    await clear_night_actions(game_id)
    await clear_doctor_last_heal(game_id)

    async def _send_new_card(p):
        meta = _role_meta_by_name(p['role']) or {}
        display = meta.get('display_ru', p['role'])
        desc = meta.get('description', '')
        icon = meta.get('icon', '')
        team = meta.get('team', 'town')
        team_label = 'Мафия' if team == 'mafia' else 'Нейтрал' if team == 'neutral' else 'Город'
        text = (f'{icon} <b>Ваша новая роль: {display}</b>\n'
                f'<b>{team_label}</b>\n\n'
                f'{desc}\n\n'
                f'🌀 <i>Безумный режим — роли перемешаны! Ваша роль изменилась.</i>')
        try:
            await bot.send_message(p['tg_id'], text, parse_mode='HTML')
        except Exception:
            pass

    await asyncio.gather(*[_send_new_card(p) for p in alive], return_exceptions=True)

    mafia_members = []
    mason_members = []
    for p in alive:
        meta = _role_meta_by_name(p['role']) or {}
        team = meta.get('team')
        if team == 'mafia':
            mafia_members.append(p)
        if p['role'] == 'Mason':
            mason_members.append(p)

    if mafia_members:
        lines = []
        for m in mafia_members:
            disp = m.get('username') or str(m.get('tg_id'))
            lines.append(f'<a href="tg://user?id={m.get("tg_id")}">{disp}</a>')
        mafia_text = '📣 <b>Роли перемешаны!</b> Ваша новая команда (мафия):\n' + '\n'.join(lines)
        await asyncio.gather(*[
            bot.send_message(m['tg_id'], mafia_text, parse_mode='HTML')
            for m in mafia_members
        ], return_exceptions=True)

    if len(mason_members) > 1:
        lines = []
        for m in mason_members:
            disp = m.get('username') or str(m.get('tg_id'))
            lines.append(f'<a href="tg://user?id={m.get("tg_id")}">{disp}</a>')
        mason_text = '🧭 <b>Роли перемешаны!</b> Ваши новые собратья-масоны:\n' + '\n'.join(lines)
        await asyncio.gather(*[
            bot.send_message(m['tg_id'], mason_text, parse_mode='HTML')
            for m in mason_members
        ], return_exceptions=True)

    try:
        await bot.send_message(chat_id, '🌀 <b>Безумный режим!</b> Роли среди живых игроков перемешаны.\nПроверьте личные сообщения — пришла новая роль.', message_thread_id=thread_id, parse_mode='HTML')
    except Exception:
        pass


async def _unlock_ach(user_id: int, name: str, b: Bot):
    tier = await add_progress(user_id, name, 1)
    if tier:
        meta = next((a for a in ACHIEVEMENTS if a['name'] == name), None)
        if meta:
            coins = meta['coins'][tier - 1]
            te = TIER_EMOJIS[tier]
            cu = await get_user_by_dbid(user_id)
            if cu:
                try:
                    await b.send_message(cu['tg_id'],
                        f'🏅 <b>Достижение улучшено!</b>\n{te} {meta["display"]} (ур. {tier})\n{meta["desc"]}\n🎁 +{coins} 🪙',
                        parse_mode='HTML')
                    await add_coins(user_id, coins, f'Ачивка: {meta["display"]} ур.{tier}')
                except Exception:
                    pass


async def run_game_loop(game_id: int, chat_id: int, thread_id: int, bot: Bot):
    """Run simple game loop: night -> discussion -> voting -> repeat until win"""
    round_no = 1

    async def _check_win() -> bool:
        players_all = await list_players(game_id)
        maf_count = 0
        town_count = 0
        survivor_alive = False
        winning_team = None
        for p in players_all:
            if p['alive'] == 0:
                continue
            role = p.get('role') or ''
            meta = _role_meta_by_name(role)
            team = meta['team'] if meta else 'town'
            if team == 'mafia':
                maf_count += 1
            elif team == 'town':
                town_count += 1
            if role == 'Survivor':
                survivor_alive = True

        win = None
        extra_wins = []

        # Gray Cardinal check
        gray_alive_unrevealed = False
        for p in players_all:
            if p['alive'] == 0:
                continue
            role = p.get('role') or ''
            if role == 'GrayCardinal':
                revealed = await get_role_revealed(game_id, p['user_id'])
                if not revealed:
                    gray_alive_unrevealed = True

        survivor_icon = '🏃'
        gray_icon = '🃏'
        if maf_count == 0 and town_count == 0:
            win = '🏆 <b>Нейтралы побеждают!</b>'
            winning_team = 'neutral'
            if survivor_alive:
                extra_wins.append(f'{survivor_icon} <b>Выживший</b> остался в живых и побеждает!')
            if gray_alive_unrevealed:
                extra_wins.append(f'{gray_icon} <b>Серый кардинал</b> остался инкогнито и побеждает!')
        elif maf_count == 0:
            win = '🏆 <b>Город побеждает!</b>'
            winning_team = 'town'
            if survivor_alive:
                extra_wins.append(f'{survivor_icon} <b>Выживший</b> остался в живых и побеждает вместе с городом!')
            if gray_alive_unrevealed:
                extra_wins.append(f'{gray_icon} <b>Серый кардинал</b> остался инкогнито и побеждает вместе с городом!')
        elif maf_count >= town_count and maf_count > 0:
            win = '🏆 <b>Мафия побеждает!</b>'
            winning_team = 'mafia'
            if survivor_alive:
                extra_wins.append(f'{survivor_icon} <b>Выживший</b> остался в живых и побеждает!')
            if gray_alive_unrevealed:
                extra_wins.append(f'{gray_icon} <b>Серый кардинал</b> остался инкогнито и побеждает!')

        if not win:
            return False

        try:
            player_lines = []
            alive_list = []
            dead_list = []
            for p in players_all:
                role = p.get('role') or ''
                meta = _role_meta_by_name(role) or {}
                display = meta.get('display_ru', role)
                icon = meta.get('icon', '❓')
                name = p.get('username') or str(p.get('tg_id'))
                mention = f'<a href="tg://user?id={p.get("tg_id")}">{name}</a>'
                status = '✅' if p['alive'] == 1 else '💀'
                player_lines.append(f'{icon} {display} — {mention} {status}')
                if p['alive'] == 1:
                    alive_list.append(mention)
                else:
                    dead_list.append(mention)

            summary_parts = [win]
            summary_parts.append('\n<b>👥 Итоги игры:</b>')
            summary_parts.append('\n'.join(player_lines))

            if alive_list:
                summary_parts.append(f'\n🏅 <b>Выжившие:</b> ' + ', '.join(alive_list))
            if dead_list:
                summary_parts.append(f'💀 <b>Погибшие:</b> ' + ', '.join(dead_list))

            if extra_wins:
                summary_parts.append('\n' + '\n'.join(extra_wins))

            summary_text = '\n'.join(summary_parts)
            await bot.send_message(chat_id, summary_text, message_thread_id=thread_id, parse_mode='HTML')
            await update_game_state(game_id, 'finished')
            await reset_protection_used(game_id)
            await clear_item_usage(game_id)
            await clear_pending_poison_kills(game_id)
            await clear_masked_flags(game_id)

            # send DM with result + shop button to each player
            try:
                shop_builder = InlineKeyboardBuilder()
                shop_builder.add(InlineKeyboardButton(text='🪙 Открыть магазин', callback_data='open_shop'))
                shop_kb = shop_builder.as_markup()

                async def _send_game_result(p):
                    role = p.get('role') or ''
                    meta = _role_meta_by_name(role) or {}
                    team = meta.get('team', 'town')
                    is_winner = False
                    if team == winning_team:
                        is_winner = True
                    elif winning_team == 'town' and team == 'neutral' and p['alive'] == 1:
                        is_winner = True
                    elif winning_team == 'mafia' and team == 'neutral' and p['alive'] == 1:
                        is_winner = True
                    # Gray Cardinal: wins if alive and role not revealed
                    if role == 'GrayCardinal' and p['alive'] == 1:
                        revealed = await get_role_revealed(game_id, p['user_id'])
                        is_winner = not revealed
                    if is_winner:
                        msg = ('🏆 <b>Победа!</b>\n\n'
                               'Вы и ваша команда одержали победу в этой битве умов и хитрости. '
                               'Отличная игра, вы были на высоте!\n\n'
                               '🎁 Награда: <b>+10 монет</b> за победу.\n\n'
                               'Продолжайте в том же духе! 🎉')
                    else:
                        msg = ('💔 <b>Поражение…</b>\n\n'
                               'В этот раз удача была не на вашей стороне, но это не повод '
                               'расстраиваться. Каждая игра делает вас сильнее и опытнее!\n\n'
                               'В следующий раз обязательно получится! 💪')
                    try:
                        await bot.send_message(p['tg_id'], msg, reply_markup=shop_kb, parse_mode='HTML')
                    except Exception:
                        pass

                await asyncio.gather(*[_send_game_result(p) for p in players_all], return_exceptions=True)
            except Exception:
                logger.exception('Failed to send DM result for game %s', game_id)

            # award coins and win/loss, record role stats (batch, 1 transaction)
            started_at_str = await get_game_started(game_id)
            game_duration = 0
            if started_at_str:
                try:
                    started_dt = datetime.fromisoformat(started_at_str)
                    game_duration = int((datetime.now() - started_dt).total_seconds())
                except Exception:
                    pass

            batch_results = []
            for p in players_all:
                role = p.get('role') or ''
                meta = _role_meta_by_name(role) or {}
                team = meta.get('team', 'town')
                is_winner = False
                if team == winning_team:
                    is_winner = True
                elif winning_team == 'town' and team == 'neutral' and p['alive'] == 1:
                    is_winner = True
                elif winning_team == 'mafia' and team == 'neutral' and p['alive'] == 1:
                    is_winner = True
                # Gray Cardinal: wins if alive and role not revealed
                if role == 'GrayCardinal' and p['alive'] == 1:
                    revealed = await get_role_revealed(game_id, p['user_id'])
                    is_winner = not revealed
                batch_results.append({
                    'user_id': p['user_id'],
                    'coins': 10,
                    'won': is_winner,
                    'role_name': role,
                    'play_seconds': game_duration,
                    'reason': 'Победа в игре',
                })
            # save achievement tracking before cleanup
            _gf_kill = _game_vigilante_gf.pop(game_id, False)
            _sk_kills = _game_sk_kills.pop(game_id, 0)
            _game_doctor_saves.pop(game_id, None)
            _night_kills = _game_night_kills.pop(game_id, {})

            await batch_game_end(batch_results)

            # check achievements for each player
            async def _ach(user_id, name):
                await _unlock_ach(user_id, name, bot)
            for p in players_all:
                uid = p['user_id']
                role = p.get('role') or ''
                meta = _role_meta_by_name(role) or {}
                team = meta.get('team', 'town')
                won = next((r['won'] for r in batch_results if r['user_id'] == uid), False)
                alive = p['alive'] == 1
                if won and role == 'Jester':
                    await _ach(uid, 'jester_win')
                if alive and won and role == 'GrayCardinal':
                    revealed = await get_role_revealed(game_id, uid)
                    if not revealed:
                        await _ach(uid, 'gray_cardinal_win')
                if alive and won and role == 'Survivor':
                    await _ach(uid, 'survivor_win')
                # cumulative role wins
                if won:
                    rs = await get_role_stats(uid)
                    for rs_entry in rs:
                        if rs_entry['role_name'] == 'Mafia' and rs_entry['wins'] >= 1:
                            await _ach(uid, 'mafia_win_3')
                        if rs_entry['role_name'] == 'Vanilla Town' and rs_entry['wins'] >= 1:
                            await _ach(uid, 'town_win_5')
                # vigilante killed Godfather
                if _gf_kill and role == 'Vigilante' and won:
                    await _ach(uid, 'vigilante_godfather')
                # serial killer 3+ kills
                if _sk_kills >= 3 and role == 'SerialKiller':
                    await _ach(uid, 'serial_killer_3')
                # doctor saved 3+ different targets in one game
                if role == 'Doctor':
                    saved_set = _game_doctor_saves.get(game_id, {}).get(uid, set())
                    if len(saved_set) >= 3:
                        await _ach(uid, 'doctor_3_saves')
                # first win ever (progress = total wins)
                if won:
                    await _ach(uid, 'first_win')
                # triple kill in one night
                if _night_kills.get(uid, 0) >= 3:
                    await _ach(uid, 'triple_kill_night')
                # cumulative: games played (progress = total games)
                rs = await get_role_stats(uid)
                total_games = sum(r['games'] for r in rs)
                if total_games >= 1:
                    await _ach(uid, 'game_50')
                # own 5+ distinct items
                distinct_items = await get_distinct_item_count(uid)
                if distinct_items >= 5:
                    await _ach(uid, 'item_collector_5')
                # first_lynch — handled inline in lynch section
        except Exception:
            logger.exception('Failed to announce win for %s:%s', chat_id, thread_id)
        return True

    while True:
        logger.info('Starting night %s for game %s', round_no, game_id)
        # set phase to night
        try:
            await update_game_state(game_id, 'night')
        except Exception:
            logger.exception('Failed to set game phase to night')

        # Process pending slow poison kills from previous night
        try:
            poison_targets = await get_pending_poison_kills(game_id)
            if poison_targets:
                for pt_uid in poison_targets:
                    pt_user = await get_user_by_dbid(pt_uid)
                    pt_alive = any(p['user_id'] == pt_uid and p.get('alive') == 1 for p in await list_players(game_id))
                    if pt_alive:
                        await set_player_alive(game_id, pt_uid, False)
                        pt_name = pt_user.get('username') or str(pt_user.get('tg_id'))
                        try:
                            await bot.send_message(chat_id, f'☠️ <b>Яд медленного действия:</b> {pt_name} погиб(ла) от яда.', message_thread_id=thread_id, parse_mode='HTML')
                        except Exception:
                            logger.exception('Failed to announce slow poison kill')
                await clear_pending_poison_kills(game_id)
            # Check win condition after poison kills
            if await _check_win():
                return
        except Exception:
            logger.exception('Failed to process slow poison kills for game %s', game_id)

        # Tick carnival mask counters (each night decrements by 1, expires after 2 nights)
        try:
            await tick_masked_flags(game_id)
        except Exception:
            logger.exception('Failed to tick masked flags for game %s', game_id)

        # notify thread with rich night message
        try:
            alive_players_for_msg = await get_alive_players(game_id)
            alive_names = []
            for i, ap in enumerate(alive_players_for_msg, 1):
                if await get_player_masked(game_id, ap['user_id']):
                    aname = '❓ Неизвестный'
                else:
                    aname = ap.get('username') or str(ap.get('tg_id'))
                alive_names.append(f'{i}. {aname}')
            alive_list_str = '\n'.join(alive_names)
            night_duration_min = NIGHT_DURATION // 60
            night_duration_sec = NIGHT_DURATION % 60
            if night_duration_min > 0 and night_duration_sec > 0:
                timer_str = f'{_ru_plural(night_duration_min, ("минута", "минуты", "минут"))} {_ru_plural(night_duration_sec, ("секунда", "секунды", "секунд"))}'
            elif night_duration_min > 0:
                timer_str = _ru_plural(night_duration_min, ('минута', 'минуты', 'минут'))
            else:
                timer_str = _ru_plural(night_duration_sec, ('секунда', 'секунды', 'секунд'))

            msg_text = (f'🌃 <b>Наступает ночь {round_no}</b>\n\n'
                        f'На улицы города выходят лишь самые отважные и бесстрашные. Утром попробуем сосчитать их головы...\n\n'
                        f'👥 <b>Живые игроки:</b>\n'
                        f'{alive_list_str}\n\n'
                        f'⏳ Спать осталось {timer_str}.')

            me = await bot.get_me()
            bu = getattr(me, 'username', None)
            builder = InlineKeyboardBuilder()
            if bu:
                url = f'https://t.me/{bu}'
                builder.add(InlineKeyboardButton(text='🌙 Перейти к боту', url=url))
                kb = builder.as_markup()
                await bot.send_message(chat_id, msg_text, message_thread_id=thread_id, reply_markup=kb, parse_mode='HTML')
            else:
                await bot.send_message(chat_id, msg_text, message_thread_id=thread_id, parse_mode='HTML')
        except Exception:
            logger.exception('Failed to send night start to thread %s:%s', chat_id, thread_id)

        # send action prompts to active roles in DM (parallel)
        try:
            players = await get_players_for_game(game_id)
            actionable = ('mafia','Detective','Doctor','Bodyguard','Vigilante','Tracker','Roleblocker','Consigliere','Framer','Blackmailer','SerialKiller')

            async def _send_night_prompt(p):
                role = p.get('role') or ''
                meta = _role_meta_by_name(role)
                team = meta['team'] if meta else None
                if team != 'mafia' and role not in actionable:
                    return None
                if p.get('alive') == 0:
                    return None
                tg = p.get('tg_id')
                doctor_exclude = None
                if role == 'Doctor':
                    doctor_exclude = await get_doctor_last_heal(game_id, p['user_id'])
                kb_list = []
                for t in players:
                    if t.get('alive') == 0:
                        continue
                    if role != 'Doctor' and t['user_id'] == p['user_id']:
                        continue
                    if t['user_id'] == doctor_exclude:
                        continue
                    label = t.get('username') or f'Player {t["user_id"]}'
                    kb_list.append([InlineKeyboardButton(text=label, callback_data=f'na:{game_id}:{p["user_id"]}:{t["user_id"]}')])
                kb_list.append([InlineKeyboardButton(text='Пропустить', callback_data=f'na:{game_id}:{p["user_id"]}:0')])
                kb = InlineKeyboardMarkup(inline_keyboard=kb_list)
                try:
                    await bot.send_message(tg, f'🌙 Ночь {round_no}: выберите цель для роли <b>{meta.get("display_ru") if meta else role}</b>.', reply_markup=kb, parse_mode='HTML')
                except TelegramForbiddenError:
                    return p['user_id']
                except Exception:
                    logger.exception('Failed to DM night prompt to %s', tg)
                # extra prompt for allseeing item (only if not used this game yet)
                if await get_user_item_count(p['user_id'], 'allseeing') > 0 and not await is_item_used(game_id, p['user_id'], 'allseeing'):
                    try:
                        as_list = []
                        for t in players:
                            if t['user_id'] == p['user_id'] or t.get('alive') == 0:
                                continue
                            label = t.get('username') or f'Player {t["user_id"]}'
                            as_list.append([InlineKeyboardButton(text=label, callback_data=f'na:{game_id}:{p["user_id"]}:{t["user_id"]}:allseeing')])
                        as_list.append([InlineKeyboardButton(text='Пропустить', callback_data=f'na:{game_id}:{p["user_id"]}:0:allseeing')])
                        as_kb = InlineKeyboardMarkup(inline_keyboard=as_list)
                        await bot.send_message(tg, '👁️ <b>Всевидящий:</b> выберите цель, чью роль хотите узнать.', reply_markup=as_kb, parse_mode='HTML')
                    except Exception:
                        logger.exception('Failed to DM allseeing prompt to %s', tg)
                # extra prompt for carnival mask (only if not used this game)
                if await get_user_item_count(p['user_id'], 'carnival_mask') > 0 and not await is_item_used(game_id, p['user_id'], 'carnival_mask'):
                    try:
                        cm_builder = InlineKeyboardBuilder()
                        cm_builder.add(InlineKeyboardButton(text='🎭 Надеть маску', callback_data=f'na:{game_id}:{p["user_id"]}:0:carnival_mask'))
                        await bot.send_message(tg, '🎭 <b>Карнавальная маска:</b> нажмите кнопку, чтобы скрыть своё имя в списке живых на один день.', reply_markup=cm_builder.as_markup(), parse_mode='HTML')
                    except Exception:
                        logger.exception('Failed to DM carnival mask prompt to %s', tg)
                return None

            results = await asyncio.gather(*[_send_night_prompt(p) for p in players], return_exceptions=True)
            unreachable = [r for r in results if r is not None and not isinstance(r, Exception)]
            if unreachable:
                names = []
                for uid in unreachable:
                    u = await get_user_by_dbid(uid)
                    names.append(u.get('username') or str(u.get('tg_id')))
                await bot.send_message(chat_id, '⚠️ Следующие игроки не открыли диалог с ботом и не получили ночные действия: ' + ', '.join(names), message_thread_id=thread_id)
        except Exception:
            logger.exception('Failed to send night prompts')

        # wait for night duration but allow early exit if all active roles acted
        # compute expected actors (alive players with actionable roles)
        players_for_game = await get_players_for_game(game_id)
        actionable = ('mafia','Detective','Doctor','Bodyguard','Vigilante','Tracker','Roleblocker','Consigliere','Framer','Blackmailer','SerialKiller')
        expected_actors = set()
        for p in players_for_game:
            if p.get('alive') == 0:
                continue
            role = p.get('role') or ''
            meta = _role_meta_by_name(role)
            team = meta['team'] if meta else None
            if team == 'mafia' or role in actionable:
                expected_actors.add(p['user_id'])

        start_ts = asyncio.get_event_loop().time()
        deadline = start_ts + NIGHT_DURATION
        # poll for actions until deadline or until all expected acted (in-memory, no DB)
        while True:
            async with _night_actors_lock:
                acted = _night_actors.get(game_id, set())
            acted_expected = acted & expected_actors
            if expected_actors and acted_expected >= expected_actors:
                break
            now = asyncio.get_event_loop().time()
            if now >= deadline:
                break
            await asyncio.sleep(1)

        # clear in-memory tracker for next round
        async with _night_actors_lock:
            _night_actors.pop(game_id, None)

        # process night actions
        # clear previous night's flags (framed, blackmailed, disguised)
        try:
            await clear_player_flags(game_id)
        except Exception:
            logger.exception('Failed to clear player flags for game %s', game_id)
        # apply disguise items — set disguised flag for players who own one
        try:
            all_players = await list_players(game_id)
            for p in all_players:
                if p.get('alive') and await get_user_item_count(p['user_id'], 'disguise') > 0:
                    await set_player_disguised(game_id, p['user_id'], 1)
                    await use_user_item(p['user_id'], 'disguise')
        except Exception:
            logger.exception('Failed to apply disguise items for game %s', game_id)
        actions = await fetch_night_actions(game_id)
        await clear_night_actions(game_id)

        # compute blocked actors
        blocked = set()

        protected = set()
        _night_doctor_map: dict[int, int] = {}  # target -> doctor_uid (for achievement tracking)
        bodyguard_map = {}  # target -> bodyguard_actor
        mafia_votes = {}  # target -> {'count': int, 'voters': list of actor_user_id}
        other_kills = []  # list of (actor, target, role) for Vigilante/SerialKiller
        detective_actions = []
        consigliere_actions = []
        tracker_actions = []
        framer_actions = []
        blackmailer_actions = []
        allseeing_actions = []

        for a in actions:
            actor = a['actor_user_id']
            role = a['role']
            target = a['target_user_id']
            if target == 0:
                continue
            if role == '_allseeing':
                allseeing_actions.append((actor, target))
                continue
            if actor in blocked:
                logger.info('Actor %s roleblocked; skipping action', actor)
                continue
            if role == 'Roleblocker':
                blocked.add(target)
            elif role == 'Doctor':
                protected.add(target)
                _night_doctor_map[target] = actor  # for achievement tracking
                try:
                    await set_doctor_last_heal(game_id, actor, target)
                except Exception:
                    logger.exception('Failed to set doctor_last_heal for %s:%s', game_id, actor)
            elif role == 'Bodyguard':
                bodyguard_map[target] = actor
            elif role in ('Vigilante', 'SerialKiller'):
                other_kills.append((actor, target, role))
            elif role in ('Mafia', 'Godfather'):
                if target not in mafia_votes:
                    mafia_votes[target] = {'count': 0, 'voters': []}
                mafia_votes[target]['count'] += 1
                mafia_votes[target]['voters'].append(actor)
            elif role == 'Detective':
                detective_actions.append((actor, target))
            elif role == 'Consigliere':
                consigliere_actions.append((actor, target))
            elif role == 'Tracker':
                tracker_actions.append((actor, target))
            elif role == 'Framer':
                framer_actions.append((actor, target))
            elif role == 'Blackmailer':
                blackmailer_actions.append((actor, target))
            # other roles can be extended here

        # Determine Mafia kill target by majority vote
        mafia_kill_target = None
        if mafia_votes:
            max_votes = max(v['count'] for v in mafia_votes.values())
            top = [t for t, v in mafia_votes.items() if v['count'] == max_votes]
            if len(top) == 1:
                mafia_kill_target = top[0]
            else:
                # tie — Godfather breaks it if he voted for one of the tied
                gf_target = None
                for a in actions:
                    if a['role'] == 'Godfather' and a['actor_user_id'] not in blocked and a['target_user_id'] and a['target_user_id'] != 0:
                        gf_target = a['target_user_id']
                        break
                if gf_target in top:
                    mafia_kill_target = gf_target

        # Send Mafia vote summary to all alive Mafia faction members
        try:
            mafia_roles = {'Mafia', 'Godfather', 'Consigliere', 'Framer', 'Blackmailer'}
            mafia_alive = [p for p in players if p.get('alive') == 1 and mafia_roles.intersection({p.get('role', '')})]
            mafia_vote_lines = ['🗳️ <b>Голосование мафии:</b>']
            for tid, vote_info in mafia_votes.items():
                tu = await get_user_by_dbid(tid)
                tname = tu.get('username') or str(tu.get('tg_id')) if tu else str(tid)
                voter_names = []
                for vid in vote_info['voters']:
                    vu = await get_user_by_dbid(vid)
                    vname = vu.get('username') or str(vu.get('tg_id')) if vu else str(vid)
                    voter_names.append(vname)
                mafia_vote_lines.append(f'  • {tname} — {_ru_plural(vote_info["count"], ("голос", "голоса", "голосов"))}: {", ".join(voter_names)}')
            if mafia_kill_target:
                ktu = await get_user_by_dbid(mafia_kill_target)
                ktname = ktu.get('username') or str(ktu.get('tg_id')) if ktu else str(mafia_kill_target)
                mafia_vote_lines.append(f'\n🔪 <b>Решено:</b> убить {ktname}')
            else:
                mafia_vote_lines.append(f'\n✋ <b>Единого решения нет.</b> В эту ночь мафия никого не убила.')
            mafia_summary = '\n'.join(mafia_vote_lines)
            await asyncio.gather(*[
                bot.send_message(mp['tg_id'], mafia_summary, parse_mode='HTML')
                for mp in mafia_alive
            ], return_exceptions=True)
        except Exception:
            logger.exception('Failed to send mafia vote summary')

        deaths = set()
        doctor_saves = set()
        bodyguard_saves = {}
        bodyguard_died_set = set()
        protection_saves = set()

        async def _try_protection(target_db) -> bool:
            if await get_protection_used(game_id, target_db):
                return False
            if await use_user_item(target_db, 'protection'):
                await set_protection_used(game_id, target_db)
                protection_saves.add(target_db)
                return True
            return False

        # track who killed whom for slow poison
        killed_by: dict[int, int] = {}

        # resolve mafia kill
        if mafia_kill_target:
            if mafia_kill_target in protected:
                doctor_saves.add(mafia_kill_target)
                logger.info('Mafia target %s protected by Doctor; kill prevented', mafia_kill_target)
                doc_uid = _night_doctor_map.get(mafia_kill_target)
                if doc_uid:
                    _game_doctor_saves.setdefault(game_id, {}).setdefault(doc_uid, set()).add(mafia_kill_target)
            elif mafia_kill_target in bodyguard_map:
                bg_actor = bodyguard_map[mafia_kill_target]
                bodyguard_saves[mafia_kill_target] = bg_actor
                bodyguard_died_set.add(bg_actor)
                deaths.add(bg_actor)
                logger.info('Bodyguard %s died protecting mafia target %s', bg_actor, mafia_kill_target)
            elif await _try_protection(mafia_kill_target):
                logger.info('Mafia target %s saved by protection item', mafia_kill_target)
            else:
                deaths.add(mafia_kill_target)
                vi = mafia_votes.get(mafia_kill_target)
                if vi:
                    killed_by[mafia_kill_target] = random.choice(vi['voters'])

        # resolve other kills (Vigilante, SerialKiller)
        for actor, target, role in other_kills:
            if target in protected:
                doctor_saves.add(target)
                logger.info('Target %s protected by Doctor; kill prevented', target)
                doc_uid = _night_doctor_map.get(target)
                if doc_uid:
                    _game_doctor_saves.setdefault(game_id, {}).setdefault(doc_uid, set()).add(target)
                continue
            if target in bodyguard_map:
                bg_actor = bodyguard_map[target]
                bodyguard_saves[target] = bg_actor
                bodyguard_died_set.add(bg_actor)
                deaths.add(bg_actor)
                logger.info('Bodyguard %s died protecting %s', bg_actor, target)
                continue
            if await _try_protection(target):
                logger.info('Target %s saved by protection item', target)
                continue
            deaths.add(target)
            killed_by[target] = actor
            # achievement: vigilante kills Godfather
            if role == 'Vigilante':
                tgt_role_name = await get_player_role(game_id, target)
                if tgt_role_name == 'Godfather':
                    _game_vigilante_gf[game_id] = True
            # track SK kills for achievement
            if role == 'SerialKiller':
                _game_sk_kills[game_id] = _game_sk_kills.get(game_id, 0) + 1
            # track kills per actor per night for triple_kill_night
            _game_night_kills.setdefault(game_id, {})[actor] = _game_night_kills.get(game_id, {}).get(actor, 0) + 1

        # Двойник — перенаправить убийство на случайного живого
        try:
            all_alive = await list_players(game_id)
            all_alive = [p for p in all_alive if p.get('alive') == 1]
            double_redirects = {}
            for died_uid in list(deaths):
                if died_uid in bodyguard_died_set:
                    continue
                if (
                    await get_user_item_count(died_uid, 'double') > 0
                    and not await is_item_used(game_id, died_uid, 'double')
                ):
                    candidates = [p['user_id'] for p in all_alive if p['user_id'] != died_uid and p['user_id'] not in deaths]
                    if candidates:
                        replacement = random.choice(candidates)
                        await use_user_item(died_uid, 'double')
                        await mark_item_used(game_id, died_uid, 'double')
                        double_redirects[died_uid] = replacement
            for died_uid, replacement in double_redirects.items():
                deaths.discard(died_uid)
                deaths.add(replacement)
                try:
                    ou = await get_user_by_dbid(died_uid)
                    if ou:
                        await bot.send_message(ou.get('tg_id'), '🔄 <b>Двойник:</b> вместо вас умер другой человек!', parse_mode='HTML')
                except Exception:
                    logger.exception('Failed to DM double save for %s', died_uid)
                try:
                    ru = await get_user_by_dbid(replacement)
                    if ru:
                        await bot.send_message(ru.get('tg_id'), '💀 <b>Ночь:</b> вы стали жертвой Двойника вместо другого игрока.', parse_mode='HTML')
                except Exception:
                    logger.exception('Failed to DM double victim %s', replacement)
                # achievement: double save
                await _unlock_ach(died_uid, 'double_save', bot)
        except Exception:
            logger.exception('Failed to process double item for game %s', game_id)

        # Slow poison — record killer for next night if victim had the item
        try:
            for died_uid in list(deaths):
                if died_uid in bodyguard_died_set:
                    continue
                if (
                    await get_user_item_count(died_uid, 'slow_poison') > 0
                    and not await is_item_used(game_id, died_uid, 'slow_poison')
                    and died_uid in killed_by
                ):
                    await use_user_item(died_uid, 'slow_poison')
                    await mark_item_used(game_id, died_uid, 'slow_poison')
                    await add_pending_poison_kill(game_id, killed_by[died_uid])
                    logger.info('Slow poison activated: victim=%s killer=%s', died_uid, killed_by[died_uid])
                    await _unlock_ach(died_uid, 'poison_revenge', bot)
        except Exception:
            logger.exception('Failed to process slow poison for game %s', game_id)

        # apply deaths and notify dead players
        regular_dead_names = []
        bodyguard_died_names = []
        doctor_saved_names = []
        protection_saved_names = []
        bodyguard_saved_names = []
        for user_db_id in deaths:
            await set_player_alive(game_id, user_db_id, False)
            u = await get_user_by_dbid(user_db_id)
            name = u.get('username') or str(u.get('tg_id'))
            if user_db_id in bodyguard_died_set:
                bodyguard_died_names.append(name)
            else:
                regular_dead_names.append(name)
                try:
                    await bot.send_message(u.get('tg_id'), '💀 <b>Ночь:</b> вас убили. У вас есть 15 секунд, чтобы написать предсмертное сообщение. Просто напишите его в ответ на это сообщение.', parse_mode='HTML')
                    await create_last_word(game_id, user_db_id, u.get('tg_id'), chat_id, thread_id, name)
                    last_words_pending_tg_ids.add(u.get('tg_id'))
                except Exception:
                    logger.exception('Failed to DM death notice to %s', u.get('tg_id'))

        async def _send_doctor_save(uid):
            u = await get_user_by_dbid(uid)
            if not u:
                return
            doc_name = u.get('username') or str(u.get('tg_id'))
            doctor_saved_names.append(doc_name)
            try:
                await bot.send_message(u.get('tg_id'), '💉 <b>Ночь:</b> на вас было совершено нападение, но <b>Доктор</b> спас вас!', parse_mode='HTML')
            except Exception:
                logger.exception('Failed to DM doctor save notice to %s', u.get('tg_id'))

        async def _send_protection_save(uid):
            u = await get_user_by_dbid(uid)
            if not u:
                return
            p_name = u.get('username') or str(u.get('tg_id'))
            protection_saved_names.append(p_name)
            try:
                await bot.send_message(u.get('tg_id'), '🛡️ <b>Ночь:</b> на вас было совершено нападение, но ваша <b>Защита</b> сработала и спасла вас!', parse_mode='HTML')
            except Exception:
                logger.exception('Failed to DM protection save notice to %s', u.get('tg_id'))

        async def _send_bodyguard_notice(target_db, bg_db):
            u = await get_user_by_dbid(target_db)
            if not u:
                return
            target_name = u.get('username') or str(u.get('tg_id'))
            bodyguard_saved_names.append(target_name)
            bg_u = await get_user_by_dbid(bg_db)
            if not bg_u:
                return
            try:
                await bot.send_message(bg_u.get('tg_id'), f'💀 <b>Ночь:</b> вы пожертвовали собой, защищая {target_name}. Вы выбываете из игры.\n\nСпасибо за участие!', parse_mode='HTML')
            except Exception:
                logger.exception('Failed to DM bodyguard death notice to %s', bg_db)

        await asyncio.gather(*[_send_doctor_save(uid) for uid in doctor_saves], return_exceptions=True)
        await asyncio.gather(*[_send_protection_save(uid) for uid in protection_saves], return_exceptions=True)
        await asyncio.gather(*[_send_bodyguard_notice(t, b) for t, b in bodyguard_saves.items()], return_exceptions=True)

        # collect last words from killed players (15s max, forward immediately on response)
        game_pending_tg_ids = {tg_id for tg_id in last_words_pending_tg_ids}
        deadline_lw = asyncio.get_event_loop().time() + 15
        while asyncio.get_event_loop().time() < deadline_lw and game_pending_tg_ids:
            collected = await get_collected_last_words(game_id)
            for lw in collected:
                tg_id = lw['tg_id']
                if tg_id in game_pending_tg_ids:
                    try:
                        await bot.send_message(
                            chat_id,
                            f'📢 <b>Кто-то из жителей слышал, как {lw["name"]} кричал(а) перед смертью:</b>\n{lw["text"]}',
                            message_thread_id=thread_id,
                            parse_mode='HTML'
                        )
                    except Exception:
                        logger.exception('Failed to send last word for %s', tg_id)
                    await delete_last_word(lw['id'])
                    last_words_pending_tg_ids.discard(tg_id)
                    game_pending_tg_ids.discard(tg_id)
            await asyncio.sleep(0.5)
        # delete any leftover pending last words for this game
        leftover = await get_collected_last_words(game_id)
        for lw in leftover:
            await delete_last_word(lw['id'])
        for tg_id in list(last_words_pending_tg_ids):
            pass  # non-collected ones stay for next poll cycle

        # announce night results — detailed card
        night_lines = [f'🌙 <b>Итоги ночи</b>\n']
        if regular_dead_names:
            night_lines.append('💀 <b>Убиты:</b> ' + ', '.join(regular_dead_names))
        if bodyguard_died_names:
            for bg_name in bodyguard_died_names:
                night_lines.append(f'🛡️ <b>Телохранитель</b> {bg_name} погиб, защищая другого игрока.')
        if not regular_dead_names and not bodyguard_died_names:
            night_lines.append('💫 Ночью никого не убито.')

        if doctor_saved_names:
            night_lines.append('💉 <b>Спасены доктором:</b> ' + ', '.join(doctor_saved_names))
        if protection_saved_names:
            night_lines.append('🛡️ <b>Спасены защитой:</b> ' + ', '.join(protection_saved_names))
        if bodyguard_saved_names:
            night_lines.append('🛡️ <b>Спасены телохранителем:</b> ' + ', '.join(bodyguard_saved_names))

        try:
            await bot.send_message(chat_id, '\n'.join(night_lines), message_thread_id=thread_id, parse_mode='HTML')
        except Exception:
            logger.exception('Failed to send night results to thread %s:%s', chat_id, thread_id)

        # Process Framer actions — set framed flag
        for _framer_actor, framer_target in framer_actions:
            try:
                await set_player_framed(game_id, framer_target, 1)
            except Exception:
                logger.exception('Failed to set framed flag for player %s', framer_target)

        # Process Blackmailer actions — set blackmailed flag
        for _bm_actor, bm_target in blackmailer_actions:
            try:
                await set_player_blackmailed(game_id, bm_target, 1)
            except Exception:
                logger.exception('Failed to set blackmailed flag for player %s', bm_target)

        # Process Всевидящий — send role reveal to actor
        for as_actor, as_target in allseeing_actions:
            if as_actor in deaths:
                continue
            try:
                as_user = await get_user_by_dbid(as_actor)
                if not as_user:
                    continue
                tgt_user = await get_user_by_dbid(as_target)
                tgt_name = tgt_user.get('username') or str(tgt_user.get('tg_id')) if tgt_user else str(as_target)
                tgt_role = await get_player_role(game_id, as_target)
                meta = _role_meta_by_name(tgt_role) or {}
                role_display = meta.get('display_ru', tgt_role or 'Неизвестно')
                as_msg = f'👁️ <b>Всевидящий:</b>\n{tgt_name} — {role_display}.'
                await bot.send_message(as_user.get('tg_id'), as_msg, parse_mode='HTML')
                # mark target's role as revealed (Gray Cardinal check)
                await set_role_revealed(game_id, as_target)
            except Exception:
                logger.exception('Failed to send allseeing result %s:%s', game_id, as_actor)

        # Send Detective investigation results (only if investigator is still alive)
        for det_actor, det_target in detective_actions:
            if det_actor in deaths:
                continue
            try:
                det_user = await get_user_by_dbid(det_actor)
                if not det_user:
                    continue
                tgt_user = await get_user_by_dbid(det_target)
                tgt_name = tgt_user.get('username') or str(tgt_user.get('tg_id')) if tgt_user else str(det_target)
                tgt_role = await get_player_role(game_id, det_target)
                flags = await get_player_flags(game_id, det_target)
                has_docs = await get_user_item_count(det_target, 'documents') > 0
                if flags.get('framed'):
                    role_display = 'Мафия'
                elif has_docs:
                    role_display = 'Мирный'
                    await use_user_item(det_target, 'documents')
                elif tgt_role == 'Godfather':
                    role_display = 'Мирный'
                    await _unlock_ach(det_actor, 'detective_godfather', bot)
                else:
                    meta = _role_meta_by_name(tgt_role) or {}
                    if meta.get('team') == 'mafia':
                        role_display = 'Мафия'
                    else:
                        role_display = 'Мирный'
                det_msg = f'🔍 <b>Результат проверки:</b>\n{tgt_name} — {role_display}.'
                # if target was killed this night, Detective witnesses the killers
                if det_target in deaths:
                    killer_names = []
                    # mafia voters for this target
                    if det_target in mafia_votes:
                        for vid in mafia_votes[det_target]['voters']:
                            vu = await get_user_by_dbid(vid)
                            if vu:
                                killer_names.append(vu.get('username') or str(vu.get('tg_id')))
                    # other killers (Vigilante, SerialKiller)
                    for _ok_actor, ok_target, _ok_role in other_kills:
                        if ok_target == det_target:
                            vu = await get_user_by_dbid(_ok_actor)
                            if vu:
                                killer_names.append(vu.get('username') or str(vu.get('tg_id')))
                    if killer_names:
                        det_msg += '\n\n🔪 <b>На месте преступления замечены:</b>\n' + ', '.join(killer_names) + '.'
                await bot.send_message(det_user.get('tg_id'), det_msg, parse_mode='HTML')
            except Exception:
                logger.exception('Failed to send Detective result %s:%s', game_id, det_actor)

        # Send Consigliere investigation results (only if alive)
        for con_actor, con_target in consigliere_actions:
            if con_actor in deaths:
                continue
            try:
                con_user = await get_user_by_dbid(con_actor)
                if not con_user:
                    continue
                tgt_user = await get_user_by_dbid(con_target)
                tgt_name = tgt_user.get('username') or str(tgt_user.get('tg_id')) if tgt_user else str(con_target)
                tgt_role = await get_player_role(game_id, con_target)
                flags = await get_player_flags(game_id, con_target)
                if flags.get('disguised'):
                    role_display = '❓ Неизвестно'
                else:
                    meta = _role_meta_by_name(tgt_role) or {}
                    role_display = meta.get('display_ru', tgt_role or 'Неизвестно')
                con_msg = f'🔎 <b>Результат расследования:</b>\n{tgt_name} — {role_display}.'
                await bot.send_message(con_user.get('tg_id'), con_msg, parse_mode='HTML')
                # mark target's role as revealed (Gray Cardinal check) — only if not disguised
                if not flags.get('disguised'):
                    await set_role_revealed(game_id, con_target)
            except Exception:
                logger.exception('Failed to send Consigliere result %s:%s', game_id, con_actor)

        # Send Tracker results (only if alive)
        for tr_actor, tr_target in tracker_actions:
            if tr_actor in deaths:
                continue
            try:
                tr_user = await get_user_by_dbid(tr_actor)
                if not tr_user:
                    continue
                tgt_user = await get_user_by_dbid(tr_target)
                tgt_name = tgt_user.get('username') or str(tgt_user.get('tg_id')) if tgt_user else str(tr_target)
                flags = await get_player_flags(game_id, tr_target)
                if flags.get('disguised'):
                    tr_msg = f'🚶 <b>Результат слежки:</b>\n{tgt_name} — ❓ Неизвестно.'
                else:
                    visited = []
                    if tr_target not in blocked:
                        for night_a in actions:
                            if night_a['actor_user_id'] == tr_target and night_a['target_user_id'] and night_a['target_user_id'] != 0:
                                vu = await get_user_by_dbid(night_a['target_user_id'])
                                if vu:
                                    visited.append(vu.get('username') or str(vu.get('tg_id')))
                    if visited:
                        tr_msg = f'🚶 <b>Результат слежки:</b>\n{tgt_name} посещал(а): ' + ', '.join(visited) + '.'
                    else:
                        tr_msg = f'🚶 <b>Результат слежки:</b>\n{tgt_name} никого не посещал(а).'
                await bot.send_message(tr_user.get('tg_id'), tr_msg, parse_mode='HTML')
            except Exception:
                logger.exception('Failed to send Tracker result %s:%s', game_id, tr_actor)

        # --- win condition check ---

        # check win after night deaths
        if await _check_win():
            break

        # check if only 2 or fewer players remain — end immediately
        alive = await get_alive_players(game_id)
        if len(alive) <= 2:
            if not await _check_win():
                try:
                    await bot.send_message(chat_id, '⚡ Осталось слишком мало игроков. Игра завершается.', message_thread_id=thread_id)
                    await update_game_state(game_id, 'finished')
                except Exception:
                    logger.exception('Failed to send early end notice')
            break
        else:
            # short discussion
            try:
                await update_game_state(game_id, 'discussion')
            except Exception:
                logger.exception('Failed to set game phase to discussion')
            try:
                await bot.send_message(chat_id, f'💬 Обсуждение: {DISCUSSION_DURATION} секунд. Удачного обсуждения!', message_thread_id=thread_id)
            except Exception:
                logger.exception('Failed to send discussion notice for %s:%s', chat_id, thread_id)
            await asyncio.sleep(DISCUSSION_DURATION)

            # Voting phase
            # send voting DMs to alive players (parallel)
            async def _send_vote_dm(p):
                voter_db = p['user_id']
                tg = p['tg_id']
                voter_role = await get_player_role(game_id, voter_db)
                voter_team = None
                if voter_role:
                    vm = _role_meta_by_name(voter_role)
                    if vm:
                        voter_team = vm.get('team')
                builder = InlineKeyboardBuilder()
                kb_list = []
                for t in alive:
                    if t['user_id'] == voter_db:
                        continue
                    # mafia cannot vote against mafia
                    if voter_team == 'mafia':
                        t_role = await get_player_role(game_id, t['user_id'])
                        if t_role:
                            tm = _role_meta_by_name(t_role)
                            if tm and tm.get('team') == 'mafia':
                                continue
                    label = t.get('username') or f'Player {t["user_id"]}'
                    cb = f'vote:{game_id}:{voter_db}:{t["user_id"]}'
                    kb_list.append([InlineKeyboardButton(text=label, callback_data=cb)])
                kb_list.append([InlineKeyboardButton(text='Пропустить голосование', callback_data=f'vote:{game_id}:{voter_db}:0')])
                kb = InlineKeyboardMarkup(inline_keyboard=kb_list)
                try:
                    await bot.send_message(tg, '🗳️ <b>Голосование</b>\nВыберите, кого повесить. Ваш выбор отобразится в теме игры.', reply_markup=kb, parse_mode='HTML')
                except Exception:
                    logger.exception('Failed to send vote DM to %s', tg)

            await asyncio.gather(*[_send_vote_dm(p) for p in alive], return_exceptions=True)

            # set phase to voting
            try:
                await update_game_state(game_id, 'voting')
            except Exception:
                logger.exception('Failed to set game phase to voting')

            # notify thread about voting with button
            try:
                voting_text = (f'🗳️ <b>Пришло время определить и наказать виноватых.</b>\n\n'
                               f'Голосование продлится {VOTING_DURATION} секунд.')
                me = await bot.get_me()
                bu = getattr(me, 'username', None)
                builder = InlineKeyboardBuilder()
                if bu:
                    url = f'https://t.me/{bu}'
                    builder.add(InlineKeyboardButton(text='🗳 Голосовать', url=url))
                    kb = builder.as_markup()
                    await bot.send_message(chat_id, voting_text, message_thread_id=thread_id, reply_markup=kb, parse_mode='HTML')
                else:
                    await bot.send_message(chat_id, voting_text, message_thread_id=thread_id, parse_mode='HTML')
            except Exception:
                logger.exception('Failed to send voting notification for %s:%s', chat_id, thread_id)

            # wait for votes but allow early exit if all alive players voted
            alive = await get_alive_players(game_id)
            alive_count = len(alive)
            start_ts = asyncio.get_event_loop().time()
            deadline = start_ts + VOTING_DURATION
            while True:
                votes = await get_votes_for_game(game_id)
                voter_ids = {v['voter_user_id'] for v in votes}
                if alive_count > 0 and voter_ids >= {p['user_id'] for p in alive}:
                    break
                now = asyncio.get_event_loop().time()
                if now >= deadline:
                    break
                await asyncio.sleep(1)

            votes = await get_votes_for_game(game_id)
            await clear_votes(game_id)

            # tally votes
            tally = {}
            skip_count = 0
            for v in votes:
                tgt = v['target_user_id']
                if tgt == 0:
                    skip_count += 1
                    continue
                tally[tgt] = tally.get(tgt, 0) + 1
            if skip_count > 0 and max(tally.values(), default=0) < skip_count:
                lynch_text = 'Большинство пропустило голосование. Никого не повесили.'
                try:
                    await bot.send_message(chat_id, lynch_text, message_thread_id=thread_id)
                except Exception:
                    logger.exception('Failed to send lynch result for %s:%s', chat_id, thread_id)
            elif not tally:
                lynch_text = 'Голосов нет. Никого не повесили.'
                try:
                    await bot.send_message(chat_id, lynch_text, message_thread_id=thread_id)
                except Exception:
                    logger.exception('Failed to send lynch result for %s:%s', chat_id, thread_id)
            else:
                # find max
                max_votes = max(tally.values())
                top = [uid for uid, cnt in tally.items() if cnt == max_votes]
                if len(top) > 1:
                    lynch_text = 'Ничья голосов. Никого не повесили.'
                    try:
                        await bot.send_message(chat_id, lynch_text, message_thread_id=thread_id)
                    except Exception:
                        logger.exception('Failed to send lynch result for %s:%s', chat_id, thread_id)
                    continue
                else:
                    # candidate for lynch
                    lynched = top[0]
                    user = await get_user_by_dbid(lynched)
                    candidate_name = user.get('username') or str(user.get('tg_id'))

                    # switch to lynch decision phase
                    try:
                        await update_game_state(game_id, 'lynch')
                    except Exception:
                        logger.exception('Failed to set game phase to lynch')

                    # post decision message with like/dislike buttons and store message id for live updates
                    try:
                        builder = InlineKeyboardBuilder()
                        builder.add(InlineKeyboardButton(text='За (0)', callback_data=f'lynch:{game_id}:{lynched}:1'))
                        builder.add(InlineKeyboardButton(text='Против (0)', callback_data=f'lynch:{game_id}:{lynched}:0'))
                        kb = builder.as_markup()
                        candidate_mention = f'<a href="tg://user?id={user.get("tg_id")}">{candidate_name}</a>'
                        msg_text = (f'⚖️ <b>Предложение на повешение</b>\n\n'
                                    f'👤 {candidate_mention}\n\n'
                                    f'🕒 Голосование открыто — {LYNCH_DECISION_DURATION} сек.\n'
                                    f'📊 За: <b>0</b> • Против: <b>0</b>\n\n'
                                    f'Нажмите кнопку ниже, чтобы проголосовать.')
                        msg = await bot.send_message(chat_id, msg_text, reply_markup=kb, message_thread_id=thread_id, parse_mode='HTML')
                        try:
                            await set_lynch_message(game_id, lynched, chat_id, msg.message_id)
                        except Exception:
                            logger.exception('Failed to store lynch message id for game %s', game_id)
                    except Exception:
                        logger.exception('Failed to post lynch decision for %s:%s', chat_id, thread_id)
                        # fallback: nobody lynched
                        continue

                # wait for lynch decision duration but allow early exit when all alive players voted
                start_ts = asyncio.get_event_loop().time()
                deadline = start_ts + LYNCH_DECISION_DURATION
                while True:
                    counts_check = await get_lynch_vote_counts(game_id, lynched)
                    total_votes = sum(counts_check.values())
                    alive_players = await get_alive_players(game_id)
                    if alive_players and total_votes >= len(alive_players):
                        break
                    now = asyncio.get_event_loop().time()
                    if now >= deadline:
                        break
                    await asyncio.sleep(1)

                # remove buttons from the thread message so they are not clickable anymore
                try:
                    lm = await get_lynch_message(game_id, lynched)
                    if lm:
                        await bot.edit_message_reply_markup(chat_id=lm['chat_id'], message_id=lm['message_id'], reply_markup=None)
                        await clear_lynch_message(game_id, lynched)
                except Exception:
                    logger.exception('Failed to clear lynch decision buttons for %s:%s', chat_id, thread_id)

                # tally and apply result
                counts = await get_lynch_vote_counts(game_id, lynched)
                likes = counts.get(1, 0)
                dislikes = counts.get(0, 0)
                # fetch voter details for display
                try:
                    details = await get_lynch_votes_details(game_id, lynched)
                except Exception:
                    details = []
                like_mentions = []
                dislike_mentions = []
                for d in details:
                    try:
                        vrow = await get_user_by_dbid(d['voter_user_id'])
                        if not vrow:
                            continue
                        vname = vrow.get('username') or str(vrow.get('tg_id'))
                        vmention = f'<a href="tg://user?id={vrow.get("tg_id")}">{vname}</a>'
                        if int(d['choice']) == 1:
                            like_mentions.append(vmention)
                        else:
                            dislike_mentions.append(vmention)
                    except Exception:
                        continue
                # clear lynch votes for this candidate
                await clear_lynch_votes(game_id, lynched)

                # Алиби — автоматический +1 «Против», если владелец должен быть повешен
                alibi_saved = False
                if likes > dislikes:
                    if (
                        await get_user_item_count(lynched, 'alibi') > 0
                        and not await is_item_used(game_id, lynched, 'alibi')
                    ):
                        await use_user_item(lynched, 'alibi')
                        await mark_item_used(game_id, lynched, 'alibi')
                        dislikes += 1
                        if likes <= dislikes:
                            alibi_saved = True
                            try:
                                await bot.send_message(user.get('tg_id'), '🔗 <b>Алиби:</b> сработал автоматический голос «Против», и вас не повесили!', parse_mode='HTML')
                            except Exception:
                                pass

                if likes > dislikes:
                    # lynch succeeds
                    await set_player_alive(game_id, lynched, False)
                    # DM the lynched player
                    try:
                        urow = await get_user_by_dbid(lynched)
                        if urow:
                            await bot.send_message(urow.get('tg_id'), '💀 <b>Вы повешены.</b> Вы исключены из игры и не можете писать в игровой теме. Спасибо за участие.', parse_mode='HTML')
                    except Exception:
                        logger.exception('Failed to DM lynched player %s', lynched)
                    await _unlock_ach(lynched, 'first_lynch', bot)

                    # Отравленный кинжал — убить случайного проголосовавшего «За»
                    dagger_victim = None
                    dagger_survivor = None
                    try:
                        if (
                            await get_user_item_count(lynched, 'poisoned_dagger') > 0
                            and not await is_item_used(game_id, lynched, 'poisoned_dagger')
                        ):
                            await use_user_item(lynched, 'poisoned_dagger')
                            await mark_item_used(game_id, lynched, 'poisoned_dagger')
                            dagger_voters = [d for d in details if int(d['choice']) == 1 and d['voter_user_id'] != lynched]
                            if dagger_voters:
                                chosen = random.choice(dagger_voters)
                                dagger_victim = chosen['voter_user_id']
                                # check if victim has Антидот
                                if (
                                    await get_user_item_count(dagger_victim, 'antidote') > 0
                                    and not await is_item_used(game_id, dagger_victim, 'antidote')
                                ):
                                    await use_user_item(dagger_victim, 'antidote')
                                    await mark_item_used(game_id, dagger_victim, 'antidote')
                                    dagger_survivor = dagger_victim
                                    dagger_victim = None
                                    try:
                                        su = await get_user_by_dbid(dagger_survivor)
                                        if su:
                                            await bot.send_message(su.get('tg_id'), '💊 <b>Антидот:</b> вы пережили отравленный кинжал!', parse_mode='HTML')
                                    except Exception:
                                        pass
                                else:
                                    await set_player_alive(game_id, dagger_victim, False)
                                    try:
                                        vu = await get_user_by_dbid(dagger_victim)
                                        if vu:
                                            await bot.send_message(vu.get('tg_id'), '💀 <b>Отравленный кинжал:</b> вы голосовали за повешение и погибли от яда!', parse_mode='HTML')
                                    except Exception:
                                        pass
                    except Exception:
                        logger.exception('Failed to process poisoned dagger for %s', lynched)

                    # check if lynched player is Jester
                    jester_wins = False
                    try:
                        lynched_role = await get_player_role(game_id, lynched)
                        if lynched_role == 'Jester':
                            jester_wins = True
                    except Exception:
                        logger.exception('Failed to check Jester role for %s', lynched)

                    try:
                        candidate_mention = f'<a href="tg://user?id={user.get("tg_id")}">{candidate_name}</a>'
                        likes_text = ', '.join(like_mentions) if like_mentions else '—'
                        dislikes_text = ', '.join(dislike_mentions) if dislike_mentions else '—'
                        final_text = (f'🔨 <b>По результатам голосования</b>\n\n'
                                      f'{candidate_mention} — <b>повешен(а)</b>\n\n'
                                      f'📊 Результат: За: <b>{likes}</b> ({likes_text}) • Против: <b>{dislikes}</b> ({dislikes_text})')
                        if jester_wins:
                            final_text += (f'\n\n🎭 <b>Шут (Jester) добился своей цели!</b>\n'
                                            f'{candidate_mention} был(а) повешен(а) и одержал(а) победу!')
                        if dagger_victim:
                            try:
                                du = await get_user_by_dbid(dagger_victim)
                                dname = du.get('username') or str(du.get('tg_id')) if du else str(dagger_victim)
                                dmention = f'<a href="tg://user?id={du.get("tg_id")}">{dname}</a>' if du else dname
                                final_text += f'\n\n🔪 <b>Отравленный кинжал:</b> {dmention} также погибает от яда!'
                            except Exception:
                                pass
                        elif dagger_survivor:
                            try:
                                su = await get_user_by_dbid(dagger_survivor)
                                sname = su.get('username') or str(su.get('tg_id')) if su else str(dagger_survivor)
                                smention = f'<a href="tg://user?id={su.get("tg_id")}">{sname}</a>' if su else sname
                                final_text += f'\n\n💊 <b>Отравленный кинжал:</b> {smention} пережил(а) яд благодаря Антидоту!'
                            except Exception:
                                pass
                        await bot.send_message(chat_id, final_text, message_thread_id=thread_id, parse_mode='HTML')
                        # if jester, DM him his win
                        if jester_wins:
                            try:
                                await bot.send_message(user.get('tg_id'), '🎭 <b>Вы — Шут!</b>\nВас повесили, а значит вы добились своей цели. Поздравляем с победой!', parse_mode='HTML')
                            except Exception:
                                logger.exception('Failed to DM Jester win to %s', lynched)
                    except Exception:
                        logger.exception('Failed to announce lynch result for %s:%s', chat_id, thread_id)
                else:
                    try:
                        candidate_mention = f'<a href="tg://user?id={user.get("tg_id")}">{candidate_name}</a>'
                        likes_text = ', '.join(like_mentions) if like_mentions else '—'
                        dislikes_text = ', '.join(dislike_mentions) if dislike_mentions else '—'
                        final_text = (f'✋ <b>Повешение отменено</b>\n\n'
                                      f'{candidate_mention}\n\n'
                                      f'📊 За: <b>{likes}</b> ({likes_text}) • Против: <b>{dislikes}</b> ({dislikes_text})')
                        if alibi_saved:
                            final_text += '\n\n🔗 <b>Алиби:</b> автоматический голос «Против» спас кандидата от повешения!'
                        await bot.send_message(chat_id, final_text, message_thread_id=thread_id, parse_mode='HTML')
                    except Exception:
                        logger.exception('Failed to announce lynch canceled for %s:%s', chat_id, thread_id)

        # check win after lynch resolution (someone may have been killed)
        if await _check_win():
            break

        # crazy mode: shuffle roles among alive players after day phase
        try:
            g_check = await get_game_by_id(game_id)
            if g_check and g_check.get('crazy_mode'):
                await _shuffle_roles_crazy(game_id, chat_id, thread_id, bot)
        except Exception:
            logger.exception('Failed to shuffle roles (crazy mode) for %s', game_id)

        round_no += 1

    # cleanup per-game state when game ends
    try:
        await delete_last_words_by_game(game_id)
        await clear_item_usage(game_id)
        await clear_pending_poison_kills(game_id)
        await clear_masked_flags(game_id)
        for tg_id in list(last_words_pending_tg_ids):
            last_words_pending_tg_ids.discard(tg_id)
    except Exception:
        logger.exception('Failed to cleanup last words for game %s', game_id)


async def main():
    token = BOT_TOKEN
    if not token:
        logger.error('BOT_TOKEN not set in env (use .env or export BOT_TOKEN)')
        return

    bot = Bot(token)
    dp = Dispatcher()

    @dp.message(Command(commands=['start']))
    async def cmd_start(message: Message):
        # handle deep-link start parameter: /start game_<id>
        payload = ''
        if message.text and ' ' in message.text:
            payload = message.text.split(' ', 1)[1].strip()
        if payload.startswith('game_'):
            try:
                gid = int(payload.split('_', 1)[1])
                # create user record and attempt to add to game if exists
                user = message.from_user
                user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
                game = await get_game_by_id(gid)
                if game:
                    added = await add_player(game['id'], user_db_id)
                    # try to fetch chat title for friendlier message
                    try:
                        chat = await bot.get_chat(game['chat_id'])
                        chat_title = getattr(chat, 'title', None) or f'чат {game["chat_id"]}'
                    except Exception:
                        chat_title = f'чат {game["chat_id"]}'
                    verb = 'добавлены' if added else 'уже в'
                    await message.answer(f'Вы подключены к боту и {verb} игре в группе «{chat_title}». Вернитесь в топик игры.')
                    # update lobby message in thread to show current players
                    try:
                        if game.get('lobby_message_id'):
                            text = await _build_lobby_text(game['id'])
                            # rebuild join button
                            me = await bot.get_me()
                            bot_username = getattr(me, 'username', None)
                            if bot_username:
                                url = f'https://t.me/{bot_username}?start=game_{game["id"]}'
                                builder = InlineKeyboardBuilder()
                                builder.add(InlineKeyboardButton(text='Присоединиться к лобби', url=url))
                                kb = builder.as_markup()
                                try:
                                    await bot.edit_message_text(text=text, chat_id=game['chat_id'], message_id=game['lobby_message_id'], reply_markup=kb, parse_mode='HTML')
                                except TelegramBadRequest:
                                    pass
                    except Exception:
                        logger.exception('Failed to update lobby message for game %s', gid)

                    # signal lobby timer task (if running) by updating DB state; the polling task will pick up new player counts
                    if gid in LOBBY_TASKS:
                        # optional: wake up the task by cancelling sleep via task (no direct API) — rely on polling
                        pass

                    return
            except Exception:
                logger.exception('Failed to handle start payload')
        await message.answer('🤖 Mafia Bot готов. Введите /newgame в нужном топике, чтобы создать лобби.')

    @dp.message(Command(commands=['newgame']))
    async def cmd_newgame(message: Message):
        thread_id = getattr(message, 'message_thread_id', None)
        chat_id = message.chat.id
        if not thread_id:
            await message.reply('Команда должна быть выполнена в топике/теме (topic). Переместитесь в нужный топик и вызовите /newgame там.')
            return
        existing = await get_game_by_thread(chat_id, thread_id)
        if existing and existing.get('state') == 'lobby':
            await message.answer('ℹ️ В этом топике уже есть активное лобби. Нажмите кнопку в сообщении лобби, чтобы присоединиться, или дождитесь начала игры.')
            return
        game_id = await create_game(chat_id, thread_id)
        logger.info('Created new game %s in thread %s:%s', game_id, chat_id, thread_id)
        # persist initial lobby deadline so it survives restarts
        try:
            await set_game_phase_deadline(game_id, int(time.time()) + LOBBY_INITIAL)
        except Exception:
            logger.exception('Failed to set initial lobby deadline for %s', game_id)
        # create deep-link button so users open DM with bot and auto-join
        try:
            me = await bot.get_me()
            bot_username = getattr(me, 'username', None)
        except Exception:
            bot_username = None
        if bot_username:
            url = f'https://t.me/{bot_username}?start=game_{game_id}'
            builder = InlineKeyboardBuilder()
            builder.add(InlineKeyboardButton(text='Присоединиться к лобби', url=url))
            kb = builder.as_markup()
            msg = await message.answer('🎉 Лобби создано! Нажмите кнопку, чтобы присоединиться через личные сообщения бота.', reply_markup=kb)
            # store lobby message id so we can update it when players join
            try:
                await set_game_lobby_message(game_id, msg.message_id)
            except Exception:
                logger.exception('Failed to store lobby message id for game %s', game_id)
        else:
            await message.answer(f'🎉 Лобби создано. Игроки могут присоединиться в личных сообщениях боту: /start game_{game_id}')

        # start lobby countdown task for this game (created in newgame)
        try:
            if game_id in LOBBY_TASKS:
                try:
                    LOBBY_TASKS[game_id].cancel()
                except Exception:
                    pass
            LOBBY_TASKS[game_id] = asyncio.create_task(lobby_countdown(game_id, chat_id, thread_id, bot))
        except Exception:
            logger.exception('Failed to start lobby countdown for game %s', game_id)


    @dp.message(Command(commands=['leave']))
    async def cmd_leave(message: Message):
        thread_id = getattr(message, 'message_thread_id', None)
        chat_id = message.chat.id
        if not thread_id:
            await message.reply('Команда доступна только в топике игры.')
            return
        game = await get_game_by_thread(chat_id, thread_id)
        if not game:
            await message.reply('Лобби не найдено.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        await remove_player(game['id'], user_db_id)
        players = await list_players(game['id'])
        await message.answer(_ru_plural(len(players), ('Вы вышли из лобби. Остался', 'Вы вышли из лобби. Осталось', 'Вы вышли из лобби. Осталось')) + f' {_ru_plural(len(players), ("игрок", "игрока", "игроков"))}.')
        logger.info('User %s left game %s', user.id, game['id'])
        # update lobby message in thread if present
        try:
            if game.get('lobby_message_id'):
                text = await _build_lobby_text(game['id'])
                me = await bot.get_me()
                bot_username = getattr(me, 'username', None)
                kb = None
                if bot_username:
                    url = f'https://t.me/{bot_username}?start=game_{game["id"]}'
                    builder = InlineKeyboardBuilder()
                    builder.add(InlineKeyboardButton(text='Присоединиться к лобби', url=url))
                    kb = builder.as_markup()
                await bot.edit_message_text(text=text, chat_id=game['chat_id'], message_id=game['lobby_message_id'], reply_markup=kb, parse_mode='HTML')
        except Exception:
            logger.exception('Failed to update lobby message after leave for game %s', game['id'])

    @dp.message(Command(commands=['crazy']))
    async def cmd_crazy(message: Message):
        thread_id = getattr(message, 'message_thread_id', None)
        chat_id = message.chat.id
        if not thread_id:
            await message.reply('Команда доступна только в топике игры.')
            return
        game = await get_game_by_thread(chat_id, thread_id)
        if not game:
            await message.reply('Лобби не найдено. Создайте его с /newgame.')
            return
        if game.get('state') != 'lobby':
            await message.reply('Режим можно переключить только до начала игры.')
            return
        new_val = not game.get('crazy_mode')
        await set_crazy_mode(game['id'], new_val)
        status = '🌀 <b>Безумный режим</b> включён! Роли будут меняться каждый ход.' if new_val else '❌ Безумный режим выключен.'
        await message.answer(status, parse_mode='HTML')
        # update lobby message
        try:
            if game.get('lobby_message_id'):
                text = await _build_lobby_text(game['id'])
                me = await bot.get_me()
                bot_username = getattr(me, 'username', None)
                kb = None
                if bot_username:
                    url = f'https://t.me/{bot_username}?start=game_{game["id"]}'
                    builder = InlineKeyboardBuilder()
                    builder.add(InlineKeyboardButton(text='Присоединиться к лобби', url=url))
                    kb = builder.as_markup()
                await bot.edit_message_text(text=text, chat_id=game['chat_id'], message_id=game['lobby_message_id'], reply_markup=kb, parse_mode='HTML')
        except Exception:
            logger.exception('Failed to update lobby message after crazy toggle for game %s', game['id'])

    @dp.message(Command(commands=['startgame']))
    async def cmd_start_game(message: Message):
        thread_id = getattr(message, 'message_thread_id', None)
        chat_id = message.chat.id
        if not thread_id:
            await message.reply('Команда доступна только в топике/теме игры.')
            return
        game = await get_game_by_thread(chat_id, thread_id)
        if not game:
            await message.reply('Лобби не найдено. Создайте его с /newgame.')
            return
        players = await list_players(game['id'])
        n_players = len(players)
        if n_players < MIN_PLAYERS:
            await message.reply(f'Для начала игры нужно минимум {_ru_plural(MIN_PLAYERS, ("игрок", "игрока", "игроков"))}. Сейчас: {n_players}')
            return
        if n_players > MAX_PLAYERS:
            await message.reply(f'Максимум поддерживаемых игроков: {MAX_PLAYERS}. Сейчас: {n_players}')
            return
        # Ensure caller is part of game
        caller_db_id = await get_or_create_user(message.from_user.id, getattr(message.from_user, 'full_name', None), getattr(message.from_user, 'username', None))
        if not await game_has_player(game['id'], caller_db_id):
            await message.reply('Только игроки из лобби могут запускать игру. Присоединитесь через кнопку в сообщении лобби.')
            return

        started = await try_start_game(game['id'])
        if not started:
            await message.reply('Игра уже была запущена другим игроком.')
            return
        await set_game_started(game['id'])
        logger.info('Game %s started in thread %s', game['id'], thread_id)

        await distribute_roles_and_notify(game['id'], chat_id, thread_id, bot, n_players)

        try:
            g_check = await get_game_by_id(game['id'])
            if g_check and g_check.get('crazy_mode'):
                await bot.send_message(chat_id, '🌀 <b>Безумный режим!</b> Роли будут перемешаны после каждого дня.', message_thread_id=thread_id, parse_mode='HTML')
        except Exception:
            pass

        # send button to view role in DM
        try:
            me = await bot.get_me()
            bu = getattr(me, 'username', None)
            builder = InlineKeyboardBuilder()
            if bu:
                url = f'https://t.me/{bu}?start=game_{game["id"]}'
                builder.add(InlineKeyboardButton(text='👁 Посмотреть роль', url=url))
                kb = builder.as_markup()
                await bot.send_message(chat_id, '🎮 <b>Игра начинается!</b>\n\nВ течение нескольких секунд бот пришлёт вам личное сообщение с ролью и её описанием.', message_thread_id=thread_id, reply_markup=kb, parse_mode='HTML')
            else:
                await bot.send_message(chat_id, '🎮 <b>Игра начинается!</b>\n\nВ течение нескольких секунд бот пришлёт вам личное сообщение с ролью и её описанием.', message_thread_id=thread_id, parse_mode='HTML')
        except Exception:
            logger.exception('Failed to send game start button for game %s', game['id'])

        # start game loop task for this game
        try:
            asyncio.create_task(run_game_loop(game['id'], chat_id, thread_id, bot))
        except Exception:
            logger.exception('Failed to start game loop for game %s', game['id'])

    @dp.message(Command(commands=['shop']))
    async def cmd_shop(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Магазин доступен только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance_coins = user_data.get('coins', 0) if user_data else 0
        balance_diamonds = user_data.get('diamonds', 0) if user_data else 0
        coin_items = await get_shop_items()
        builder = InlineKeyboardBuilder()
        lines = [f'🪙 <b>Магазин</b>\n',
                 f'Баланс: <b>{balance_coins}</b> 🪙 | 💎 <b>{balance_diamonds}</b>\n',
                 f'{"─" * 20}']
        if coin_items:
            lines.append('\n🪙 <b>За монеты:</b>')
            for item in coin_items:
                lines.append(f'{item["data"]} — {item["price"]} 🪙')
                builder.add(InlineKeyboardButton(text=f'{item["data"]} — {item["price"]} 🪙', callback_data=f'shop_buy:{item["name"]}'))
        if DIAMOND_SHOP_ITEMS:
            lines.append('\n💎 <b>За алмазы:</b>')
            for item in DIAMOND_SHOP_ITEMS:
                lines.append(f'{item["display"]} — {item["price"]} 💎')
                builder.add(InlineKeyboardButton(text=f'{item["display"]} — {item["price"]} 💎', callback_data=f'dshop_buy:{item["name"]}'))
        lines.append('\n⭐ <b>Купить монеты/алмазы:</b> /buy')
        builder.adjust(1)
        await message.answer('\n'.join(lines), reply_markup=builder.as_markup(), parse_mode='HTML')

    @dp.message(Command(commands=['buy']))
    async def cmd_buy(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Покупки доступны только в личных сообщениях с ботом.')
            return
        lines = ['⭐ <b>Купить за Telegram Stars</b>\n']
        lines.append('💎 <b>Алмазы:</b>')
        for p in DIAMOND_PACKAGES:
            lines.append(f'{p["label"]} — {p["stars"]} ⭐')
        lines.append('')
        lines.append('🪙 <b>Монеты:</b>')
        for p in COIN_PACKAGES:
            lines.append(f'{p["label"]} — {p["stars"]} ⭐')
        lines.append('')
        lines.append('Нажмите на пакет ниже:')
        builder = InlineKeyboardBuilder()
        for p in DIAMOND_PACKAGES:
            builder.add(InlineKeyboardButton(text=f'{p["label"]} — {p["stars"]} ⭐', callback_data=f'buy_diamond:{p["diamonds"]}:{p["stars"]}'))
        builder.row()
        for p in COIN_PACKAGES:
            builder.add(InlineKeyboardButton(text=f'{p["label"]} — {p["stars"]} ⭐', callback_data=f'buy_coin:{p["coins"]}:{p["stars"]}'))
        builder.adjust(1)
        await message.answer('\n'.join(lines), reply_markup=builder.as_markup(), parse_mode='HTML')

    @dp.callback_query(lambda c: c.data and c.data.startswith('buy_diamond:'))
    async def handle_buy_diamond(call: CallbackQuery):
        parts = call.data.split(':')
        diamonds = int(parts[1])
        stars = int(parts[2])
        await call.answer()
        prices = [LabeledPrice(label=f'{diamonds} 💎', amount=stars)]
        await call.message.answer_invoice(
            title=f'{diamonds} 💎 Алмазов',
            description=f'Пополнение баланса на {diamonds} алмазов.',
            payload=f'diamonds:{diamonds}',
            currency='XTR',
            prices=prices,
        )

    @dp.callback_query(lambda c: c.data and c.data.startswith('buy_coin:'))
    async def handle_buy_coin(call: CallbackQuery):
        parts = call.data.split(':')
        coins = int(parts[1])
        stars = int(parts[2])
        await call.answer()
        prices = [LabeledPrice(label=f'{coins} 🪙', amount=stars)]
        await call.message.answer_invoice(
            title=f'{coins} 🪙 Монет',
            description=f'Пополнение баланса на {coins} монет.',
            payload=f'coins:{coins}',
            currency='XTR',
            prices=prices,
        )

    @dp.pre_checkout_query()
    async def pre_checkout_handler(pre_checkout_q: PreCheckoutQuery):
        await pre_checkout_q.answer(ok=True)

    @dp.message(F.successful_payment)
    async def successful_payment_handler(message: Message):
        payload = message.successful_payment.invoice_payload
        user_db_id = await get_or_create_user(message.from_user.id, getattr(message.from_user, 'full_name', None), getattr(message.from_user, 'username', None))
        if payload.startswith('diamonds:'):
            amount = int(payload.split(':')[1])
            await add_diamonds(user_db_id, amount, 'Покупка за Stars')
            await message.answer(f'✅ <b>+{amount} 💎</b> зачислено! Спасибо за поддержку!', parse_mode='HTML')
        elif payload.startswith('coins:'):
            amount = int(payload.split(':')[1])
            await add_coins(user_db_id, amount, 'Покупка за Stars')
            await message.answer(f'✅ <b>+{amount} 🪙</b> зачислено! Спасибо за поддержку!', parse_mode='HTML')

    @dp.message(Command(commands=['help']))
    async def cmd_help(message: Message):
        lines = [
            '🤖 <b>Mafia Bot — команды</b>\n',
            '<b>В топике игры:</b>',
            '/newgame — создать лобби',
            '/startgame — принудительный старт (минимум 4 игрока)',
            '/leave — покинуть лобби',
            '/crazy — включить/выключить безумный режим',
            '',
            '<b>В личных сообщениях:</b>',
            '/profile — профиль и статистика',
            '/shop — магазин предметов',
            '/buy — купить монеты/алмазы за Telegram Stars',
            '/daily — ежедневный бонус',
            '/anon текст — анонимка в игровой чат',
            '/lottery — использовать лотерейный билет',
        ]
        await message.answer('\n'.join(lines), parse_mode='HTML')

    @dp.message(Command(commands=['daily']))
    async def cmd_daily(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Бонус доступен только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        success, text = await claim_daily(user_db_id)
        await message.answer(text, parse_mode='HTML')

    @dp.message(Command(commands=['anon']))
    async def cmd_anon(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Анонимка доступна только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        # check ownership
        if await get_user_item_count(user_db_id, 'anonymous') < 1:
            await message.answer('❌ У вас нет 📱 Анонимки. Купите в магазине — /shop')
            return
        args = message.text.removeprefix('/anon').strip()
        if not args:
            await message.answer('📱 Использование: <code>/anon текст сообщения</code>\n\nСообщение будет отправлено в игровой чат как <b>Неизвестный</b>.', parse_mode='HTML')
            return
        # find active games where this user is a player
        user_games = await get_games_by_state('night')
        user_games += await get_games_by_state('discussion')
        user_games += await get_games_by_state('voting')
        user_games += await get_games_by_state('lynch_decision')
        target_game = None
        for g in user_games:
            if await game_has_player(g['id'], user_db_id):
                target_game = g
                break
        if not target_game:
            await message.answer('❌ Вы не участвуете ни в одной активной игре.')
            return
        await use_user_item(user_db_id, 'anonymous')
        chat_id = target_game.get('chat_id')
        thread_id = target_game.get('thread_id')
        try:
            await bot.send_message(chat_id, f'📱 <b>Неизвестный:</b> {args}', message_thread_id=thread_id, parse_mode='HTML')
            await message.answer('✅ Анонимка отправлена в игровой чат.')
        except Exception as e:
            logger.exception('Failed to send anonymous message')
            await message.answer('❌ Не удалось отправить сообщение.')

    @dp.message(Command(commands=['lottery']))
    async def cmd_lottery(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Лотерея доступна только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        if await get_user_item_count(user_db_id, 'lottery') < 1:
            await message.answer('❌ У вас нет 🎲 Счастливого билета. Купите в магазине — /shop')
            return
        await use_user_item(user_db_id, 'lottery')
        if random.random() < 0.5:
            await add_coins(user_db_id, 100, 'Счастливый билет')
            await message.answer('🎉 <b>Поздравляем!</b> Вы выиграли <b>+100 🪙</b>!', parse_mode='HTML')
        else:
            await message.answer('😞 <b>Не повезло.</b> В этот раз ничего. Попробуйте ещё раз!', parse_mode='HTML')

    async def _profile_text_kb(user_db_id: int, tg_user) -> tuple[str, InlineKeyboardMarkup]:
        user_data = await get_user_by_dbid(user_db_id)
        name_link = f'<a href="tg://user?id={tg_user.id}">{tg_user.full_name}</a>'
        coins = user_data.get('coins', 0)
        diamonds = user_data.get('diamonds', 0)
        wins = user_data.get('wins', 0)
        losses = user_data.get('losses', 0)
        total = wins + losses
        ratio = f'{wins / total:.2f}' if total > 0 else '—'

        created_raw = user_data.get('created_at')
        if created_raw:
            try:
                created_dt = datetime.fromisoformat(created_raw) if isinstance(created_raw, str) else created_raw
                days = (datetime.now() - created_dt).days
                registered = _ru_plural(days, ('день', 'дня', 'дней')) if days else 'менее дня'
            except Exception:
                registered = 'неизвестно'
        else:
            registered = 'неизвестно'

        items = await get_user_items_with_details(user_db_id)
        items_text = '\n'.join(f'• {i["display"]} — {i["quantity"]} шт.' for i in items) if items else '—'

        ach_count = await get_achievement_count(user_db_id)
        ach_total = len(ACHIEVEMENTS)

        text = (
            f'👤 <b>Профиль</b>\n\n'
            f'{name_link}\n'
            f'📅 В боте: {registered}\n\n'
            f'🏆 Победы: {wins}\n'
            f'💔 Поражения: {losses}\n'
            f'📊 Соотношение: {ratio}\n\n'
            f'🪙 Монеты: {coins}\n'
            f'💎 Алмазы: {diamonds}\n'
            f'🏅 Достижения: {ach_count}/{ach_total}\n\n'
            f'🎒 <b>Предметы:</b>\n{items_text}'
        )

        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text=f'🏅 Достижения ({ach_count}/{ach_total})', callback_data='show_achievements'))
        return text, builder.as_markup()

    @dp.message(Command(commands=['profile']))
    async def cmd_profile(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Профиль доступен только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        if not user_data:
            await message.answer('❌ Не удалось загрузить профиль.')
            return
        text, kb = await _profile_text_kb(user_db_id, user)
        await message.answer(text, parse_mode='HTML', reply_markup=kb)

    @dp.message(Command(commands=['stats']))
    async def cmd_stats(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Статистика доступна только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        if not user_data:
            await message.answer('❌ Не удалось загрузить статистику.')
            return

        wins = user_data.get('wins', 0)
        losses = user_data.get('losses', 0)
        total = wins + losses
        ratio = f'{wins / total:.2f}' if total > 0 else '—'

        play_secs = await get_total_play_seconds(user_db_id)
        if play_secs >= 3600:
            play_time = f'{play_secs // 3600}ч {(play_secs % 3600) // 60}м'
        elif play_secs >= 60:
            play_time = f'{play_secs // 60}м'
        else:
            play_time = f'{play_secs}с'

        # per-role stats
        role_stats = await get_role_stats(user_db_id)
        role_lines = []
        for rs in role_stats:
            meta = _role_meta_by_name(rs['role_name'])
            icon = meta.get('icon', '❓') if meta else '❓'
            display = meta.get('display_ru', rs['role_name']) if meta else rs['role_name']
            r_total = rs['games']
            r_ratio = f'{rs["wins"] / r_total:.2f}' if r_total > 0 else '—'
            role_lines.append(f'{icon} {display}: {rs["wins"]}п/{rs["losses"]}пор ({r_ratio}) — {r_total}игр')
        role_text = '\n'.join(role_lines) if role_lines else '—'

        # favorite item (most owned)
        items = await get_user_items_with_details(user_db_id)
        fav_item = max(items, key=lambda i: i['quantity']) if items else None
        fav_text = f'{fav_item["display"]} — {fav_item["quantity"]} шт.' if fav_item else '—'

        text = (
            f'📊 <b>Статистика</b>\n\n'
            f'👤 {user.full_name}\n'
            f'🏆 Победы: {wins}\n'
            f'💔 Поражения: {losses}\n'
            f'📊 Соотношение: {ratio}\n'
            f'⏱ В игре: {play_time}\n\n'
            f'🎭 <b>По ролям:</b>\n{role_text}\n\n'
            f'⭐ <b>Любимый предмет:</b> {fav_text}'
        )
        await message.answer(text, parse_mode='HTML')

    @dp.message(Command(commands=['achievements', 'achievement']))
    async def cmd_achievements(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Достижения доступны только в личных сообщениях с ботом.')
            return
        user = message.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        await _send_achievements_list(message, user_db_id)

    async def _send_achievements_list(msg_or_call, user_db_id: int):
        unlocked = await get_user_achievements(user_db_id)
        unlocked_map: dict[str, set[int]] = {}
        for a in unlocked:
            unlocked_map.setdefault(a['achievement_name'], set()).add(a['tier'])

        lines = ['🏅 <b>Достижения</b>\n']
        for ach in ACHIEVEMENTS:
            tiers = unlocked_map.get(ach['name'], set())
            tier_str = ''
            for i in range(3):
                te = TIER_EMOJIS[i + 1]
                if i + 1 in tiers:
                    tier_str += te
                else:
                    tier_str += f'<code>[ ]</code>'
            coins_total = sum(ach['coins'][i] for i in range(3) if (i + 1) in tiers)
            desc = ach['desc']
            lines.append(f'{tier_str} {ach["display"]}\n  {desc} (+{coins_total}🪙)')
        total_types = len({a['achievement_name'] for a in unlocked})
        lines.append(f'\n📊 Типов ачивок: {total_types}/{len(ACHIEVEMENTS)}')
        text = '\n'.join(lines)

        if isinstance(msg_or_call, CallbackQuery):
            back_builder = InlineKeyboardBuilder()
            back_builder.add(InlineKeyboardButton(text='◀ В профиль', callback_data='back_to_profile'))
            await msg_or_call.edit_text(text, parse_mode='HTML', reply_markup=back_builder.as_markup())
        else:
            await msg_or_call.answer(text, parse_mode='HTML')

    @dp.callback_query(lambda c: c.data == 'show_achievements')
    async def cb_show_achievements(call: CallbackQuery):
        await call.answer()
        user = call.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        await _send_achievements_list(call, user_db_id)

    @dp.callback_query(lambda c: c.data == 'back_to_profile')
    async def cb_back_to_profile(call: CallbackQuery):
        await call.answer()
        user = call.from_user
        user_db_id = await get_or_create_user(user.id, getattr(user, 'full_name', None), getattr(user, 'username', None))
        text, kb = await _profile_text_kb(user_db_id, user)
        await call.message.edit_text(text, parse_mode='HTML', reply_markup=kb)

    @dp.message(Command(commands=['give']))
    async def cmd_give(message: Message):
        if message.chat.type != 'private':
            await message.answer('ℹ️ Передача доступна только в личных сообщениях с ботом.')
            return
        args = message.text.removeprefix('/give').strip()
        if not args:
            await message.answer('📦 Использование: <code>/give @username_or_id item_name [количество]</code>\n\nПример: /give @durov protection 2', parse_mode='HTML')
            return
        parts = args.split()
        if len(parts) < 2:
            await message.answer('❌ Укажите получателя и название предмета.')
            return
        target_str = parts[0]
        item_name = parts[1]
        quantity = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1

        # resolve target user
        target_clean = target_str.lstrip('@')
        target_user = None
        # try by tg_id if numeric
        if target_str.lstrip('-').isdigit():
            target_user = await get_user_by_dbid(int(target_str))
        if not target_user:
            target_user = await get_user_by_username(target_clean)
        if not target_user:
            await message.answer(f'❌ Пользователь {target_str} не найден.')
            return

        sender = message.from_user
        sender_db_id = await get_or_create_user(sender.id, getattr(sender, 'full_name', None), getattr(sender, 'username', None))
        if target_user['id'] == sender_db_id:
            await message.answer('❌ Нельзя передать предмет самому себе.')
            return

        # check item exists in shop
        shop_names = {i['name'] for i in SHOP_ITEMS} | {i['name'] for i in DIAMOND_SHOP_ITEMS}
        if item_name not in shop_names:
            await message.answer(f'❌ Предмет <b>{item_name}</b> не найден в магазине.', parse_mode='HTML')
            return

        ok, msg = await transfer_item(sender_db_id, target_user['id'], item_name, quantity)
        if ok:
            target_mention = target_user.get('username') or target_user.get('tg_username') or str(target_user['id'])
        else:
            target_mention = ''
        await message.answer(msg + (f' Получатель: {target_mention}' if ok else ''), parse_mode='HTML')

    @dp.callback_query(lambda c: c.data and c.data.startswith('shop_buy:'))
    async def handle_shop_buy(call: CallbackQuery):
        await call.answer()
        item_name = call.data.split(':', 1)[1]
        meta = next((it for it in SHOP_ITEMS if it['name'] == item_name), None)
        if not meta:
            await call.message.edit_text('❌ Товар не найден.', reply_markup=None)
            return
        user_db_id = await get_or_create_user(call.from_user.id, getattr(call.from_user, 'full_name', None), getattr(call.from_user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance = user_data.get('coins', 0) if user_data else 0
        price = meta['price']

        header = f'{meta["display"]} — {price} 🪙'
        desc = meta['description']
        if balance < price:
            text = (f'{header}\n\n{desc}\n\n'
                    f'🪙 Баланс: {balance} 🪙\n\n'
                    f'❌ <b>Недостаточно монет.</b>')
            await call.message.edit_text(text, reply_markup=None, parse_mode='HTML')
            return

        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text='✅ Да', callback_data=f'shop_confirm:{item_name}:1'))
        builder.add(InlineKeyboardButton(text='⬅ Назад', callback_data='open_shop'))
        kb = builder.as_markup()

        text = (f'{header}\n\n{desc}\n\n'
                f'🪙 Баланс: {balance} 🪙\n\n'
                f'<b>Подтвердите покупку:</b>')
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

    @dp.callback_query(lambda c: c.data and c.data.startswith('shop_confirm:'))
    async def handle_shop_confirm(call: CallbackQuery):
        await call.answer()
        parts = call.data.split(':')
        if len(parts) != 3:
            return
        _, item_name, confirm = parts
        if confirm != '1':
            back_builder = InlineKeyboardBuilder()
            back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
            await call.message.edit_text('❌ Покупка отменена.', reply_markup=back_builder.as_markup())
            return
        meta = next((it for it in SHOP_ITEMS if it['name'] == item_name), None)
        if not meta:
            await call.message.edit_text('❌ Товар не найден.', reply_markup=None)
            return
        user_db_id = await get_or_create_user(call.from_user.id, getattr(call.from_user, 'full_name', None), getattr(call.from_user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance = user_data.get('coins', 0) if user_data else 0
        price = meta['price']
        if balance < price:
            back_builder = InlineKeyboardBuilder()
            back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
            await call.message.edit_text('❌ Недостаточно монет. Покупка отменена.', reply_markup=back_builder.as_markup())
            return
        await add_user_item(user_db_id, meta['name'])
        await add_coins(user_db_id, -price, f'Покупка: {meta["name"]}')
        back_builder = InlineKeyboardBuilder()
        back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
        await call.message.edit_text(f'✅ <b>Куплено:</b> {meta["display"]} за {price} 🪙.', reply_markup=back_builder.as_markup(), parse_mode='HTML')

    @dp.callback_query(lambda c: c.data and c.data.startswith('dshop_buy:'))
    async def handle_dshop_buy(call: CallbackQuery):
        item_name = call.data.split(':', 1)[1]
        meta = next((i for i in DIAMOND_SHOP_ITEMS if i['name'] == item_name), None)
        if not meta:
            await call.answer('❌ Товар не найден.')
            return
        await call.answer()
        user_db_id = await get_or_create_user(call.from_user.id, getattr(call.from_user, 'full_name', None), getattr(call.from_user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance = user_data.get('diamonds', 0) if user_data else 0
        price = meta['price']

        header = f'{meta["display"]} — {price} 💎'
        desc = meta['description']
        if balance < price:
            text = (f'{header}\n\n{desc}\n\n'
                    f'💎 Баланс: {balance} 💎\n\n'
                    f'❌ <b>Недостаточно алмазов.</b> Купите алмазы через /buy')
            await call.message.edit_text(text, reply_markup=None, parse_mode='HTML')
            return

        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text='✅ Да', callback_data=f'dshop_confirm:{item_name}:1'))
        builder.add(InlineKeyboardButton(text='⬅ Назад', callback_data='open_shop'))
        kb = builder.as_markup()

        text = (f'{header}\n\n{desc}\n\n'
                f'💎 Баланс: {balance} 💎\n\n'
                f'<b>Подтвердите покупку:</b>')
        await call.message.edit_text(text, reply_markup=kb, parse_mode='HTML')

    @dp.callback_query(lambda c: c.data and c.data.startswith('dshop_confirm:'))
    async def handle_dshop_confirm(call: CallbackQuery):
        await call.answer()
        parts = call.data.split(':')
        if len(parts) != 3:
            return
        _, item_name, confirm = parts
        if confirm != '1':
            back_builder = InlineKeyboardBuilder()
            back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
            await call.message.edit_text('❌ Покупка отменена.', reply_markup=back_builder.as_markup())
            return
        meta = next((i for i in DIAMOND_SHOP_ITEMS if i['name'] == item_name), None)
        if not meta:
            await call.message.edit_text('❌ Товар не найден.', reply_markup=None)
            return
        user_db_id = await get_or_create_user(call.from_user.id, getattr(call.from_user, 'full_name', None), getattr(call.from_user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance = user_data.get('diamonds', 0) if user_data else 0
        price = meta['price']
        if balance < price:
            back_builder = InlineKeyboardBuilder()
            back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
            await call.message.edit_text('❌ Недостаточно алмазов. Купите алмазы через /buy', reply_markup=back_builder.as_markup())
            return
        await add_user_item(user_db_id, meta['name'])
        await add_diamonds(user_db_id, -price, f'Покупка: {meta["name"]}')
        back_builder = InlineKeyboardBuilder()
        back_builder.add(InlineKeyboardButton(text='◀ В магазин', callback_data='open_shop'))
        await call.message.edit_text(f'✅ <b>Куплено:</b> {meta["display"]} за {price} 💎.', reply_markup=back_builder.as_markup(), parse_mode='HTML')

    @dp.callback_query(lambda c: c.data == 'open_shop')
    async def handle_open_shop(call: CallbackQuery):
        if call.message.chat.type != 'private':
            await call.answer('Магазин доступен только в ЛС.', show_alert=True)
            return
        await call.answer()
        user_db_id = await get_or_create_user(call.from_user.id, getattr(call.from_user, 'full_name', None), getattr(call.from_user, 'username', None))
        user_data = await get_user_by_dbid(user_db_id)
        balance_coins = user_data.get('coins', 0) if user_data else 0
        balance_diamonds = user_data.get('diamonds', 0) if user_data else 0
        coin_items = await get_shop_items()
        builder = InlineKeyboardBuilder()
        lines = [f'🪙 <b>Магазин</b>\n',
                 f'Баланс: <b>{balance_coins}</b> 🪙 | 💎 <b>{balance_diamonds}</b>\n']
        if coin_items:
            for item in coin_items:
                builder.add(InlineKeyboardButton(text=f'{item["data"]} — {item["price"]} 🪙', callback_data=f'shop_buy:{item["name"]}'))
        if DIAMOND_SHOP_ITEMS:
            for item in DIAMOND_SHOP_ITEMS:
                builder.add(InlineKeyboardButton(text=f'{item["display"]} — {item["price"]} 💎', callback_data=f'dshop_buy:{item["name"]}'))
        builder.adjust(1)
        text = '\n'.join(lines) + '\nВыберите товар:'
        try:
            await call.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        except Exception:
            await call.message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')

    # handle last words from killed players (DMs only)
    @dp.message(lambda msg: msg.from_user.id in last_words_pending_tg_ids and msg.text and not msg.message_thread_id)
    async def handle_last_word(message: Message):
        tg_id = message.from_user.id
        if tg_id in last_words_pending_tg_ids:
            await submit_last_word(tg_id, message.text)
            try:
                await message.answer('✅ Ваше предсмертное сообщение получено.')
            except Exception:
                pass

    # message filter to restrict posting during running games
    @dp.message()
    async def enforce_topic_permissions(message: Message):
        # only care about messages sent in topics
        thread_id = getattr(message, 'message_thread_id', None)
        if not thread_id:
            return
        chat_id = message.chat.id
        game = await get_game_by_thread(chat_id, thread_id)
        if not game:
            return
        state = game.get('state')
        # allow during lobby or after finished
        if state in ('lobby', 'finished'):
            return
        # allow messages only during discussion or lynch phase
        if state in ('discussion', 'lynch'):
            # allow only players who are alive and part of the game
            user_db = await get_or_create_user(message.from_user.id, getattr(message.from_user, 'full_name', None), getattr(message.from_user, 'username', None))
            players = await list_players(game['id'])
            for p in players:
                if p['user_id'] == user_db and p.get('alive') == 1:
                    # also deny if player is blackmailed
                    flags = await get_player_flags(game['id'], user_db)
                    if flags and flags.get('blackmailed') == 1:
                        return
                    return
        # otherwise delete message
        try:
            await bot.delete_message(chat_id, message.message_id)
            # optionally notify user in DM — if player is dead, send dead-specific text
            try:
                user_db = await get_or_create_user(message.from_user.id, getattr(message.from_user, 'full_name', None), getattr(message.from_user, 'username', None))
                players = await list_players(game['id'])
                # find player entry
                player_row = next((p for p in players if p['user_id'] == user_db), None)
                if player_row and player_row.get('alive') == 0:
                    # dead player
                    await bot.send_message(message.from_user.id, '💀 <b>Вы мертвы.</b> Вы не можете отправлять сообщения в игровой теме. Спасибо за участие. Вы можете наблюдать за игрой.', parse_mode='HTML')
                else:
                    await bot.send_message(message.from_user.id, '🚫 <b>В топике идёт игра.</b> Подождите окончания обсуждения или голосования, чтобы писать.', parse_mode='HTML')
            except Exception:
                pass
        except Exception:
            logger.exception('Failed to delete message in restricted topic %s:%s', chat_id, thread_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith('na:'))
    async def handle_night_action(call: CallbackQuery):
        # format: na:game_id:actor_db_id:target_db_id or na:game_id:actor_db_id:target_db_id:allseeing
        await call.answer()
        parts = call.data.split(':')
        if len(parts) < 4:
            await call.message.answer('Некорректные данные.')
            return
        _, game_id, actor_db_id, target_db_id = parts[:4]
        is_allseeing = len(parts) >= 5 and parts[4] == 'allseeing'
        is_carnival_mask = len(parts) >= 5 and parts[4] == 'carnival_mask'
        game_id = int(game_id)
        actor_db_id = int(actor_db_id)
        target_db_id = int(target_db_id)
        # verify that caller is the correct user
        caller_tg = call.from_user.id
        actor_row = await get_user_by_dbid(actor_db_id)
        if not actor_row or actor_row['tg_id'] != caller_tg:
            await call.answer('Эта кнопка не для вас.', show_alert=True)
            return
        # verify game is in night phase and actor is alive
        try:
            na_game = await get_game_by_id(game_id)
            if not na_game or na_game.get('state') != 'night':
                await call.answer('Сейчас не ночная фаза.', show_alert=True)
                return
            if not await is_player_alive(game_id, actor_db_id):
                await call.answer('Вы мертвы и не можете совершать ночные действия.', show_alert=True)
                return
        except Exception:
            logger.exception('Failed to verify night action state for %s', game_id)
        if is_allseeing:
            if await is_item_used(game_id, actor_db_id, 'allseeing'):
                await call.answer('Вы уже использовали Всевидящего в этой игре.', show_alert=True)
                return
            role = '_allseeing'
            await use_user_item(actor_db_id, 'allseeing')
            await mark_item_used(game_id, actor_db_id, 'allseeing')
        elif is_carnival_mask:
            if await is_item_used(game_id, actor_db_id, 'carnival_mask'):
                await call.answer('Вы уже использовали Карнавальную маску в этой игре.', show_alert=True)
                return
            await set_player_masked(game_id, actor_db_id)
            await use_user_item(actor_db_id, 'carnival_mask')
            await mark_item_used(game_id, actor_db_id, 'carnival_mask')
            role = '_carnival_mask'
        else:
            role = await get_player_role(game_id, actor_db_id)
        await record_night_action(game_id, actor_db_id, role or '', target_db_id)
        async with _night_actors_lock:
            _night_actors.setdefault(game_id, set()).add(actor_db_id)
        # show friendly label
        if target_db_id == 0:
            label = 'Пропущено'
        else:
            tgt = await get_user_by_dbid(int(target_db_id))
            label = tgt.get('username') or str(tgt.get('tg_id'))
        try:
            await call.message.edit_text(f'✅ <b>Ваш выбор зафиксирован:</b> {label}', reply_markup=None, parse_mode='HTML')
        except Exception:
            # fallback
            await call.message.answer('✅ Ваш выбор зафиксирован.')
        logger.info('Recorded night action: game=%s actor=%s role=%s target=%s', game_id, actor_db_id, role, target_db_id)

        # announce to thread that this role acted (reveal role, not the player)
        try:
            game = await get_game_by_id(game_id)
            if game:
                chat_id = game['chat_id']
                thread_id = game['thread_id']
                role_meta = _role_meta_by_name(role) or {}
                role_label = role_meta.get('display_ru', role)
                # map specific roles to friendly public messages (Russian) without revealing actor
                role_announcements = {
                    'Mafia': '🌑 Мафия выбрала жертву.',
                    'Godfather': '🌑 Мафия выбрала жертву.',
                    'Detective': '🔍 Комиссар провёл проверку.',
                    'Doctor': '🩺 Доктор вышел на дежурство.',
                    'Bodyguard': '🛡️ Телохранитель встал на дежурство.',
                    'Vigilante': '🔫 Горожанин совершил ночное действие.',
                    'SerialKiller': '🔪 Серийный убийца выбрал жертву.',
                    'Roleblocker': '🚫 Кто-то попытался заблокировать действие.',
                    'Tracker': '👣 Следопыт следил за игроком.',
                    'Consigliere': '🎯 Советник получил(а) информацию.',
                    'Framer': '🧩 Кто-то пытался подставить невиновного.',
                    'Blackmailer': '🛑 Кто-то отправил шантажное сообщение.',
                    '_allseeing': '👁️ Всевидящий использовал свой дар.',
                    '_carnival_mask': '🎭 Карнавальная маска активирована.',
                }
                ann = role_announcements.get(role, f'{role_label} выполнил(а) ночное действие.')
                await bot.send_message(chat_id, ann, message_thread_id=thread_id)
        except Exception:
            logger.exception('Failed to announce night action for game %s', game_id)

    @dp.callback_query(lambda c: c.data and c.data.startswith('vote:'))
    async def handle_vote(call: CallbackQuery):
        # format: vote:game_id:voter_db_id:target_db_id
        await call.answer()
        parts = call.data.split(':')
        if len(parts) != 4:
            await call.message.answer('Некорректные данные.')
            return
        _, game_id, voter_db_id, target_db_id = parts
        game_id = int(game_id)
        voter_db_id = int(voter_db_id)
        target_db_id = int(target_db_id)
        caller_tg = call.from_user.id
        voter_row = await get_user_by_dbid(voter_db_id)
        if not voter_row or voter_row['tg_id'] != caller_tg:
            await call.answer('Эта кнопка не для вас.', show_alert=True)
            return

        # check game is in voting phase and voter is alive
        try:
            v_game = await get_game_by_id(game_id)
            if not v_game or v_game.get('state') != 'voting':
                await call.answer()
                return
            if not await is_player_alive(game_id, voter_db_id):
                await call.answer()
                return
            # mafia cannot vote against mafia
            if target_db_id != 0:
                voter_role = await get_player_role(game_id, voter_db_id)
                if voter_role:
                    vm = _role_meta_by_name(voter_role)
                    if vm and vm.get('team') == 'mafia':
                        tgt_role = await get_player_role(game_id, target_db_id)
                        if tgt_role:
                            tm = _role_meta_by_name(tgt_role)
                            if tm and tm.get('team') == 'mafia':
                                await call.answer('Нельзя голосовать против своей мафии.', show_alert=True)
                                return
        except Exception:
            logger.exception('Failed to verify vote state for %s', game_id)

        await record_vote(game_id, voter_db_id, target_db_id)
        if target_db_id == 0:
            label = 'Пропущено'
        else:
            tgt = await get_user_by_dbid(target_db_id)
            label = tgt.get('username') or str(tgt.get('tg_id'))
        try:
            await call.message.edit_text(f'🗳️ <b>Вы проголосовали за:</b> {label}', reply_markup=None, parse_mode='HTML')
        except Exception:
            pass

        # announce vote in thread
        try:
            if v_game:
                chat_id = v_game['chat_id']
                thread_id = v_game['thread_id']
                voter_name = voter_row.get('username') or str(voter_row.get('tg_id'))
                if target_db_id == 0:
                    tgt_name = 'Пропуск'
                else:
                    tgt_row = await get_user_by_dbid(target_db_id)
                    tgt_name = tgt_row.get('username') or str(tgt_row.get('tg_id'))
                # use HTML mentions
                try:
                    voter_mention = f'<a href="tg://user?id={voter_row.get("tg_id")}">{voter_name}</a>'
                    if target_db_id == 0:
                        tgt_mention = '<b>Пропуск</b>'
                    else:
                        tgt_row = await get_user_by_dbid(target_db_id)
                        tgt_mention = f'<a href="tg://user?id={tgt_row.get("tg_id")}">{tgt_name}</a>'
                    await bot.send_message(chat_id, f'🗳️ Голос: {voter_mention} проголосовал(а) за {tgt_mention}', message_thread_id=thread_id, parse_mode='HTML')
                except Exception:
                    # fallback to plain text
                    await bot.send_message(chat_id, f'Голос: {voter_name} проголосовал(а) за {tgt_name}', message_thread_id=thread_id)
        except Exception:
            logger.exception('Failed to announce vote for game %s', game_id)

    # handlers for lynch decision
    @dp.callback_query(lambda c: c.data and c.data.startswith('lynch:'))
    async def handle_lynch_decision(call: CallbackQuery):
        # format: lynch:game_id:candidate_db_id:choice
        await call.answer()
        parts = call.data.split(':')
        if len(parts) != 4:
            await call.message.answer('Некорректные данные.')
            return
        _, game_id, candidate_db_id, choice = parts
        game_id = int(game_id)
        candidate_db_id = int(candidate_db_id)
        choice = int(choice)
        # map caller to user db id
        caller = call.from_user
        voter_db_id = await get_or_create_user(caller.id, getattr(caller, 'full_name', None), getattr(caller, 'username', None))

        # check if voter is alive and part of this game
        try:
            game_players = await list_players(game_id)
            voter_found = None
            for gp in game_players:
                if gp['user_id'] == voter_db_id:
                    voter_found = gp
                    break
            if not voter_found:
                try:
                    await call.answer('Вы не участвуете в этой игре.', show_alert=True)
                except Exception:
                    pass
                return
            if voter_found.get('alive') == 0:
                try:
                    await call.answer('Нельзя голосовать мёртвым.', show_alert=True)
                except Exception:
                    pass
                return
            if voter_db_id == candidate_db_id:
                try:
                    await call.answer('Нельзя голосовать за себя.', show_alert=True)
                except Exception:
                    pass
                return
        except Exception:
            logger.exception('Failed to verify voter for lynch')

        # prevent duplicate voting: check if caller already voted for this candidate
        try:
           existing = await get_lynch_vote_choice(game_id, candidate_db_id, voter_db_id)
           if existing is not None:
               try:
                   await call.answer('Вы уже голосовали.', show_alert=True)
               except Exception:
                   pass
               return
        except Exception:
           logger.exception('Failed to check existing lynch vote')

        # record the vote
        try:
           await record_lynch_vote(game_id, candidate_db_id, voter_db_id, choice)
        except sqlite3.IntegrityError:
           # race condition - another concurrent request inserted the same vote
           try:
               await call.answer('Вы уже голосовали.', show_alert=True)
           except Exception:
               pass
           return
        except Exception:
           logger.exception('Failed to record lynch vote for %s by %s', candidate_db_id, voter_db_id)
           try:
               await call.answer('Произошла ошибка при записи голоса.', show_alert=True)
           except Exception:
               pass
           return

        # confirm to voter (do not edit the thread message)
        try:
            await call.answer('Ваш голос учтён.', show_alert=False)
        except Exception:
            pass

        # update live counts on lynch message
        try:
           lm = await get_lynch_message(game_id, candidate_db_id)
           if lm:
               counts = await get_lynch_vote_counts(game_id, candidate_db_id)
               likes = counts.get(1, 0)
               dislikes = counts.get(0, 0)
               builder = InlineKeyboardBuilder()
               builder.add(InlineKeyboardButton(text=f'За ({likes})', callback_data=f'lynch:{game_id}:{candidate_db_id}:1'))
               builder.add(InlineKeyboardButton(text=f'Против ({dislikes})', callback_data=f'lynch:{game_id}:{candidate_db_id}:0'))
               try:
                   await bot.edit_message_reply_markup(chat_id=lm['chat_id'], message_id=lm['message_id'], reply_markup=builder.as_markup())
               except TelegramBadRequest as e:
                   # ignore "message is not modified" errors
                   if 'message is not modified' in str(e):
                       pass
                   else:
                       logger.exception('Failed to update lynch message counts for %s:%s', lm.get('chat_id'), lm.get('message_id'))
               except Exception:
                   logger.exception('Failed to update lynch message counts for %s:%s', lm.get('chat_id'), lm.get('message_id'))
        except Exception:
           logger.exception('Failed to fetch/update lynch message for live counts')

        # announce in thread (summary of individual vote)
        try:
            game = await get_game_by_id(game_id)
            if game:
                chat_id = game['chat_id']
                thread_id = game['thread_id']
                voter = await get_user_by_dbid(voter_db_id)
                voter_name = voter.get('username') or str(voter.get('tg_id'))
                candidate = await get_user_by_dbid(candidate_db_id)
                candidate_name = candidate.get('username') or str(candidate.get('tg_id'))
                verb = 'За' if choice == 1 else 'Против'
                target_form = 'повешение' if choice == 1 else 'повешения'
                try:
                    voter_mention = f'<a href="tg://user?id={voter.get("tg_id")}">{voter_name}</a>'
                    candidate_mention = f'<a href="tg://user?id={candidate.get("tg_id")}">{candidate_name}</a>'
                    await bot.send_message(chat_id, f'🗳️ Решение: {voter_mention} голосует <b>{verb}</b> {target_form} {candidate_mention}', message_thread_id=thread_id, parse_mode='HTML')
                except Exception:
                    await bot.send_message(chat_id, f'Решение: {voter_name} голосует {verb} {target_form} {candidate_name}', message_thread_id=thread_id)
        except Exception:
            logger.exception('Failed to announce lynch vote')

    # initialize DB
    await init_db()

    # restore lobby timers and running games from DB so restart is resilient
    try:
        now = int(time.time())
        lobbies = await get_games_by_state('lobby')
        for g in lobbies:
            gid = g['id']
            if gid in LOBBY_TASKS:
                continue
            # ensure required fields
            if not g.get('chat_id') or not g.get('thread_id'):
                continue
            LOBBY_TASKS[gid] = asyncio.create_task(lobby_countdown(gid, g['chat_id'], g['thread_id'], bot))
        running = await get_games_by_state('running')
        for g in running:
            try:
                asyncio.create_task(run_game_loop(g['id'], g['chat_id'], g['thread_id'], bot))
            except Exception:
                logger.exception('Failed to restore running game %s', g['id'])
        # restore pending last words so they survive restart
        pending = await get_pending_last_words()
        for lw in pending:
            last_words_pending_tg_ids.add(lw['tg_id'])
    except Exception:
        logger.exception('Failed to restore timers on startup')

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await close_db()

if __name__ == '__main__':
    asyncio.run(main())
