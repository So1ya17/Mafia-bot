import json
import random
from pathlib import Path

ROLES_PATH = Path(__file__).parent / 'roles.json'
MAX_PLAYERS = 20


def load_roles():
    with open(ROLES_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


_roles_cache = None


def get_all_roles():
    global _roles_cache
    if _roles_cache is None:
        _roles_cache = load_roles()
    return _roles_cache


def compute_mafia_count(n: int) -> int:
    if n <= 5:
        return 1
    if n <= 9:
        return 2
    if n <= 13:
        return 3
    if n <= 16:
        return 4
    return 5


_PRESETS = {
    4: ['Mafia', 'Doctor', 'Vanilla Town', 'Vanilla Town'],
    5: ['Mafia', 'Doctor', 'Vanilla Town', 'Vanilla Town', 'Vanilla Town'],
}


def _role_by_name(name: str):
    for r in get_all_roles():
        if r['name'] == name:
            return r
    return None


def _pick_diverse(pool: list[dict], count: int, n_players: int, already_selected: list[str]) -> list[str]:
    used = {}
    for name in already_selected:
        used[name] = used.get(name, 0) + 1

    eligible = [r for r in pool if r['min_players'] <= n_players]

    picked = []
    for _ in range(count):
        if not eligible:
            break
        under_limit = [
            r for r in eligible
            if used.get(r['name'], 0) < r.get('max_instances', 999)
        ]
        if not under_limit:
            break
        min_picked = min(used.get(r['name'], 0) for r in under_limit)
        candidates = [r for r in under_limit if used.get(r['name'], 0) == min_picked]
        pick = random.choice(candidates)
        picked.append(pick['name'])
        used[pick['name']] = used.get(pick['name'], 0) + 1

    return picked


def _build_mafia_team(count: int, n_players: int) -> list[str]:
    all_roles = get_all_roles()
    roles = []

    if count > 0 and n_players >= 6:
        roles.append('Godfather')
        count -= 1

    special_mafia = [
        r for r in all_roles
        if r['team'] == 'mafia'
        and r['name'] not in ('Godfather', 'Mafia')
        and r['min_players'] <= n_players
    ]
    if n_players >= 10 and count > 0 and special_mafia:
        pick = random.choice(special_mafia)
        roles.append(pick['name'])
        count -= 1
        special_mafia = [r for r in special_mafia if r['name'] != pick['name']]

    if n_players >= 14 and count > 0 and special_mafia:
        pick = random.choice(special_mafia)
        roles.append(pick['name'])
        count -= 1
        special_mafia = [r for r in special_mafia if r['name'] != pick['name']]

    while count > 0:
        roles.append('Mafia')
        count -= 1

    return roles


def generate_roles(n_players: int) -> list[str]:
    if n_players < 4:
        raise ValueError('Minimum 4 players')
    if n_players > MAX_PLAYERS:
        raise ValueError(f'Max supported players is {MAX_PLAYERS}')

    if n_players in _PRESETS:
        return list(_PRESETS[n_players])

    all_roles = get_all_roles()
    roles = []

    # --- Mafia ---
    mafia_count = compute_mafia_count(n_players)
    roles.extend(_build_mafia_team(mafia_count, n_players))

    # --- Mandatory town roles ---
    if n_players >= 6:
        det = _role_by_name('Detective')
        if det and det['min_players'] <= n_players:
            roles.append('Detective')
    doc = _role_by_name('Doctor')
    if doc and doc['min_players'] <= n_players:
        roles.append('Doctor')

    # --- Neutrals ---
    neutral_count = 0
    if n_players <= 8:
        neutral_count = 0
    elif n_players <= 13:
        neutral_count = 1
    else:
        neutral_count = 2

    neutral_pool = [r for r in all_roles if r['team'] == 'neutral']
    roles.extend(_pick_diverse(neutral_pool, neutral_count, n_players, roles))

    # --- Bonus active town roles ---
    bonus_count = 0
    if n_players >= 12:
        bonus_count = 3
    elif n_players >= 10:
        bonus_count = 2
    elif n_players >= 8:
        bonus_count = 1

    mandatory = {'Detective', 'Doctor'}
    active_pool = [
        r for r in all_roles
        if r['team'] == 'town'
        and r['name'] not in mandatory
        and r['name'] != 'Vanilla Town'
    ]
    roles.extend(_pick_diverse(active_pool, bonus_count, n_players, roles))

    # --- Fill with Vanilla Town ---
    while len(roles) < n_players:
        roles.append('Vanilla Town')

    random.shuffle(roles)
    return roles


def assign_roles_to_players(player_list: list, roles: list[str]) -> dict:
    if len(player_list) != len(roles):
        raise ValueError('players and roles length mismatch')
    random.shuffle(roles)
    assignment = {}
    for p, r in zip(player_list, roles):
        assignment[p] = r
    return assignment


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('n', type=int)
    args = p.parse_args()
    print(generate_roles(args.n))
