from dataclasses import dataclass

@dataclass
class Player:
    user_id: int
    username: str
    role: str = ''
    alive: bool = True

@dataclass
class Game:
    id: int
    thread_id: int
    state: str
    players: list[Player] | None = None
