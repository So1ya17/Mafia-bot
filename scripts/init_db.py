import sys
from pathlib import Path
# Ensure project root is in sys.path so `app` package is importable when running scripts directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncio
from app.db import init_db, close_db

async def _init():
    await init_db()
    await close_db()

if __name__ == '__main__':
    asyncio.run(_init())
    print('DB initialized')
