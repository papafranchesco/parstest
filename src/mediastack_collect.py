"""
mediastack_collect.py
Сбор до 1500 новостей (3языка × 5страниц × 100) за <days_back> дней.
Читает ключ только из переменной окружения MEDIASTACK_KEY.

Структура dict идентична другим коллектором:
    {source, stype, url, title, published, summary}
"""

import os, requests
from datetime import datetime, timedelta
from dateutil.parser import parse as pdt
from typing import Dict, Generator

API_KEY = os.getenv("MEDIASTACK_KEY")
if not API_KEY:
    raise RuntimeError("MEDIASTACK_KEY не найден в переменных окружения (.env)")

URL = "http://api.mediastack.com/v1/news"

#конфиг коллектoра
KEYWORDS    = ["Putin","Trump"]
LANGUAGES   = ["en", "fr", "ru"]     # можно расширить
DAYS_BACK   = 150                     # глубина выборки
MAX_PAGES   = 5                      # 5 × 100 = 500 на язык

# --------------------------------------------------------------------------- #
def fetch_mediastack() -> Generator[Dict, None, None]:
    from_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    to_date   = datetime.utcnow().strftime("%Y-%m-%d")
    keywords_q = ",".join(KEYWORDS)

    for lang in LANGUAGES:
        for page in range(1, MAX_PAGES + 1):
            params = {
                "access_key": API_KEY,
                "keywords": keywords_q,
                "languages": lang,
                "date": f"{from_date},{to_date}",
                "limit": 100,
                "offset": (page - 1) * 100,
                "sort": "published_desc",
            }
            try:
                data = requests.get(URL, params=params, timeout=15).json()
            except Exception as e:
                print(f"[WARN] MediaStack {lang} p{page}: {e}")
                break

            articles = data.get("data", [])
            if not articles:
                break

            for art in articles:
                yield {
                    "source":   art.get("source") or "MediaStack",
                    "stype":    "api_ms",
                    "url":      art["url"],
                    "title":    art["title"],
                    "published": pdt(art["published_at"]) if art.get("published_at") else None,
                    "summary":   art.get("description") or "",
                }
