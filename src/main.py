import re, yaml
from pathlib import Path
from tqdm import tqdm
from dateutil import tz

from db import get_conn, upsert_source, upsert_article, upsert_mention
from rss_collect import fetch_rss
from api_collect import fetch_newsapi
from sentiment_openai import classify
from mediastack_collect import fetch_mediastack

CFG = yaml.safe_load((Path(__file__).resolve().parent.parent /
                      "config" / "config.yaml").read_text())

SUBJECTS = {
    "Путин":  r"путин\w*|putin",
    "Трамп": r"трамп\w*|trump"
}
PATTERNS = {k: re.compile(v, re.I) for k, v in SUBJECTS.items()}



def detect(text: str):
    text_lc = text.lower()
    found = []
    if "путин"  in text_lc or "putin" in text_lc or "poutine" in text_lc:  found.append("Путин")
    if "трамп" in text_lc or "trump" in text_lc: found.append("Трамп")
    return found


def make_snippet(text: str, kw: str, span=40):
    pos = text.lower().find(kw.lower())
    if pos == -1:
        return text[:2 * span]
    start = max(0, pos - span)
    end   = min(len(text), pos + len(kw) + span)
    return text[start:end].strip()

def main():
    news = [*fetch_rss(), *fetch_mediastack()]
    # ←‑‑‑ 1. ОБЪЯВЛЯЕМ СЧЁТЧИКИ
    inserted_articles = 0
    inserted_mentions = 0

    with get_conn() as conn:
        cur = conn.cursor()
        for item in tqdm(news, desc="Processing"):

            # 2. Фильтруем по ключевым словам
            subjects = detect((item["title"] or "") + " " + (item["summary"] or ""))
            if not subjects:
                continue

            # 3. Вставляем ИСТОЧНИК и СТАТЬЮ
            sid = upsert_source(cur, item["source"], item["stype"])
            aid = upsert_article(cur, sid, item["title"], item["url"],
                                 item["published"], item["summary"])

            # ←‑‑‑ 4. ПРИРАЩАЕМ если действительно добавилась новая статья
            if cur.rowcount:          # rowcount = 1 если INSERT не был проигнорирован
                inserted_articles += 1

            # 5. Вставляем УПОМИНАНИЯ
            for sub in subjects:
                snippet = make_snippet(item["summary"] or item["title"], sub)
                sent = classify(snippet)
                upsert_mention(cur, aid, sub, sent, snippet)

                # ←‑‑‑ 5a. Считаем только новые упоминания
                if cur.rowcount:
                    inserted_mentions += 1

    # ←‑‑‑ 6. Отчёт после завершения цикла
    print(f"Added {inserted_articles} new articles, "
          f"{inserted_mentions} new mentions")

if __name__ == "__main__":
    main()
