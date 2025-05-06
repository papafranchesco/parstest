import sqlite3, yaml
from pathlib import Path
from contextlib import contextmanager

BASE_DIR = Path(__file__).resolve().parent.parent      # корень проекта
CFG = yaml.safe_load((BASE_DIR / "config" / "config.yaml").read_text())
DB_PATH = BASE_DIR / CFG.get("database", {}).get("path", "data/news.db")
DB_PATH.parent.mkdir(exist_ok=True)

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")  # включаем FK
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sources (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name     TEXT UNIQUE,
    type     TEXT,
    url      TEXT
);
CREATE TABLE IF NOT EXISTS articles (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id  INTEGER REFERENCES sources(id),
    title      TEXT,
    url        TEXT UNIQUE,
    published  TEXT,
    summary    TEXT
);
CREATE TABLE IF NOT EXISTS mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id      INTEGER REFERENCES articles(id),
    subject         TEXT,
    sentiment       TEXT,
    context_snippet TEXT,
    UNIQUE(article_id, subject)
);
"""

def init_schema():
    with get_conn() as conn:
        conn.executescript(SCHEMA_SQL)
        print("SQLite schema created at", DB_PATH)


# апсерты
def upsert_source(cur, name, stype, url=None):
    cur.execute("""
        INSERT OR IGNORE INTO sources (name, type, url)
        VALUES (?, ?, ?)
    """, (name, stype, url))
    cur.execute("SELECT id FROM sources WHERE name = ?", (name,))
    return cur.fetchone()["id"]


def upsert_article(cur, source_id, title, url, published, summary):
    cur.execute("""
        INSERT OR IGNORE INTO articles
        (source_id, title, url, published, summary)
        VALUES (?, ?, ?, ?, ?)
    """, (source_id, title, url, published, summary))
    cur.execute("SELECT id FROM articles WHERE url = ?", (url,))
    return cur.fetchone()["id"]


def upsert_mention(cur, article_id, subject, sentiment, snippet):
    cur.execute("""
        INSERT OR IGNORE INTO mentions
        (article_id, subject, sentiment, context_snippet)
        VALUES (?, ?, ?, ?)
    """, (article_id, subject, sentiment, snippet))
