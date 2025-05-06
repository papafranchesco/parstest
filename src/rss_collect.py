import feedparser, yaml
from pathlib import Path
from dateutil.parser import parse as dt

CFG = yaml.safe_load((Path(__file__).resolve().parent.parent /
                      "config" / "config.yaml").read_text())



def fetch_rss():
    """Генератор словарей новостей из RSS‑лент."""
    for feed in CFG["rss_feeds"]:
        fp = feedparser.parse(feed["url"])
        for e in fp.entries:
            yield {
                "source": feed["name"],
                "stype": "rss",
                "url": e.link,
                "title": e.title,
                "published": (
                    dt(e.published) if getattr(e, "published", None) else None
                ),
                "summary": getattr(e, "summary", "")
            }
