# sentiment_openai.py  –  версия для openai‑python >= 1.0.0
import os, json, hashlib, time, pathlib
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError, APIError, APIConnectionError

load_dotenv()
client = OpenAI() # ключ берётся из переменной окружения

CACHE_DIR  = pathlib.Path("cache"); CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / "sentiment_openai.json"
_cache = json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}

SYSTEM = ("You are a sentiment‑analysis assistant for news in Russian, "
          "English and French. Respond with one word only: "
          "positive, neutral or negative.")

def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def classify(text: str, model="gpt-3.5-turbo-0125") -> str:
    h = _digest(text)
    if h in _cache:
        return _cache[h]

    for attempt in range(5):
        try:
            r = client.chat.completions.create(
                    model=model,
                    temperature=0,
                    max_tokens=1,
                    messages=[
                        {"role": "system", "content": SYSTEM},
                        {"role": "user",   "content": text[:1000]},
                    ]
                )
            label = r.choices[0].message.content.strip().lower()
            if label not in {"positive", "neutral", "negative"}:
                label = "neutral"
            _cache[h] = label
            CACHE_FILE.write_text(json.dumps(_cache))
            return label

        except (RateLimitError, APIConnectionError):
            time.sleep(2 ** attempt)          # экспоненциальный backoff
        except APIError as e:
            print("OpenAI API error:", e)
            break

    return "neutral"
