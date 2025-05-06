import pandas as pd, matplotlib.pyplot as plt, seaborn as sns
from db import get_conn

def plot_daily():
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT date(substr(published, 1, 10)) AS day,
                   subject,
                   COUNT(*) AS n
            FROM mentions
            JOIN articles ON articles.id = mentions.article_id
            WHERE published IS NOT NULL
            GROUP BY day, subject
            ORDER BY day
        """, conn)

    pivot = (df.pivot(index="day", columns="subject", values="n")
               .fillna(0)
               .astype(int))

    pivot.plot(marker="o", figsize=(10,6), alpha=0.5)
    plt.title("Ежедневные упоминания Путина и Трампа")
    plt.xlabel("Дата"); plt.ylabel("Количество")
    plt.tight_layout(); plt.show()

if __name__ == "__main__":
    plot_daily()
