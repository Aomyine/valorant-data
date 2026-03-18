import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

headers = {"User-Agent": "Mozilla/5.0"}
BASE = "https://www.vlr.gg"
CSV_FILE = "vlr_matches_raw.csv"

month_map = {
    "January": "01",
    "February": "02",
    "March": "03",
    "April": "04",
    "May": "05",
    "June": "06",
    "July": "07",
    "August": "08",
    "September": "09",
    "October": "10",
    "November": "11",
    "December": "12"
}

def clean(text):
    return (text or "").strip()

def convert_date(date_str):
    try:
        if "Today" in date_str:
            return datetime.today().strftime("%Y-%m-%d")  # Retorna a data atual
        elif "Yesterday" in date_str:
            return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")  # Retorna o dia anterior
        else:
            return datetime.strptime(date_str, "%a, %B %d, %Y").strftime("%Y-%m-%d")  # Converte para o formato desejado
    except Exception as e:
        print(f"Erro ao converter data: {e}")
        return ""

# Função para capturar os links das partidas
def get_match_links(pages=5, limit=200):

    links = []
    seen = set()

    for page in range(1, pages + 1):

        url = "https://www.vlr.gg/matches/results" if page == 1 else f"https://www.vlr.gg/matches/results/?page={page}"

        print("Coletando:", url)

        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")

        matches = soup.select("a.wf-module-item")

        for m in matches:

            href = m.get("href")

            if href and href.startswith("/"):
                full = BASE + href

                if full not in seen:
                    seen.add(full)
                    links.append(full)

            if len(links) >= limit:
                return links[:limit]

    return links[:limit]

# Função para raspar os dados de uma partida
def scrape_match(match_url):
    r = requests.get(match_url, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")

    match_id = match_url.split("/")[3]

    match_date = extract_date(soup)

    rows = []

    tbodys = soup.select("tbody")[:2]

    for tb in tbodys:

        for tr in tb.select("tr"):

            name = tr.select_one(".text-of")
            team = tr.select_one(".ge-text-light")

            if not name:
                continue

            player = clean(name.get_text())
            team = clean(team.get_text()) if team else ""

            raw_stats = [
                clean(x.get_text())
                for x in tr.select(".mod-stat .mod-both")
            ]

            stats = [
                s for s in raw_stats
                if s and not s.startswith("+") and not s.startswith("-")
            ]

            if len(stats) < 10:
                continue

            rating, acs, k, d, a, kast, adr, hs, fk, fd = stats[:10]

            first_kill_death_diff = 0
            if fk and fd:
                try:
                    first_kill_death_diff = int(fk) - int(fd)
                except ValueError:
                    first_kill_death_diff = 0

            rows.append({
                "match_id": match_id,
                "match_url": match_url,
                "match_date": match_date,
                "player": player,
                "team": team,
                "rating": rating,
                "ACS": acs,
                "Kills": k,
                "Deaths": d,
                "Assists": a,
                "KAST": kast,
                "ADR": adr,
                "HS%": hs,
                "First Kills": fk,
                "First Deaths": fd,
                "First Kill/Death Diff": first_kill_death_diff
            })

    return match_id, rows

# Função principal
def main():

    if os.path.exists(CSV_FILE):
        old_df = pd.read_csv(CSV_FILE)

        existing_ids = set(old_df["match_id"].astype(str).unique())

        print("CSV existente carregado. Partidas já salvas:", len(existing_ids))

    else:
        old_df = pd.DataFrame()
        existing_ids = set()
        print("Nenhum CSV anterior encontrado. Vai criar do zero.")

    match_links = get_match_links(pages=5, limit=200)

    print("Links coletados:", len(match_links))

    new_rows = []
    new_matches = 0

    for i, link in enumerate(match_links, start=1):

        match_id = link.split("/")[3]

        if match_id in existing_ids:
            continue

        print(f"[{i}/{len(match_links)}] Nova partida -> {match_id}")

        try:
            mid, rows = scrape_match(link)

            if not rows:
                continue

            new_rows.extend(rows)
            existing_ids.add(mid)
            new_matches += 1

        except Exception as e:
            print("Erro:", link, e)

        time.sleep(0.4)

        if new_matches >= 200:
            break

    print("Novas partidas adicionadas:", new_matches)

    new_df = pd.DataFrame(new_rows)

    if not old_df.empty and not new_df.empty:
        final_df = pd.concat([old_df, new_df], ignore_index=True)

    elif not old_df.empty:
        final_df = old_df

    else:
        final_df = new_df

    if not final_df.empty:
        final_df["match_id"] = final_df["match_id"].astype(str)

        final_df = final_df.drop_duplicates(subset=["match_id", "player"], keep="first")

    final_df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")

    print("\nCSV atualizado:", CSV_FILE)
    print("Total linhas:", len(final_df))

if __name__ == "__main__":
    main()
