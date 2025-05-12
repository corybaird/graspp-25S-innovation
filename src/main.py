#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import requests
import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


class DataScraper:
    def __init__(self, base_urls, download_dir, years):
        self.base_urls = base_urls
        self.download_dir = download_dir
        self.years = years
        os.makedirs(self.download_dir, exist_ok=True)

    @staticmethod
    def _sanitize_filename(text: str) -> str:
        name = re.sub(r"\s+", " ", text.strip())
        return re.sub(r'[\\/:"*?<>|]+', "_", name)

    def _scrape_excel_links(self, page_url: str):
        try:
            resp = requests.get(page_url, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            results = []
            for span in soup.find_all("span", class_="stat-dl_text"):
                if span.get_text(strip=True) != "EXCEL":
                    continue
                dl_a = span.find_parent("a", href=True)
                if not dl_a:
                    continue
                download_url = urljoin(page_url, dl_a["href"])
                table_a = span.find_previous(
                    "a",
                    class_="stat-link_text stat-dataset_list-detail-item-text js-data",
                )
                table_name = (
                    table_a.get_text(separator=" ", strip=True)
                    if table_a
                    else download_url.split("/")[-1]
                )
                results.append((download_url, table_name))
            return results
        except Exception as e:
            print(f"[SCRAPE ERROR] {page_url}: {e}")
            return []

    def _download_file(self, url: str, table_name: str, year: str):
        ext = re.search(r"\.xls[xm]?$", url)
        ext = ext.group(0) if ext else ".xls"
        safe_name = self._sanitize_filename(table_name)
        ts = int(time.time())
        filename = f"{safe_name}_{year}_{ts}{ext}"
        path = os.path.join(self.download_dir, filename)
        try:
            r = requests.get(url, stream=True, timeout=30)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            print(f"✓ {filename}")
        except Exception as e:
            print(f"[DL ERROR] {url}: {e}")

    def run(self):
        for idx, base_url in enumerate(self.base_urls):
            year = str(self.years[idx]) if idx < len(self.years) else ""
            print(f"\n▶ Scraping: {base_url}")
            items = self._scrape_excel_links(base_url)
            print(f"  → {len(items)} links found")
            for url, table_name in tqdm.tqdm(items, unit="file"):
                self._download_file(url, table_name, year)


def clean_labor_number_data(dir_path: str = DOWNLOAD_DIR):
    dfs = {}
    targets = [
        fp
        for fp in os.listdir(dir_path)
        if "産業別、売上高経常利益率別常時従業者数" in fp
    ]
    for file_name in targets:
        year = file_name[-8:-4]
        full_path = os.path.join(dir_path, file_name)
        xls = pd.ExcelFile(full_path, engine="xlrd")
        for sheet in xls.sheet_names:
            df = pd.read_excel(
                full_path,
                sheet_name=sheet,
                engine="xlrd",
                header=[0, 1],
                skiprows=0,
            )
            df.dropna(how="all", inplace=True)
            df.dropna(axis=1, how="all", inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    "_".join(filter(None, map(str, col))).strip() for col in df.columns
                ]
            else:
                df.columns = [str(c).strip() for c in df.columns]
            if year in {"2004", "2005"}:
                merged = df.iloc[1:5].fillna("").astype(str).agg(" ".join, axis=0).str.strip()
                df = df.iloc[5:]
                df.columns = merged
                df.columns.values[0] = "年度"
                df.insert(0, "産業", None)
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), "産業"] = df.iloc[:, 1]
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), df.columns[1]] = None
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.dropna(subset=[df.columns[1]])
            elif year == "2007":
                merged = df.iloc[1:4].fillna("").astype(str).agg(" ".join, axis=0).str.strip()
                df = df.iloc[4:]
                df.columns = merged
                df.columns.values[0:2] = ["産業", "年度"]
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.drop(df.columns[2], axis=1)
            elif year in {"2009", "2011", "2012", "2013"}:
                merged = df.iloc[0:3].fillna("").astype(str).agg(" ".join, axis=0).str.strip()
                df = df.iloc[3:]
                df.columns = merged
                df.columns.values[0:2] = ["産業", "年度"]
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
            elif int(year) >= 2020:
                merged = df.iloc[0:1].fillna("").astype(str).agg(" ".join, axis=0).str.strip()
                df = df.iloc[3:]
                df.columns = merged
                df.columns.values[[1, 3]] = ["産業", "年度"]
                df = df.drop(df.columns[0], axis=1)
            else:
                merged = df.iloc[2:5].fillna("").astype(str).agg(" ".join, axis=0).str.strip()
                df = df.iloc[5:]
                df.columns = merged
                df.columns.values[0:2] = ["産業", "年度"]
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.dropna(subset=[df.columns[1]])
                if year in {"2003", "2006", "2008"}:
                    df = df.drop(df.columns[2], axis=1)
            try:
                df.iloc[:, 1] = df.iloc[:, 1].str.strip()
            except AttributeError:
                pass
            dfs[year] = df
    out_dir = "data/産業別、売上高経常利益率別常時従業者数"
    os.makedirs(out_dir, exist_ok=True)
    for y, df in dfs.items():
        df.to_csv(os.path.join(out_dir, f"{y}.csv"), index=False)
    print(f"✓ Cleaned {len(dfs)} years → {out_dir}/")


def main():
    years = list(range(2023, 1991, -1))
    base_prefix = (
        "https://www.e-stat.go.jp/stat-search/files?"
        "page=1&layout=datalist&toukei=00550100&kikan=00550"
        "&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2="
    )
    unique_strings = [
        "000001218360", "000001206520", "000001166746", "000001152686",
        "000001141607", "000001131164", "000001117016", "000001105035",
        "000001086216", "000001079305", "000001079316", "000001079315",
        "000001075665", "000001045865", "000001041347", "000001041186",
        "000001023580", "000001023590", "000001079335", "000001079317",
        "000001079296", "000001079336", "000001079355", "000001079337",
        "000001079356", "000001079297", "000001079298", "000001079299",
        "000001079300", "000001079357",
    ]
    base_urls = [f"{base_prefix}{u}&tclass3val=0" for u in unique_strings]
    scraper = DataScraper(base_urls, DOWNLOAD_DIR, years)
    scraper.run()
    clean_labor_number_data()


if __name__ == "__main__":
    main()
