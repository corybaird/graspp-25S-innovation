import os
import requests
import re
import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# 1. Configuration
years = [i for i in range(2023, 1991, -1)]
BASE_URLs = ["https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001218360&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001206520&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001166746&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001152686&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001141607&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001131164&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001117016&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001105035&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001086216&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079305&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079316&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079315&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001075665&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001045865&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001041347&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001041186&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001023580&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001023590&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079335&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079317&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079296&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079336&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079355&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079337&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079356&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079297&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079298&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079299&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079300&tclass3val=0",
             "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2=000001079357&tclass3val=0"
             ]

DOWNLOAD_DIR = "../downloads"

# Make sure download folder exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def sanitize_filename(text: str) -> str:
    """Turn arbitrary text into a safe filename."""
    # remove leading/trailing whitespace, collapse internal whitespace
    name = re.sub(r"\s+", " ", text.strip())
    # replace forbidden filesystem chars with underscore
    return re.sub(r'[\\/:"*?<>|]+', "_", name)


def scrape_excel_links(page_url: str):
    """
    Returns a list of (download_url, table_name) tuples
    for every EXCEL link on the page.
    """
    resp = requests.get(page_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    # find each EXCEL span
    for span in soup.find_all("span", class_="stat-dl_text"):
        if span.get_text(strip=True) != "EXCEL":
            continue

        # find the enclosing <a href=...> for the EXCEL download
        dl_a = span.find_parent("a", href=True)
        if not dl_a:
            continue
        download_url = urljoin(page_url, dl_a["href"])

        # find the nearest preceding <a class="stat-link_text js-data">
        # which holds the table name
        table_a = span.find_previous("a", class_="stat-link_text stat-dataset_list-detail-item-text js-data")
        if not table_a:
            # fallback: use the href as name
            table_name = download_url.split("/")[-1]
        else:
            table_name = table_a.get_text(separator=" ", strip=True)

        results.append((download_url, table_name))

    return results

def download_file(url: str, table_name: str, folder: str, year: str):
    """Download URL into folder, naming it after table_name + original extension."""
    # pick extension from URL (default to .xlsx)
    ext_match = re.search(r"\.xls[xm]?$", url)
    ext = ext_match.group(0) if ext_match else ".xls"
    safe_name = sanitize_filename(table_name)
    filename = f"{safe_name}_{year}{ext}"
    path = os.path.join(folder, filename)

    if os.path.exists(path):
        print(f"↻ Skipping (exists): {filename}")
        return

    print(f"↓ Downloading {filename}")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(8 * 1024):
            f.write(chunk)

for i, BASE_URL in enumerate(BASE_URLs):
    print(f"\n▶ Scraping {BASE_URL}")
    items = scrape_excel_links(BASE_URL)
    print(f"   → Found {len(items)} EXCEL links")

    for url, table_name in tqdm.tqdm(items):
        # Download each file for all years
        download_file(url, table_name, DOWNLOAD_DIR, str(years[i]))

   