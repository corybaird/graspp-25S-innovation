import time
import os
import requests
import re
import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin


DOWNLOAD_DIR = "../downloads"

# Make sure download folder exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
import os
import requests
import re
import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class DataScraper:
    """
    A class to scrape and download EXCEL files from specified URLs.
    """
    def __init__(self, base_urls, download_dir, years):
        """
        Initializes the DataScraper with base URLs, download directory, and years.

        Args:
            base_urls (list): A list of base URLs to scrape.
            download_dir (str): The directory to save downloaded files.
            years (list): A list of years to associate with the downloads.
        """
        self.base_urls = base_urls
        self.download_dir = download_dir
        self.years = years
        os.makedirs(self.download_dir, exist_ok=True)

    def sanitize_filename(self, text: str) -> str:
        """
        Turns arbitrary text into a safe filename.

        Args:
            text (str): The text to sanitize.

        Returns:
            str: A safe filename.
        """
        name = re.sub(r"\s+", " ", text.strip())
        return re.sub(r'[\\/:"*?<>|]+', "_", name)

    def scrape_excel_links(self, page_url: str) -> list:
        """
        Scrapes a given URL for EXCEL file links and their associated table names.

        Args:
            page_url (str): The URL of the page to scrape.

        Returns:
            list: A list of tuples, where each tuple contains the download URL and the table name.
        """
        try:
            resp = requests.get(page_url)
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

                table_a = span.find_previous("a", class_="stat-link_text stat-dataset_list-detail-item-text js-data")
                if not table_a:
                    table_name = download_url.split("/")[-1]
                else:
                    table_name = table_a.get_text(separator=" ", strip=True)

                results.append((download_url, table_name))
            return results
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {page_url}: {e}")
            return []
        except Exception as e:
            print(f"Error parsing {page_url}: {e}")
            return []

    def download_file(self, url: str, table_name: str, year: str):
        ext_match = re.search(r"\.xls[xm]?$", url)
        ext = ext_match.group(0) if ext_match else ".xls"
        safe_name = self.sanitize_filename(table_name)
        timestamp = int(time.time())  # Add timestamp to filename
        filename = f"{safe_name}_{year}_{timestamp}{ext}"
        path = os.path.join(self.download_dir, filename)

        print(f"↓ Downloading {filename}")
        try:
            r = requests.get(url, stream=True)
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(8 * 1024):
                    f.write(chunk)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
        except Exception as e:
            print(f"Error saving {filename}: {e}")


    def run(self):
        """
        Executes the scraping and downloading process for all base URLs.
        """
        for i, base_url in enumerate(self.base_urls):
            print(f"\n▶ Scraping {base_url}")
            items = self.scrape_excel_links(base_url)
            print(f"  → Found {len(items)} EXCEL links")

            for url, table_name in tqdm.tqdm(items):
                if i < len(self.years):
                    self.download_file(url, table_name, str(self.years[i]))
                else:
                    print("Warning: More base URLs than years provided. Skipping year association.")
                    self.download_file(url, table_name, "")

years = [i for i in range(2023, 1991, -1)]
base_url = "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2="
unique_strings = [
    "000001218360",
    # "000001206520",
    # "000001166746",
    # "000001152686",
    # "000001141607",
    # "000001131164",
    # "000001117016",
    # "000001105035",
    # "000001086216",
    # "000001079305",
    # "000001079316",
    # "000001079315",
    # "000001075665",
    # "000001045865",
    # "000001041347",
    # "000001041186",
    # "000001023580",
    # "000001023590",
    # "000001079335",
    # "000001079317",
    # "000001079296",
    # "000001079336",
    # "000001079355",
    # "000001079337",
    # "000001079356",
    # "000001079297",
    # "000001079298",
    # "000001079299",
    # "000001079300",
    # "000001079357",
]

BASE_URLs = [f"{base_url}{unique_str}&tclass3val=0" for unique_str in unique_strings]


if __name__ == "__main__":
    scraper = DataScraper(BASE_URLs, DOWNLOAD_DIR, years)
    scraper.run()