import time
import os
import requests
import re
import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd

# Constants for crawl_page part
DOWNLOAD_DIR = "downloads"  # Changed to be relative to project root

# Constants for clean_data part
EXCEL_DIR_CLEAN = "downloads" # Assuming cleaned files are also in downloads

# Ensure download folder exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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


    def run_scraper(self):
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

def clean_labor_number_data(dir=EXCEL_DIR_CLEAN):
    """
    Clean the labor number data from Excel files.
    """
    dfs = {}
    # 1. Get the list of files in the directory
    # 2. Open the workbook and list sheets
    # 3. Read and clean each sheet into a DataFrame
    # 4. Save each DataFrame to a CSV file

    filepaths = [fp for fp in os.listdir(dir) if "産業別、売上高経常利益率別常時従業者数" in fp]
    for file_path in filepaths:
        year = file_path[-8:-4] # This might need adjustment if timestamp is present
        # Adjust to find year correctly if filename format changed due to timestamp
        match = re.search(r"_(\d{4})_\d{10,}\.xls[xm]?$", file_path)
        if match:
            year = match.group(1)
        else:
            # Fallback or error handling if year cannot be extracted
            print(f"Could not extract year from filename: {file_path}. Skipping.")
            continue

        full_path = os.path.join(dir, file_path)
        # 2. Open the workbook and list sheets
        try:
            xls = pd.ExcelFile(full_path, engine='xlrd')
        except Exception as e:
            print(f"Error opening Excel file {full_path} with xlrd: {e}. Trying openpyxl.")
            try:
                xls = pd.ExcelFile(full_path, engine='openpyxl')
            except Exception as e_opxl:
                print(f"Error opening Excel file {full_path} with openpyxl: {e_opxl}. Skipping.")
                continue

        print("Available sheets:", xls.sheet_names)
        # 3. Read and clean each sheet into a DataFrame
        for sheet in xls.sheet_names:
            try:
                df = pd.read_excel(
                    full_path,
                    sheet_name=sheet,
                    engine='xlrd' if '.xls' == os.path.splitext(full_path)[1].lower() else 'openpyxl',
                    header=[0, 1],
                    skiprows=0
                )
            except Exception as e:
                print(f"Error reading sheet {sheet} from {full_path}: {e}. Skipping sheet.")
                continue
            
            df.dropna(how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    "_".join([str(c).strip() for c in col if str(c).strip()])
                    for col in df.columns.values
                ]
            else:
                df.columns = [str(col).strip() for col in df.columns]

            if year == "2004" or year == "2005":
                merged_headers = df.iloc[1:5].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[5:] 
                df.columns = merged_headers
                df.columns.values[0] = "年度"
                df.insert(0, "産業", None)
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), "産業"] = df.iloc[:, 1]
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), df.columns[1]] = None
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.dropna(subset=[df.columns[1]])
            elif year == "2007":
                merged_headers = df.iloc[1:4].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[4:] 
                df.columns = merged_headers
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.drop(df.columns[2], axis=1)
            elif year == "2009" or year == "2011" or year == "2012" or year == "2013":
                merged_headers = df.iloc[0:3].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[3:] 
                df.columns = merged_headers
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
            elif int(year) >= 2020:
                merged_headers = df.iloc[0:1].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[3:] 
                df.columns = merged_headers
                df.columns.values[1] = "産業"
                df.columns.values[3] = "年度"
                df = df.drop(df.columns[0], axis=1)
            else: 
                merged_headers = df.iloc[2:5].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[5:] 
                df.columns = merged_headers
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.dropna(subset=[df.columns[1]])
                if year == "2003" or year == "2006" or year == "2008": 
                    df = df.drop(df.columns[2], axis=1)
            
            try:    
                df.iloc[:, 1] = df.iloc[:, 1].str.strip()
            except AttributeError:
                pass
            
            dfs[f"{year}"] = df

    os.makedirs("data/産業別、売上高経常利益率別常時従業者数", exist_ok=True)
    for key, df_to_save in dfs.items():
        df_to_save.to_csv(f"data/産業別、売上高経常利益率別常時従業者数/{key}.csv", index=True)

def main():
    # Setup for DataScraper
    years_scrape = [i for i in range(2023, 1991, -1)] # Corrected range
    base_url_scrape = "https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00550100&kikan=00550&tstat=000001010832&cycle=7&tclass1=000001023579&tclass2="
    unique_strings_scrape = [
        "000001218360", "000001206520", "000001166746", "000001152686",
        "000001141607", "000001131164", "000001117016", "000001105035",
        "000001086216", "000001079305", "000001079316", "000001079315",
        "000001075665", "000001045865", "000001041347", "000001041186",
        "000001023580", "000001023590", "000001079335", "000001079317",
        "000001079296", "000001079336", "000001079355", "000001079337",
        "000001079356", "000001079297", "000001079298", "000001079299",
        "000001079300", "000001079357",
    ]
    BASE_URLs_scrape = [f"{base_url_scrape}{unique_str}&tclass3val=0" for unique_str in unique_strings_scrape]

    # Run scraper
    print("Starting data scraping...")
    scraper = DataScraper(BASE_URLs_scrape, DOWNLOAD_DIR, years_scrape)
    scraper.run_scraper()
    print("Data scraping complete.")

    # Run data cleaning
    print("\nStarting data cleaning...")
    clean_labor_number_data()
    print("Data cleaning complete.")

if __name__ == "__main__":
    main()
