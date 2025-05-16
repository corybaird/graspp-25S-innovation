import time
import os
import requests
import re
import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import numpy as np

# Constants for crawl_page part
DOWNLOAD_DIR = "downloads"  # Changed to be relative to project root

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

class BaseCleaner:
    """
    A base class for cleaning data.
    """
    def __init__(self, download_dir):
        self.download_dir = download_dir

    def clean_data(self, target_str: str):
        """
        Placeholder for data cleaning method.
        """
        df_dict = {}
        file_paths = [fp for fp in os.listdir(self.download_dir) if target_str in fp]
        for file_path in file_paths:
            year = int(file_path.split("_")[1])
            if year < 2020:
                df = self.clean_data_before_2020(file_path, year)
            else:
                df = self.clean_data_after_2020(file_path)
            df_dict[year] = df
        return df_dict
    
    def clean_data_before_2020(self, filename, year):
        """
        Placeholder for data cleaning method for years before 2020.
        """
        raise NotImplementedError("This method should be overridden in subclasses.")
    
    def clean_data_after_2020(self, filename):
        """
        Placeholder for data cleaning method for years after 2020.
        """
        raise NotImplementedError("This method should be overridden in subclasses.")

class ResearchExpenseCleaner(BaseCleaner):
    """
    A class to clean and process research expense data.
    """
    def __init__(self, download_dir):
        super().__init__(download_dir)

    def clean_data_before_2020(self, filename, year):
        df = pd.read_excel(os.path.join(self.download_dir, filename), header=1)
        # Drop all empty columns
        df = df.dropna(axis=1, how='all')

        # 結合されたセルとコピーが必要な回数
        merged_headers = {
            "研究開発": 9,
            "研究開発投資": 1,
            "能力開発": 1,
            "研究開発費": 4,
            "委託研究開発費（百万円）": 2,
            "受託研究費（百万円）": 2,
            "うち、関係会社への委託": 1,
            "うち、関係会社からの受託": 1
        }
        if year == 2010 or year == 2013 or year == 2014:
            merged_headers['研究開発'] = 10
        
        if year == 2011 or year == 2012 or year == 2013:
            # Remove rows 0 
            df = df.iloc[1:].reset_index(drop=True)
        else:
            # Remove rows 0 to 2
            df = df.iloc[2:].reset_index(drop=True)
        
        # Remove all whitespaces from the dataframe
        df.replace(to_replace=r'\s+', value='', regex=True, inplace=True)
        
        # Handle merged cells based on merged_headers
        for key, value in merged_headers.items():
            # 5行目まではヘッダー行
            for row in range(5):
                if key in df.iloc[row].values:
                    # 結合された行を取得
                    col_index = df.iloc[row].tolist().index(key)
                    for i in range(value):
                        # 結合されたセルの右側の列に値をコピー
                        # 右側の列が空であれば値をコピー
                        if pd.isna(df.iloc[row, col_index + i + 1]):
                            df.iloc[row, col_index + i + 1] = key
                    break
        
        df.iloc[0,0] = "産業"
        # Process the first five rows to create a single header row
        header_rows = df.iloc[:5].fillna('').astype(str)
        header = header_rows.apply(lambda x: '_'.join(x).replace('__', '_').rstrip('_'), axis=0)

        # Update the dataframe with the new header
        df.columns = header
        df = df.iloc[5:].reset_index(drop=True)

        df = df.iloc[5:].reset_index(drop=True)
        df.replace({'X': np.nan, 'x': np.nan, '-': np.nan}, inplace=True)
        return df   

    # 2020年以降のデータ
    def clean_data_after_2020(self, filename):
        df = pd.read_excel(os.path.join(self.download_dir, filename), header=0)
        # Drop all empty columns
        df = df.dropna(axis=1, how='all')
        # drop columns 0, 1, 3
        df = df.drop(columns=[df.columns[0], df.columns[2], df.columns[3]])
        df.iloc[0, 0] = "産業"
        header_rows = df.iloc[:6].fillna('').astype(str)
        header = header_rows.apply(lambda x: '_'.join(x).replace('__', '_').rstrip('_'), axis=0)
        df.columns = header
        df = df.iloc[5:].reset_index(drop=True)
        df = df.iloc[7:].reset_index(drop=True)
        df.replace({'X': np.nan, 'x': np.nan, '-': np.nan}, inplace=True)
        return df  

class PatentCountCleaner(BaseCleaner):
    """
    A class to clean and process patent count data.
    """
    def __init__(self, download_dir):
        super().__init__(download_dir)

    def clean_data_before_2020(self, filename, year):
        df = pd.read_excel(os.path.join(self.download_dir, filename), header=1)
        # Remove all whitespaces from the dataframe
        df.replace(to_replace=r'\s+', value='', regex=True, inplace=True)
        df = df.copy()
        # 結合されたセルとコピーが必要な回数
        merged_headers = {
            "特許権": 3,
            "実用新案権": 3,
            "意匠権": 3,
            "件数": 2,
            "使用のもの（含供与）": 1
        }
        
        if year < 2011 or year > 2013:
            # remove column 0
            df = df.drop(columns=df.columns[0])
        if year >= 2014:
            df = df.iloc[2:].reset_index(drop=True)
        
        # Remove all whitespaces from the dataframe
        df.replace(to_replace=r'\s+', value='', regex=True, inplace=True)
        
        # Handle merged cells based on merged_headers
        for key, value in merged_headers.items():
            # 6行目まではヘッダー行
            for row in range(6):
                if key in df.iloc[row].values:
                    # 結合された行を取得
                    col_index = df.iloc[row].tolist().index(key)
                    for i in range(value):
                        # 結合されたセルの右側の列に値をコピー
                        # 右側の列が空であれば値をコピー
                        if pd.isna(df.iloc[row, col_index + i + 1]):
                            df.iloc[row, col_index + i + 1] = key
                    break
        
        # Process the first five rows to create a single header row
        df.iloc[0,0] = "産業"
        header_rows = df.iloc[:6].fillna('').astype(str)
        header = header_rows.apply(lambda x: '_'.join(x).replace('__', '_').rstrip('_'), axis=0)

        # Update the dataframe with the new header
        df.columns = header
        df = df.iloc[5:].reset_index(drop=True)

        df = df.iloc[6:].reset_index(drop=True)
        df.replace({'X': np.nan, 'x': np.nan,'Ｘ': np.nan, 'ｘ':np.nan, '***':np.nan, '-': np.nan}, inplace=True)
        return df   

    # 2020年以降の特許データ
    def clean_data_after_2020(self, filename):
        df = pd.read_excel(os.path.join(self.download_dir, filename), header=0)
        # Drop all empty columns
        df = df.dropna(axis=1, how='all')
        # drop columns 0, 1, 3
        df = df.drop(columns=[df.columns[0], df.columns[2], df.columns[3]])
        df.iloc[0,0] = "産業"
        header_rows = df.iloc[:6].fillna('').astype(str)
        header = header_rows.apply(lambda x: '_'.join(x).replace('__', '_').replace('__', '_').rstrip('_'), axis=0)
        header = header.str.replace('特許権_件数_所有数_件', '特許権_件数_所有数', regex=True)

        df.columns = header
        df = df.iloc[5:].reset_index(drop=True)
        df = df.iloc[7:].reset_index(drop=True)
        df.replace({'X': np.nan, 'x': np.nan,'Ｘ': np.nan,'ｘ':np.nan, '***':np.nan, '-': np.nan}, inplace=True)
        return df  
    
class DataCleaner:
    """
    A class to clean and process downloaded EXCEL files.
    """
    def __init__(self, download_dir):
        self.download_dir = download_dir
        self.research_expense_cleaner = ResearchExpenseCleaner(download_dir)
        self.patent_count_cleaner = PatentCountCleaner(download_dir)
        
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
    
    def clean_all_data(self):
        """
        Clean all data from the downloaded files.
        """
        # Call each cleaning function here
        self.clean_labor_number_data()
        # Add other cleaning functions as needed
        self.ResearchExpenseDict = self.research_expense_cleaner.clean_data(target_str="研究開発費及び売上高比率、受託研究費、研究開発投資、能力開発費")
        self.PatentCountDict = self.patent_count_cleaner.clean_data(target_str="産業別、企業数、特許権、実用新案権、意匠権別")
        # Save cleaned data to CSV files
        os.makedirs("data/研究開発費", exist_ok=True)
        for key, df_to_save in self.ResearchExpenseDict.items():
            df_to_save.to_csv(f"data/研究開発費/{key}.csv", index=True)
        os.makedirs("data/特許件数", exist_ok=True)
        for key, df_to_save in self.PatentCountDict.items():
            df_to_save.to_csv(f"data/特許件数/{key}.csv", index=True)
        
    def output_visualization(self, year=2020):
        """
        Output visualization of the cleaned data.
        """
        import matplotlib.pyplot as plt

        from matplotlib.font_manager import FontProperties
        # 自分のパソコンにインストールされている日本語フォントを指定してください
        # For MacOS
        fp = FontProperties(fname = '/Library/Fonts/Arial Unicode.ttf',size = 11)
        plt.rc('font', family=fp.get_name())
        # 1: Top 10 Industries by Total R&D Costs (2020)
        df_2020 = self.ResearchExpenseDict[year]
        df_2020['Total R&D Costs (Million Yen)'] = pd.to_numeric(df_2020['研究開発_研究開発費_計__百万円'], errors='coerce')
        top_rd_costs = df_2020.iloc[2:].nlargest(10, 'Total R&D Costs (Million Yen)')
        plt.figure(figsize=(10, 6))
        plt.bar(top_rd_costs['産業'], top_rd_costs['Total R&D Costs (Million Yen)'], color='skyblue')
        plt.title('Top 10 Industries by Total R&D Costs (2020)', fontsize=14)
        plt.xlabel('Industry', fontsize=12)
        plt.ylabel('R&D Costs (Million Yen)', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        os.makedirs("plots", exist_ok=True)
        plt.savefig(f"plots/top10_rd_costs_{year}.png")
        plt.close()

        # 2: Top 10 Industries by R&D Cost as Percentage of Sales (2020)
        df_2020['R&D Cost as % of Sales'] = pd.to_numeric(df_2020['研究開発_売上高研究開発費比率__％'], errors='coerce')
        top_rd_percentage = df_2020.nlargest(10, 'R&D Cost as % of Sales')
        plt.figure(figsize=(10, 6))
        plt.bar(top_rd_percentage['産業'], top_rd_percentage['R&D Cost as % of Sales'], color='orange')
        plt.title('Top 10 Industries by R&D Cost as % of Sales (2020)', fontsize=14)
        plt.xlabel('Industry', fontsize=12)
        plt.ylabel('R&D Cost as % of Sales', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"plots/top10_rd_costs_percent_{year}.png")
        plt.close()

        # 3: Top 10 Industries by Number of Companies (2020) excluding 合計 and 総合計
        df_2020['Number of Companies'] = pd.to_numeric(df_2020['研究開発_企業数__社'], errors='coerce')
        top_companies = df_2020.iloc[2:].nlargest(10, 'Number of Companies')  # Exclude the first two rows
        plt.figure(figsize=(10, 6))
        plt.bar(top_companies['産業'], top_companies['Number of Companies'], color='green')
        plt.title('Top 10 Industries by Number of Companies (2020)', fontsize=14)
        plt.xlabel('Industry', fontsize=12)
        plt.ylabel('Number of Companies', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"plots/top10_num_companies_{year}.png")
        plt.close()

        # Plot 4: Total patents owned over the years
        years = sorted(self.PatentCountDict.keys())
        years = [yr for yr in years if yr >= 2010]  # Filter years from 2010 onwards
        total_patents = [self.PatentCountDict[yr]['特許権_件数_所有数'].dropna().astype(int).sum() for yr in years]

        print(self.PatentCountDict[year]['特許権_件数_所有数'])
        plt.figure(figsize=(10, 6))
        plt.plot(years, total_patents, marker='o', label='Total Patents Owned')
        plt.title('Total Patents Owned Over the Years')
        plt.xlabel('Year')
        plt.ylabel('Total Patents Owned')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"plots/total_patents_owned_{years[0]}_{years[-1]}.png")
        plt.close()

        # Plot 5: Top 5 industries with the most patents in the latest year
        latest_year = max(years)
        latest_df = self.PatentCountDict[latest_year].iloc[2:]
        top_industries = latest_df[['産業', '特許権_件数_所有数']].dropna()
        top_industries['特許権_件数_所有数'] = top_industries['特許権_件数_所有数'].astype(int)
        top_industries = top_industries.sort_values(by='特許権_件数_所有数', ascending=False).head(5)

        plt.figure(figsize=(10, 6))
        plt.bar(top_industries['産業'], top_industries['特許権_件数_所有数'], color='skyblue')
        plt.title(f'Top 5 Industries with Most Patents in {latest_year}')
        plt.xlabel('Industry')
        plt.ylabel('Number of Patents')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"plots/top5_industries_patents_{latest_year}.png")
        plt.close()

        # Plot 6: Comparison of patents owned vs. used for a specific year
        specific_df = self.PatentCountDict[year].iloc[2:]
        specific_df = specific_df[['産業', '特許権_件数_所有数', '特許権_件数_所有数_使用のもの（含供与）_件']].dropna()
        specific_df['特許権_件数_所有数'] = specific_df['特許権_件数_所有数'].astype(int)
        specific_df['特許権_件数_所有数_使用のもの（含供与）_件'] = specific_df['特許権_件数_所有数_使用のもの（含供与）_件'].astype(int)
        specific_df = specific_df.sort_values(by='特許権_件数_所有数', ascending=False).head(5)

        plt.figure(figsize=(10, 6))
        bar_width = 0.35
        x = range(len(specific_df))
        plt.bar(x, specific_df['特許権_件数_所有数'], width=bar_width, label='Patents Owned', color='orange')
        plt.bar([p + bar_width for p in x], specific_df['特許権_件数_所有数_使用のもの（含供与）_件'], width=bar_width, label='Patents Used', color='green')
        plt.xticks([p + bar_width / 2 for p in x], specific_df['産業'], rotation=45)
        plt.title(f'Patents Owned vs. Used in {year}')
        plt.xlabel('Industry')
        plt.ylabel('Number of Patents')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"plots/patents_owned_vs_used_{year}.png")
        plt.close()

    def clean_labor_number_data(self):
        """
        Clean the labor number data from Excel files.
        """
        dfs = {}
        # 1. Get the list of files in the directory
        # 2. Open the workbook and list sheets
        # 3. Read and clean each sheet into a DataFrame
        # 4. Save each DataFrame to a CSV file

        filepaths = [fp for fp in os.listdir(self.download_dir) if "産業別、売上高経常利益率別常時従業者数" in fp]
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

            full_path = os.path.join(self.download_dir, file_path)
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
#    scraper.run_scraper()
    print("Data scraping complete.")

    # Run data cleaning
    print("\nStarting data cleaning...")
    cleaner = DataCleaner(DOWNLOAD_DIR)
    cleaner.clean_all_data()
    print("Data cleaning complete.")
    cleaner.output_visualization(year=2020)

if __name__ == "__main__":
    main()
