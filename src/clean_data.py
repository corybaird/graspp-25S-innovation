import os
import pandas as pd

dfs = {}
# 1. ファイル名を指定: 産業別、売上高経常利益率別常時従業者数
excel_dir = "downloads"

def clean_labor_number_data(dir=excel_dir):
    """
    Clean the labor number data from Excel files.
    """
    # 1. Get the list of files in the directory
    # 2. Open the workbook and list sheets
    # 3. Read and clean each sheet into a DataFrame
    # 4. Save each DataFrame to a CSV file

    filepaths = [fp for fp in os.listdir(dir) if "産業別、売上高経常利益率別常時従業者数" in fp]
    for file_path in filepaths:
        year = file_path[-8:-4]
        full_path = f"downloads/{file_path}"
        # 2. Open the workbook and list sheets
        xls = pd.ExcelFile(full_path, engine='xlrd')
        print("Available sheets:", xls.sheet_names)
        # 3. Read and clean each sheet into a DataFrame
        for sheet in xls.sheet_names:
            # Adjust `header` and `skiprows` to match where your table's real header lives
            df = pd.read_excel(
                full_path,
                sheet_name=sheet,
                engine='xlrd',
                header=[0, 1],   # e.g. two header rows; change to [0] or None as needed
                skiprows=0       # e.g. if there are extra intro rows, bump this up
            )
            # Drop fully empty rows/columns
            df.dropna(how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)

            # If pandas created a MultiIndex for columns, flatten it
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    "_".join([str(c).strip() for c in col if str(c).strip()])
                    for col in df.columns.values
                ]
            else:
                df.columns = [str(col).strip() for col in df.columns]

            if year == "2004" or year == "2005":
                # Merge rows 2, 3, and 4 into a single string for each column
                merged_headers = df.iloc[1:5].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[5:] 
                # set headers
                df.columns = merged_headers
                df.columns.values[0] = "年度"

                # Insert a new column before column 0
                df.insert(0, "産業", None)

                # Move values from the original column to the new column if "年度" is not in the original column
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), "産業"] = df.iloc[:, 1]

                # Clear the original column where values were moved
                df.loc[~df.iloc[:, 1].str.contains("年度", na=False), df.columns[1]] = None

                # Fill NaN values in column 1 with the value from the first row until another non-NaN value appears in column 0
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                
                # Drop rows where column 1 is NaN
                df = df.dropna(subset=[df.columns[1]])
            elif year == "2007":
                # Merge rows 2, 3, and 4 into a single string for each column
                merged_headers = df.iloc[1:4].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[4:] 
                df.columns = merged_headers
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                # Fill NaN values in column 1 with the value from the first row until another non-NaN value appears in column 0
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                df = df.drop(df.columns[2], axis=1)
            elif year == "2009" or year == "2011" or year == "2012" or year == "2013":
                # Merge rows 2, 3, and 4 into a single string for each column
                merged_headers = df.iloc[0:3].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[3:] 
                df.columns = merged_headers
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                # Fill NaN values in column 1 with the value from the first row until another non-NaN value appears in column 0
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
            elif int(year) >= 2020:
                merged_headers = df.iloc[0:1].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[3:] 
                df.columns = merged_headers
                # drop column 0, 2
                df.columns.values[1] = "産業"
                df.columns.values[3] = "年度"
                df = df.drop(df.columns[0], axis=1)
            else: 
                # Merge rows 2, 3, and 4 into a single string for each column
                merged_headers = df.iloc[2:5].fillna('').astype(str).agg(' '.join, axis=0).str.strip()
                df = df.iloc[5:] 
                # set headers
                df.columns = merged_headers
                
                df.columns.values[0] = "産業"
                df.columns.values[1] = "年度"
                # Fill NaN values in column 1 with the value from the first row until another non-NaN value appears in column 0
                df.iloc[:, 0] = df.iloc[:, 0].ffill()
                
                # Drop rows where column 1 is NaN
                df = df.dropna(subset=[df.columns[1]])
                # drop column 2
                if year == "2003" or year == "2006" or year == "2008": 
                    df = df.drop(df.columns[2], axis=1)
            # strip column 1 if possible
            try:    
                df.iloc[:, 1] = df.iloc[:, 1].str.strip()
            except AttributeError:
                pass
            
            dfs[f"{year}"] = df

    # save dfs to csv
    os.makedirs("data/産業別、売上高経常利益率別常時従業者数", exist_ok=True)
    for key, df in dfs.items():
        df.to_csv(f"data/産業別、売上高経常利益率別常時従業者数/{key}.csv", index=True)

if __name__ == "__main__":
    clean_labor_number_data()
    print("Data cleaning complete.")