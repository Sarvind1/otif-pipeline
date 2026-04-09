from imports import *
from ingestion_tables_multithreading import main as ingestion_tables_main
from ingestion_excels import main as ingestion_excels_main
from main import main as cal_main
from dod import main as dod_main
from sharepoint import SharepointClient
from tqdm import tqdm
import contextlib
import io
import logging

def load_creds(path):
    creds = {}
    with open(path, 'r') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                creds[key.strip()] = value.strip()
    return creds


start_time = datetime.now()
print("-" * 60)
print(f"OTIF Pipeline Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 60)

steps = [
    "Load credentials",
    "Ingest tables",
    "Ingest excels",
    "Calculate final dataframe",
    "Calculate DoD view",
    "Upload to SharePoint"
]

for step in tqdm(steps, desc="Overall Progress", unit="step"):
    if step == "Load credentials":
        # print("Current working directory:", os.getcwd())
        # print("creds.txt exists?", os.path.exists('creds.txt'))
        creds = load_creds('creds.txt')

    elif step == "Ingest tables":
        dfs_tables = ingestion_tables_main(creds)

        df = dfs_tables['dod_data']
        df_po = dfs_tables['po_data']

        tz_aware_cols = [col for col in df.columns 
                        if pd.api.types.is_datetime64_any_dtype(df[col]) and df[col].dt.tz is not None]

        for col in tz_aware_cols:
            df[col] = df[col].dt.tz_localize(None)

        df['pi_terms'] = df['supplier_payment_terms'].str.extract(r'(\d+)% PI')[0].astype(float)
        df['pi_applicable'] = df['pi_terms'].apply(lambda x: 1 if x>0 else 0)

        df['ci_terms'] = df['supplier_payment_terms'].str.extract(r'(\d+)% CI')[0].astype(float)
        df['ci_applicable'] = df['ci_terms'].apply(lambda x: 1 if x>0 else 0)

        df['plt'] = df.apply(
            lambda row: (
                max(
                    (pd.to_datetime(row['planned_prd']).date() - pd.to_datetime(row['po_created_date']).date()).days - 15
                    if row['planned_prd'] != "" and pd.notna(row['planned_prd'])
                    else 50,
                    24
                )
            ),
            axis=1
        )

        df_po["po_razin_id"] = df_po["document_number"].astype(str) + df_po["item"].astype(str) + df_po["line_id"].astype(str)
        df['inco'] = df['po_razin_id'].map(df_po.drop_duplicates(subset="po_razin_id", keep="first").set_index('po_razin_id')['incoterms']).fillna("")

        df.to_excel('dod_sql_output.xlsx')

    elif step == "Ingest excels":
        dfs_excels = ingestion_excels_main(creds)
        # continue

    elif step == "Calculate final dataframe":
        final_df = cal_main(dfs_tables, dfs_excels)
        # continue

    elif step == "Calculate DoD view":
        original_level = logging.getLogger().level
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            logging.getLogger().setLevel(logging.CRITICAL + 1)
            final_df_with_dod = dod_main(final_df, dfs_excels['buffer_mapping'])
            logging.getLogger().setLevel(original_level)
        # final_df_with_dod = final_df

    elif step == "Upload to SharePoint":
        root_url = "https://razrgroup.sharepoint.com/sites/Razor"
        library_path = "/sites/Razor/Shared%20Documents/Chetan_Locale/OTIF/Export"
        file_name = "OTIF_DWH_Import_V3.xlsx"

        date_cols = ['date_created', 'first_prd', 'prd', 'planned_prd', 'confirmed_crd', 'quality_control_date']
        number_cols = ['id', 'line_id', 'quantity', 'quantity_fulfilled/received', 'quantity_on_shipments', 'item_rate_eur', 'Pending Units', 'Pending Value']

        sharepoint = SharepointClient(root_url)
        sharepoint.init_session()
        sharepoint.update_sharepoint_excel(
            site_url=root_url,
            library=library_path,
            df=final_df_with_dod,
            file=file_name,
            sheet_name="Data",
            start_cell="A2",
            date_cols=date_cols,
            number_cols=number_cols
        )

end_time = datetime.now()
duration = end_time - start_time
print("-" * 60)
print(f"OTIF Pipeline Completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {duration}")
print("-" * 60)
