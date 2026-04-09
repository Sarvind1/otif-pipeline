from imports import *
from run_tat_calculation import main as tat_main
import os
import pandas as pd
from datetime import datetime
import ast
import json

def apply_overrides(df: pd.DataFrame, rules_json_file: str) -> pd.DataFrame:

    # Load rules from JSON
    try:
        with open(rules_json_file, 'r') as f:
            rules_data = json.load(f)
        rules = rules_data.get('rules', [])
    except Exception:
        return df
    
    if "Current Status" not in df.columns:
        return df
    
    # Process each rule
    for rule in rules:
        rule_status = rule.get('status')
        target_col = rule.get('target_column')
        text = rule.get('text')
        conditions = rule.get('conditions', [])
        
        if not all([rule_status, target_col, text]):
            continue
        
        if target_col not in df.columns:
            df[target_col] = ""
        
        # Start with status mask
        mask = df["Current Status"] == rule_status
        
        if mask.sum() == 0:
            continue
        
        # Apply all conditions (AND logic)
        for condition in conditions:
            col = condition.get('col')
            op = condition.get('op')
            val = condition.get('val')
            
            if not col or col not in df.columns:
                mask = pd.Series([False] * len(df), index=df.index)
                break
            
            # Handle NaN values
            is_nan_check = (isinstance(val, str) and val.lower() == 'nan') or (val is None)
            
            try:
                if is_nan_check:
                    if op == '==':
                        cond = pd.isna(df[col])
                    elif op == '!=':
                        cond = pd.notna(df[col])
                    else:
                        mask = pd.Series([False] * len(df), index=df.index)
                        break
                else:
                    if isinstance(val, str):
                        try:
                            val = ast.literal_eval(val)
                        except:
                            pass
                    cond = eval(f'df["{col}"] {op} val', {"df": df, "val": val})
                
                mask = mask & cond
                
            except:
                mask = pd.Series([False] * len(df), index=df.index)
                break
        
        if mask.sum() == 0:
            continue
        
        # Apply updates
        empty_mask = mask & (df[target_col] == "")
        non_empty_mask = mask & (df[target_col] != "") & (df[target_col] != text)
        
        if empty_mask.sum() > 0:
            df.loc[empty_mask, target_col] = text
        
        if non_empty_mask.sum() > 0:
            df.loc[non_empty_mask, target_col] = (
                df.loc[non_empty_mask, target_col] + " | " + text
            )
    
    # Handle negative overrides
    print(f"\n=== PROCESSING NEGATIVE OVERRIDES ===")
    
    if 'Today_Target_delay' not in df.columns:
        print("WARNING: 'Today_Target_delay' column not found")
    else:
        negative_count = 0
        on_track_count = 0
        
        for idx, value in df['Today_Target_delay'].items():
            if idx % 1000 == 0:  # Progress indicator for large datasets
                print(f"Processing row {idx}...")
            
            if isinstance(value, (int, float)) and value <= 0:
                negative_count += 1
                
                # Check if dod_overwrite column exists
                if 'dod_overwrite' not in df.columns:
                    print("Creating 'dod_overwrite' column")
                    df['dod_overwrite'] = ""
                
                # Only set 'On track' if dod_overwrite is empty/NaN
                current_override = df.loc[idx, 'dod_overwrite']
                if pd.isna(current_override) or current_override == "":
                    df.loc[idx, 'dod_overwrite'] = 'On-Track'
                    on_track_count += 1
                    if negative_count <= 5:  # Show first few updates
                        print(f"Row {idx}: Set 'On track' for Today_Target_delay = {value}")
    
    return df





def main(final_df, buffer_mapping, filename="ts_big.xlsx"):
    # print ("Starting DOD calculation...")
    dod_df = tat_main(filen=filename)  # Ensure tat_main returns the path to the generated Excel file
    dod_sql = pd.read_excel('dod_sql_output.xlsx')
    final_df = final_df.merge(dod_sql, how='left', left_on='po_razin_id', right_on='po_razin_id')
    final_df.to_csv('final_df_pre_dod.csv')
    
    buffer_map = dict(zip(buffer_mapping['Stage'], buffer_mapping['Days']))
    override_map = dict(zip(buffer_mapping['Stage'], buffer_mapping['Overrides']))

    # dod = pd.read_excel(f'{dod_df}', sheet_name='Final_Timestamps')
    dod = pd.read_excel(f'{dod_df}', sheet_name='Delay')
    dod_targets = pd.read_excel(f'{dod_df}', sheet_name='Target_Timestamps')
    dod_projected_check = pd.read_excel(f'{dod_df}', sheet_name='status')

    # Create override mask based on override_map
    # mask_override = pd.DataFrame(False, index=dod.index, columns=dod.columns)

    # for stage, override_value in override_map.items():
    #     if override_value == 1 and stage in dod.columns:
    #         mask_override[stage] = True

    mask_not_blank = dod.notna()
    mask_projected = dod_projected_check.applymap(lambda x: str(x).strip() == "Future")
    for stage, override_value in override_map.items():
        if override_value == 1 and stage in dod.columns:
            mask_projected[stage] = False

    mask_to_blank = mask_not_blank & mask_projected


    dod = dod.mask(mask_to_blank, "")

    dod['A. Anti PO Line'] = "Status A"
    dod['B. Compliance Blocked'] = "Status B"
    dod['C. Shipped'] = "Status C"
    dod['D. Master Data Blocker'] = "Status D"

    dod['Current Status'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Current Status']).fillna("")
    dod['Sub Status'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Sub Status']).fillna("")
    dod_targets['Current Status'] = dod_targets['PO_ID'].map(final_df.set_index('po_razin_id')['Current Status']).fillna("")
    dod_targets['Sub Status'] = dod_targets['PO_ID'].map(final_df.set_index('po_razin_id')['Sub Status']).fillna("")


    def xlookup_current_status(row):
        current_status = row["Current Status"]
        sub_status = row["Sub Status"]
        if sub_status in row:
            return row[sub_status]
        elif current_status in row:
            return row[current_status]
        else:
            return ""

    dod['Relevant Timestamp'] = dod.apply(xlookup_current_status, axis=1)
    dod_targets['Relevant Timestamp'] = dod_targets.apply(xlookup_current_status, axis=1)

    today = pd.to_datetime(datetime.today().date())




    def compute_days(row):
        value = row['Relevant Timestamp']
        status = row['Current Status']
        buffer = buffer_map.get(status, 0)
        # print ("Status and Value",status, value
        if value == "" or pd.isna(value):
            return "Missing"
        else:
            try:
                # date_val = pd.to_datetime(value, errors='coerce')
                date_val = value
                if pd.isna(date_val):
                    return None
                # return (today - date_val.normalize()).days - buffer
                return date_val - buffer
            except:
                return None
        
    def compute_days_fallback(row):
        value = row['Relevant Timestamp']
        status = row['Current Status']
        buffer = buffer_map.get(status, 0)
        # print ("Status and Value",status, value
        if value == "" or pd.isna(value):
            return "Missing"
        else:
            try:
                # date_val = pd.to_datetime(value, errors='coerce')
                date_val = value
                if pd.isna(date_val):
                    return None
                print (today, date_val)
                return (today - date_val.normalize()).days
                # return date_val - buffer
            except:
                return None
            
    # print ("in DODs compute_days")


    def categorize_days(x):
        if x == "Missing" or pd.isna(x):
            return "Missing"
        elif isinstance(x, str):  # Handle any other string values
            return x
        else:
            # Now we know x is numeric
            if x <= 0:
                return "On-Track"
            elif x <= 3:
                return "01-03"
            elif x <= 8:
                return "04-08"
            elif x <= 15:
                return "09-15"
            else:
                return "15+"
            
    dod['Delays'] = dod.apply(compute_days, axis=1)
    dod['Today_Target_delay'] = dod_targets.apply(compute_days_fallback, axis=1)        

    dod['Delays Bucket'] = dod['Delays'].apply(categorize_days)
    dod['Today_Target_delay Bucket'] = dod['Today_Target_delay'].apply(categorize_days)
    

    final_df['Delays'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Delays']).fillna("NA")
    final_df['Delays Bucket'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Delays Bucket']).fillna("NA")
    final_df['Today_Target_delay'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Today_Target_delay']).fillna("NA")
    final_df['Today_Target_delay Bucket'] = final_df['po_razin_id'].map(dod.drop_duplicates(subset="PO_ID", keep="first").set_index('PO_ID')['Today_Target_delay Bucket']).fillna("NA")

    final_df.to_csv('final_df_dod.csv')
    final_df = apply_overrides(final_df, "overrides.json")
    final_df["Days"] = final_df["dod_overwrite"].fillna(final_df["Today_Target_delay"])
    final_df["Days Bucket"] = final_df["dod_overwrite"].fillna(final_df["Today_Target_delay Bucket"])

    # Use .replace("", pd.NA) or check for both NaN and empty strings
    final_df["Days"] = final_df["dod_overwrite"].replace("", pd.NA).fillna(final_df["Today_Target_delay"])
    final_df["Days Bucket"] = final_df["dod_overwrite"].replace("", pd.NA).fillna(final_df["Today_Target_delay Bucket"])

    dod['Days'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Days']).fillna(dod['Today_Target_delay'])
    dod['Days Bucket'] = dod['PO_ID'].map(final_df.set_index('po_razin_id')['Days Bucket']).fillna(dod['Today_Target_delay Bucket'])
    
    dod = dod[["PO_ID", "Current Status", "Relevant Timestamp", "Days", "Days Bucket", "Delays", "Delays Bucket", "Today_Target_delay", "Today_Target_delay Bucket"]]
    dod.to_csv('days_bucket.csv')

    return final_df