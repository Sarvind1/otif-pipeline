from imports import *

def main(dfs_tables, dfs_excels):
    # final_df = cal_main(dfs_tables, dfs_excels)

    po_data = dfs_tables['po_data']
    pl_data = dfs_tables['pl_data']
    batch_data = dfs_tables['batch_data']
    inb_data = dfs_tables['inb_data']
    telex_tableau = dfs_tables['telex_tableau']
    pi_data = dfs_tables['pi_data']
    pi_ns_data = dfs_tables['pi_ns_data']
    supplier_confirmation = dfs_tables['supplier_confirmation']
    master_data = dfs_tables['master_data']
    comp = dfs_tables['compliance_hubspot']
    hs_codes_data = dfs_tables['hs_codes_data'] 

    memo_mapping = dfs_excels['memo_mapping'] ## static
    status_mapping = dfs_excels['status_mapping'] ## static
    blockers_mapping = dfs_excels['blockers_mapping'] ## static
    cm_sm_vendor_mapping = dfs_excels['cm_sm_vendor_mapping'] ## static
    asin_priority_mapping = dfs_excels['asin_priority_mapping'] ## static
    payment_terms_mapping = dfs_excels['payment_terms_mapping'] ## static
    team_priority_mapping = dfs_excels['team_priority_mapping'] ## static
    asin_static_payment_status = dfs_excels['asin_static_payment_status'] ## static
    ffw_status = dfs_excels['ffw_status']
    fob_date = dfs_excels['fob_date']
    spd_blockers = dfs_excels['spd_blockers']
    ffw_blockers = dfs_excels['ffw_blockers']
    telex_supplier = dfs_excels['telex_supplier']
    telex_ffw = dfs_excels['telex_ffw']
    payrun = dfs_excels['payrun']
    packaging_data = dfs_excels['packaging_data']
    transparency_data = dfs_excels['transparency_data']
    transparency_master = dfs_excels['transparency_master']
    prepayment = dfs_excels['prepayment']
    prd = dfs_excels['prd']
    cprd = dfs_excels['cprd']
    g2 = dfs_excels['g2']
    g4 = dfs_excels['g4']
    qc = dfs_excels['qc']
    compliance = dfs_excels['compliance']
    booking_form_data = dfs_excels['booking_form_data']

    po_data["document_number"] = po_data["document_number"].fillna("")
    po_data["asin"] = po_data["asin"].fillna("")
    po_data["item"] = po_data["item"].fillna("")


    final_df = po_data.copy()

    final_df['Vendor ID'] = final_df['po_vendor'].str.split(" ").str[0]
    final_df['Placement Batch'] = final_df['scm_po_scm_memo'].map(memo_mapping.set_index("Memo (Main)")["Summary Filter"]).fillna("Other")
    final_df["MP"] = final_df["marketplace_header"].apply(lambda x: "LATAM" if x in ["CO", "MX", "BR"] else x)

    final_df["Pending Units"] = final_df["quantity"].fillna(0).astype(int) - final_df["quantity_fulfilled/received"].fillna(0).astype(int)

    final_df["Pending Value"] = final_df["item_rate_eur"]*final_df["Pending Units"]

    final_df['Supplier Confirmation VP Check'] = final_df['document_number'].apply(
        lambda x: 'Available on VP' if x in supplier_confirmation['po_number'].values else 'Not Available on VP'
    )

    final_df['prd'] = pd.to_datetime(final_df['prd'], errors='coerce')
    final_df['first_prd'] = pd.to_datetime(final_df['first_prd'], errors='coerce')
    final_df['planned_prd'] = pd.to_datetime(final_df['planned_prd'], errors='coerce')
    final_df['accepted_prd'] = pd.to_datetime(final_df['accepted_prd'], errors='coerce')

    def prd_delay_decision(row):
        if (pd.notna(row['planned_prd']) or row['planned_prd']!="") and (pd.notna(row['first_prd']) and row['first_prd']!="") and row['first_prd'] <= row['planned_prd'] + pd.Timedelta(days=7):
            return "No Delay"
        elif (pd.notna(row['planned_prd']) or row['planned_prd']!="") and (pd.notna(row['prd']) and row['prd']!="") and row['prd'] <= row['planned_prd'] + pd.Timedelta(days=7):
            return "No Delay"
        elif row['prd_status'] in ["Auto-approved", "Approved by IM"] or pd.isna(row['first_prd']):
            return "Agreed"
        elif (pd.notna(row['accepted_prd']) or row['accepted_prd']!="") and (pd.notna(row['prd']) and row['prd']!="") and row['prd'] <= row['accepted_prd'] + pd.Timedelta(days=7):
            return "Agreed"
        else:
            return "SM Action Pending"

    final_df["PRD Delay Decision"] = final_df.apply(prd_delay_decision, axis=1)

    final_df["po_razin"] = final_df["document_number"].astype(str) + final_df["item"].astype(str)
    final_df["po_razin_id"] = final_df["document_number"].astype(str) + final_df["item"].astype(str) + final_df["line_id"].astype(str)
    final_df["razin_mp"] = final_df["item"].astype(str) + final_df["marketplace_header"].astype(str)
    final_df["asin_mp"] = final_df["asin"].astype(str) + final_df["marketplace_header"].astype(str)

    merged_df = pd.concat([
        asin_static_payment_status[['Static PO List', 'Status']].rename(columns={'Static PO List': 'document_number', 'Status': 'status'}),
        pi_ns_data[['po_number', 'status']].rename(columns={'po_number': 'document_number'})
    ], ignore_index=True).drop_duplicates(subset='document_number', keep='first')
    final_df['NS PI Status'] = final_df['document_number'].map(merged_df.set_index("document_number")["status"]).fillna("Not Submitted")

    final_df['VP PI Status'] = final_df['document_number'].map(pi_data[["PO#", "status"]].drop_duplicates(subset="PO#", keep="first").set_index("PO#")["status"]).fillna("03. PI Upload Pending") 

    final_df["PI Payment Status"] = final_df['document_number'].map(payrun[['PO No.', 'Status']].rename(columns={'PO No.': 'document_number'}).drop_duplicates(subset='document_number', keep='first').set_index("document_number")["Status"]).fillna("Not In Payment Sheet") 

    final_df['PI'] = final_df['supplier_payment_terms'].str.extract(r'(\d+)% PI')[0].astype(float)
    final_df['CI'] = final_df['supplier_payment_terms'].str.extract(r'(\d+)% CI')[0].astype(float)
    final_df['BL'] = final_df['supplier_payment_terms'].str.extract(r'(\d+)% BL')[0].astype(float)

    def extract_bl_days(term):
        try:
            match = re.search(r'BL (\d+)', term)
            if match:
                value = int(match.group(1))
                return value if value >= 45 else 0
            return 0
        except:
            return 0

    final_df["BL Days"] = final_df["supplier_payment_terms"].apply(extract_bl_days)

    final_df['Line Payment Type'] = final_df.apply(
        lambda row: "PI" if (row['PI']==100)
        else ("CI" if row['PI']+row['CI']==100 else ("BL" if row['PI']+row['BL']==100 else "BL")),
        axis=1
    )

    def func_batch_payment_type(row, po_data):
        if pd.isna(row['batch_id']) or row['batch_id']=="":
            return row['Line Payment Type']
        else:
            filtered = po_data[po_data['batch_id'] == row['batch_id']]['Line Payment Type']
            if (filtered == 'BL').any():
                return 'BL'
            else:
                return row['Line Payment Type']

    final_df['Batch Payment Type'] = final_df.apply(lambda row: func_batch_payment_type(row, final_df), axis=1)

    final_df['INB#'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'shipment_number']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['shipment_number']).fillna("")

    def func_inb_payment_type(row, po_data):
        if pd.isna(row['INB#']) or row['INB#']=="":
            return row['Line Payment Type']
        else:
            filtered = po_data[po_data['INB#'] == row['INB#']]['Line Payment Type']
            if (filtered == 'BL').any():
                return 'BL'
            else:
                return row['Line Payment Type']
    final_df['INB Payment Type'] = final_df.apply(lambda row: func_inb_payment_type(row, final_df), axis=1)

    final_df['Line Invoice Submission Status'] = final_df.apply(
        lambda row: "Submitted" if (pd.notna(row["invoice_number"]) and row["invoice_number"] != "") or row["BL Days"] != 0 else "Not Submitted",
        axis=1
    )

    def func_batch_invoice_submission_status(row, po_data):
        if pd.isna(row['batch_id']) or row['batch_id']=="":
            return row['Line Invoice Submission Status']
        else:
            filtered = po_data[po_data['batch_id'] == row['batch_id']]['Line Invoice Submission Status']
            if (filtered == 'Not Submitted').any():
                return 'Not Submitted'
            else:
                return row['Line Invoice Submission Status']

    final_df['Batch Invoice Submission Status'] = final_df.apply(lambda row: func_batch_invoice_submission_status(row, final_df), axis=1)

    def func_inb_invoice_submission_status(row, po_data):
        if pd.isna(row['INB#'])  or row['INB#']=="":
            return row['Line Invoice Submission Status']
        else:
            filtered = po_data[po_data['INB#'] == row['INB#']]['Line Invoice Submission Status']
            if (filtered == 'Not Submitted').any():
                return 'Not Submitted'
            else:
                return row['Line Invoice Submission Status']

    final_df['INB Invoice Submission Status'] = final_df.apply(lambda row: func_inb_invoice_submission_status(row, final_df), axis=1)

    final_df['Line Payment Status'] = final_df.apply(
        lambda row: "Paid"
        if row["invoice_status"] == "Bill:Paid In Full"
        or (row["BL Days"] != 0 and row["Line Invoice Submission Status"] == "Submitted")
        else "Not Paid",
        axis=1
    )

    def func_batch_payment_status(row, po_data):
        if pd.isna(row['batch_id'])  or row['batch_id']=="":
            return row['Line Payment Status']
        else:
            filtered = po_data[po_data['batch_id'] == row['batch_id']]['Line Payment Status']
            if (filtered == 'Not Paid').any():
                return 'Not Paid'
            else:
                return row['Line Payment Status']

    final_df['Batch Payment Status'] = final_df.apply(lambda row: func_batch_payment_status(row, final_df), axis=1)

    def func_inb_payment_status(row, po_data):
        if pd.isna(row['INB#']) or row['INB#']=="":
            return row['Line Payment Status']
        else:
            filtered = po_data[po_data['INB#'] == row['INB#']]['Line Payment Status']
            if (filtered == 'Not Paid').any():
                return 'Not Paid'
            else:
                return row['Line Payment Status']

    final_df['INB Payment Status'] = final_df.apply(lambda row: func_inb_payment_status(row, final_df), axis=1)

    final_df['Line Payment Approval Status'] = final_df['invoice_number'].map(payrun[['Inv#', 'Status']].drop_duplicates(subset='Inv#', keep='first').set_index("Inv#")["Status"]).fillna(final_df['Line Payment Status'].apply(lambda x: "Pay" if x=="Paid" else "Not In Payment Sheet"))

    def func_batch_payment_approval_status(row, po_data):
        filtered = po_data[po_data['batch_id'] == row['batch_id']]['Line Payment Approval Status']

        if pd.isna(row['batch_id'])  or row['batch_id']=="":
            return row['Line Payment Approval Status']
        elif (filtered == 'Reject').any():
            return 'Reject'
        elif (filtered == 'Not In Payment Sheet').any():
            return 'Not In Payment Sheet'
        elif (filtered != 'Pay').any():
            return 'On Hold'
        else:
            return 'Pay'

    final_df['Batch Payment Approval Status'] = final_df.apply(lambda row: func_batch_payment_approval_status(row, final_df), axis=1)

    def func_inb_payment_approval_status(row, po_data):
        filtered = po_data[po_data["INB#"] == row["INB#"]]["Line Payment Approval Status"]

        if pd.isna(row["INB#"]) or row['INB#']=="":
            return row["Line Payment Approval Status"]
        elif (filtered == "Reject").any():
            return "Reject"
        elif (filtered == "Not In Payment Sheet").any():
            return "Not In Payment Sheet"
        elif (filtered != "Pay").any():
            return "On Hold"
        else:
            return "Pay"

    final_df["INB Payment Approval Status"] = final_df.apply(
        lambda row: func_inb_payment_approval_status(row, final_df), axis=1
    )

    final_df["HS Code"] = final_df["razin_mp"].map(hs_codes_data[["RAZINxMP", "HS Code Status"]].drop_duplicates(subset="RAZINxMP", keep="first").set_index('RAZINxMP')['HS Code Status']).fillna("HS Code Missing")

    final_df["Batch Sign-Off"] = final_df["batch_id"].map(pl_data.drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["final_status"]).fillna("14a. Documents Missing")

    final_df["QC Stage"] = pd.to_numeric(final_df['quality_control_status'].astype(str).str[0], errors='coerce').fillna(1).astype(int)
    final_df["QC Pending"] = final_df["QC Stage"].apply(lambda x: "No" if x in [3, 6] else "Yes")

    def func_batch_qc_pending(row, po_data):
        batch_id = row['batch_id']
        filtered = po_data[po_data['batch_id'] == batch_id]['QC Pending']

        if pd.isna(batch_id) or batch_id=="" :
            return 'Yes'
        elif (filtered == 'Yes').any():
            return 'Yes'
        else:
            return row['QC Pending']

    final_df["Batch QC Pending"] = final_df.apply(lambda row: func_batch_qc_pending(row, final_df), axis=1)

    final_df['quality_control_date'] = pd.to_datetime(final_df['quality_control_date'], errors='coerce')

    def func_max_qc_date(row, po_data):
        if pd.isna(row['batch_id'])  or row['batch_id']=="":
            return 'No Batch'
        
        qc_dates = po_data.loc[po_data['batch_id'] == row['batch_id'], 'quality_control_date']
        max_date = qc_dates.max()

        if isinstance(max_date, pd.Timestamp):
            return qc_dates.max()
        else:
            return 'Not Scheduled'

    final_df['Max QC Date'] = final_df.apply(lambda row: func_max_qc_date(row, final_df), axis=1)
    # final_df['Max QC Date'] = pd.to_datetime(final_df['Max QC Date'], errors='coerce')
    final_df['Max QC Date'] = final_df['Max QC Date'].apply(
        lambda x: pd.to_datetime(x) if str(x) not in ['No Batch', 'Not Scheduled'] else x
    )

    final_df['Actual pick-up date'] = final_df['batch_id'].map(batch_data.set_index("batch_id")["actual_pickup_date"]).fillna("")
    final_df['Gate In Date'] = final_df['batch_id'].map(batch_data.set_index("batch_id")["gate_in_date"]).fillna("")
    final_df['Actual Shipping Date'] = final_df['batch_id'].map(batch_data.set_index("batch_id")["actual_shipping_date"]).fillna("")

    final_df['Status'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'status']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['status']).fillna("")
    final_df['Actual Pickup'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'actual_cargo_pick_up_date']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['actual_cargo_pick_up_date']).fillna("")
    final_df['Actual Shipping Date3'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'actual_shipping_date']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['actual_shipping_date']).fillna("")
    final_df['Actual Arrival Date'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'actual_arrival_date']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['actual_arrival_date']).fillna("")
    final_df['Actual Delivery Date'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'actual_delivery_date']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['actual_delivery_date']).fillna("")
    final_df['Expected Arrival Date'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'expected_arrival_date']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['expected_arrival_date']).fillna("")

    final_df['Actual pick-up date'] = pd.to_datetime(final_df['Actual pick-up date'], errors='coerce')
    final_df['Actual Shipping Date'] = pd.to_datetime(final_df['Actual Shipping Date'], errors='coerce')
    final_df['Actual Shipping Date3'] = pd.to_datetime(final_df['Actual Shipping Date3'], errors='coerce')
    final_df['Actual Arrival Date'] = pd.to_datetime(final_df['Actual Arrival Date'], errors='coerce')
    final_df['Actual Delivery Date'] = pd.to_datetime(final_df['Actual Delivery Date'], errors='coerce')
    final_df['Expected Arrival Date'] = pd.to_datetime(final_df['Expected Arrival Date'], errors='coerce')

    def func_batch_pickup_status(row):
        def is_non_empty(*fields):
            return any(pd.notna(row.get(field)) and str(row.get(field)).strip() != "" for field in fields)

        pickup_date = row.get('Actual pick-up date')

        if pd.isna(pickup_date) or str(pickup_date).strip() == "":
            if (
                is_non_empty('Gate In Date', 'Actual Shipping Date') or
                is_non_empty('Actual Arrival Date', 'Actual Delivery Date') or
                is_non_empty('Actual Pickup', 'Actual Shipping Date3') or
                row.get('Status') in ["In Transit", "Received", "Partially Received"]
            ):
                return "Picked"
            else:
                return "Not Picked"
        else:
            pickup_date = pd.to_datetime(pickup_date).date()
            return "Not Picked" if pickup_date >= datetime.today().date() else "Picked"

    final_df['Batch Pickup Status'] = final_df.apply(func_batch_pickup_status, axis=1)

    def func_vp_booking_status(row, batch_data):
        if row['Batch Pickup Status'] == 'Picked':
            return 'Booked'
        
        match = batch_data.loc[batch_data['batch_id'] == row['batch_id'], 'Booking Status']
        
        if not match.empty:
            return match.values[0]
        else:
            return 'Not Booked'

    final_df['VP Booking Status'] = final_df.apply(lambda row: func_vp_booking_status(row, batch_data), axis=1)

    final_df["FOB Date"] = final_df["batch_id"].map(batch_data[['batch_id', 'cfs_cut_off']].drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["cfs_cut_off"]).fillna("")
    final_df['FOB Date'] = pd.to_datetime(final_df['FOB Date'], errors='coerce')


    final_df["FOB Status"] = final_df["batch_id"].map(fob_date[['BATCH ID', 'Pickup Status']].drop_duplicates(subset="BATCH ID", keep="first").set_index("BATCH ID")["Pickup Status"]).fillna("") ## not needed anymore

    final_df["Incoterms2"] = final_df["batch_id"].map(batch_data[['batch_id', 'incoterms']].drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["incoterms"]).fillna("")
    final_df["SPD"] = final_df["batch_id"].map(batch_data[['batch_id', 'scr_date']].drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["scr_date"]).fillna("")
    final_df["SPD Delay Reason"] = final_df["batch_id"].map(batch_data[['batch_id', 'scrd_delay_reasons']].drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["scrd_delay_reasons"]).fillna("")

    def func_shipping_status(row):
        def is_non_empty(*fields):
            return any(pd.notna(row.get(f)) and str(row.get(f)).strip() != "" for f in fields)

        if pd.isna(row.get('INB#')) or str(row.get('INB#')).strip() == "":
            return "Not Shipped"
        else:
            if (
                is_non_empty('Actual Shipping Date3') or
                is_non_empty('Actual Arrival Date', 'Actual Delivery Date') or
                is_non_empty('Actual Shipping Date') or
                row.get('Status') in ["In Transit", "Received", "Partially Received"]
            ):
                return "Shipped"
            else:
                return "Not Shipped"

    final_df['Shipping Status'] = final_df.apply(func_shipping_status, axis=1)


    final_df['Substatus'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'substatus']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['substatus']).fillna("")
    final_df['Shipment Method'] = final_df['po_razin_id'].map(inb_data[['PO&RAZIN&ID', 'shipment_method']].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['shipment_method']).fillna("")

    def func_booking_form_status(row, booking_form_df):
        if pd.isna(row['batch_id']) or row['batch_id'] == "":
            return "Not Sent"
        
        if row['Incoterms2'] in ["DDP", "DAP"] or row['Shipment Method'] in ["Truck", "Air"]:
            return "Sent"

        match = booking_form_df.loc[booking_form_df['Batch Id'] == row['batch_id'], 'Status']
        
        if match.empty or match.iloc[0] == "Not Sent":
            return "Not Sent"
        else:
            return "Sent"

    final_df['Booking Form Status'] = final_df.apply(
        lambda row: func_booking_form_status(row, booking_form_data),
        axis=1
    )


    final_df['Gate In Date'] = pd.to_datetime(final_df['Gate In Date'], errors='coerce')
    final_df['Actual pick-up date'] = pd.to_datetime(final_df['Actual pick-up date'], errors='coerce')
    final_df['confirmed_crd'] = pd.to_datetime(final_df['confirmed_crd'], errors='coerce')
    final_df['confirmed_crd'] = pd.to_datetime(final_df['confirmed_crd'], errors='coerce')
    final_df['Actual Pickup'] = pd.to_datetime(final_df['Actual Pickup'], errors='coerce')

    def calculate_estimated_lotif_delivery_date(row):
        if (pd.notna(row['Actual Delivery Date']) and row['Actual Delivery Date']!=""):
            return row['Actual Delivery Date']
        elif (pd.notna(row['Actual Arrival Date']) and row['Actual Arrival Date']!=""):
            return row['Actual Arrival Date'] + timedelta(days=15)
        elif (pd.notna(row['Actual Shipping Date3']) and row['Actual Shipping Date3']!=""):
            return row['Actual Shipping Date3'] + timedelta(days=40)
        elif (pd.notna(row['Actual Pickup']) and row['Actual Pickup']!=""):
            return row['Actual Pickup'] + timedelta(days=50)
        elif (pd.notna(row['Gate In Date']) and row['Gate In Date']!=""):
            return row['Gate In Date'] + timedelta(days=45)
        elif (pd.notna(row['Actual pick-up date']) and row['Actual pick-up date']!=""):
            return row['Actual pick-up date'] + timedelta(days=50)
        elif (pd.notna(row['confirmed_crd']) and row['confirmed_crd']!=""):
            return row['confirmed_crd'] + timedelta(days=45)
        elif (pd.notna(row['planned_prd']) and row['planned_prd']!=""):
            return row['planned_prd'] + timedelta(days=55)
        else:
            return datetime.today() + timedelta(days=100)
        
    final_df['Estimated OTIF Delivery Date'] = final_df.apply(calculate_estimated_lotif_delivery_date, axis=1)
    final_df['Estimated OTIF Delivery Date'] = pd.to_datetime(final_df['Estimated OTIF Delivery Date'], errors='coerce').dt.date

    inb_data_2 = pd.DataFrame()
    inb_data_2['Shipment Number'] = final_df['INB#'].unique()
    inb_data_2['Joey Status'] = inb_data_2['Shipment Number'].map(telex_supplier[['shipment number', 'Final Status']].drop_duplicates(subset="shipment number", keep="first").set_index('shipment number')['Final Status']).fillna("Not Released")
    inb_data_2['Tableau (Supplier)'] = inb_data_2['Shipment Number'].map(telex_tableau[['shipment_number', 'Final Status (Supplier)']].drop_duplicates(subset="shipment_number", keep="first").set_index('shipment_number')['Final Status (Supplier)']).fillna("Not Released")
    inb_data_2['Tableau (FFW)'] = inb_data_2['Shipment Number'].map(telex_tableau[['shipment_number', 'Final Status (FFW)']].drop_duplicates(subset="shipment_number", keep="first").set_index('shipment_number')['Final Status (FFW)']).fillna("Not Released")
    inb_data_2['Muazam Status'] = inb_data_2['Shipment Number'].map(telex_ffw[['Shipment Number', 'Final Status']].drop_duplicates(subset="Shipment Number", keep="first").set_index('Shipment Number')['Final Status']).fillna("Not Released")

    inb_data_2['Final Status (FFW)'] = inb_data_2.apply(lambda row: row['Tableau (FFW)'] if row['Tableau (FFW)']=="Released" else row['Muazam Status'], axis=1)
    inb_data_2['Final Status (SM)'] = inb_data_2.apply(lambda row: row['Final Status (FFW)'] if row['Final Status (FFW)']=="Released" else row['Tableau (Supplier)'], axis=1)
    inb_data_2['Final Status (Supplier)'] = inb_data_2.apply(lambda row: row['Final Status (FFW)'] if row['Final Status (FFW)']=="Released" else row['Final Status (SM)'] if row['Final Status (SM)']=="Released" else row['Joey Status'], axis=1)

    inb_status_map = inb_data_2.set_index('Shipment Number')['Final Status (Supplier)']

    def func_supplier_telex_status(row):
        if row['Substatus'] == "Delivered" or (pd.isna(row['INB#']) or row['INB#'] == ""):
            return "Released"
        else:
            return inb_status_map.get(row['INB#'], None)
        
    final_df['Supplier Telex Status'] = final_df.apply(func_supplier_telex_status, axis=1).fillna("Not in INB Sheet")

    inb_status_map = inb_data_2.set_index('Shipment Number')['Final Status (SM)']

    def func_supplier_telex_status(row):
        if row['Substatus']=="Delivered" or (pd.isna(row['INB#']) or row['INB#']==""):
            return "Released"
        else:
            return inb_status_map.get(row['INB#'], "Not in INB Sheet")
        
    final_df['SM Telex Status'] = final_df.apply(func_supplier_telex_status, axis=1).fillna("Not in INB Sheet")

    inb_map = inb_data_2.set_index('Shipment Number')['Final Status (FFW)'].to_dict()

    def func_supplier_telex_status(row):
        if row['Substatus'] == "Delivered" or (pd.isna(row['INB#']) or row['INB#']==""):
            return "Released"
        else:
            return inb_map.get(row['INB#'], "Not in INB Sheet")

    final_df['FFW Telex Status'] = final_df.apply(func_supplier_telex_status, axis=1).fillna("Not in INB Sheet")

    telex_tableau_map = telex_tableau[["batch_id", "Batch Status"]].drop_duplicates(subset="batch_id", keep="first").set_index('batch_id')['Batch Status']

    def func_batch_telex(row):
        if row['Substatus'] == "Delivered" or (pd.isna(row['batch_id']) or row['batch_id'] == "") or row["FFW Telex Status"]=="Released":
            return "Released"
        else:
            return telex_tableau_map.get(row['batch_id'], "Released")
        
    final_df['Batch Telex'] = final_df.apply(func_batch_telex, axis=1).fillna("Released")

    final_df['Vendor ID'] = pd.to_numeric(final_df['Vendor ID'], errors='coerce').astype('Int64')
    cm_sm_vendor_mapping['Vendor ID'] = pd.to_numeric(cm_sm_vendor_mapping['Vendor ID'], errors='coerce').astype('Int64')

    final_df['CM'] = final_df['Vendor ID'].map(cm_sm_vendor_mapping[['Vendor ID', 'CM']].drop_duplicates(subset="Vendor ID", keep="first").set_index('Vendor ID')['CM']).fillna("")
    final_df['SM'] = final_df['Vendor ID'].map(cm_sm_vendor_mapping[['Vendor ID', 'SM']].drop_duplicates(subset="Vendor ID", keep="first").set_index('Vendor ID')['SM']).fillna("")

    final_df["razin_mp_vendor"] = final_df["item"].astype(str).str.upper() + final_df["marketplace_header"].astype(str) + final_df["Vendor ID"].astype(str)

    ## would be better to fetch this info from a api or table
    comp["RAZIN&MP&Vendor"] = comp["RAZIN&MP&Vendor"].str.strip()
    comp["compliance_status"] = comp["compliance_status"].str.strip()

    comp = comp[(comp["compliance_status"] != "") & (pd.notna(comp["compliance_status"]))]

    final_df['Compliance Status'] = final_df['razin_mp_vendor'].map(comp[["RAZIN&MP&Vendor", "compliance_status"]].drop_duplicates(subset="RAZIN&MP&Vendor", keep="first").set_index('RAZIN&MP&Vendor')['compliance_status']).fillna("Missing")

    batch_compliance_map = final_df.groupby("batch_id")["Compliance Status"].apply(
        lambda x: "Approved" if (x == "Approved").all() else "Pending Approval"
    )

    def get_batch_compliance(pickup_status, batch_id, compliance_status):
        if pickup_status == "Picked":
            return "Approved"
        elif pd.isna(batch_id) or batch_id=="":
            return compliance_status
        else:
            return batch_compliance_map.get(batch_id, "Pending Approval")

    final_df["Batch Compliance"] = final_df.apply(
        lambda row: get_batch_compliance(
            row["Batch Pickup Status"],
            row["batch_id"],
            row["Compliance Status"]
        ),
        axis=1
    )

    final_df['A. Anti PO Line'] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"]=="Picked" else "Yes" if row["considered_for_anti-po"]=="Yes" else "No", axis=1)
    final_df['B. Compliance Blocked'] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"]=="Picked" else "Yes" if row["Compliance Status"] in ["Blocked", "Missing"] else "No", axis=1)

    final_df['C. Shipped'] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"]=="Picked" else "Yes" if row["production_status"]=="Shipped" else "No", axis=1)

    final_df['01. PO Approval Pending'] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"]=="Picked" else "Yes" if row["final_status"] in ["Pending Supervisor Approval", "Rejected By Supervisor"] else "No", axis=1)
    final_df['02. Supplier Confirmation Pending'] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"]=="Picked" else "No" if row["supplier_confirmation_status"]=="Confirmed" else "Yes", axis=1)
    final_df['03. PI Upload Pending'] = final_df.apply(lambda row: 
                                                    "No" if row["Batch Pickup Status"]=="Picked" 
                                                    else "Yes" if (row["NS PI Status"]=="Not Submitted" and row["PI"]!=0 and row["VP PI Status"]=="03. PI Upload Pending")
                                                    else "No", axis=1)
    final_df['04. PI Approval Pending'] = final_df.apply(lambda row:
        "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if (row["PI"] == 0 or (str(row["VP PI Status"])[:2] == "05")) else (
                "Yes" if row["03. PI Upload Pending"] == "Yes" else (
                    "No" if row["NS PI Status"] != "Not Submitted" else "Yes"
                )
            )
        ), axis=1
    )
    final_df['05. PI Payment Pending'] = final_df.apply(lambda row:
        "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if row["04. PI Approval Pending"] == "Yes" else (
                "No" if (row["PI"] == 100 and row["Line Payment Status"] == "Paid") else (
                    "Yes" if (row["NS PI Status"] != "Paid In Full" and row["PI"] != 0) else "No"
                )
            )
        ), axis=1
    )
    packaging_map = packaging_data.drop_duplicates(subset='PORAZIN', keep="first").set_index('PORAZIN')['Final Status'].to_dict()

    def func_packaging_pending(row):
        if row['Batch Pickup Status'] == "Picked":
            return "No"
        else:
            return packaging_map.get(row['po_razin'], "Yes")

    final_df['06. Packaging Pending'] = final_df.apply(func_packaging_pending, axis=1)

    final_df["Transparency Check"] = final_df["asin"].map(transparency_master.drop_duplicates(subset="ASIN", keep="first").set_index("ASIN")["Transparency Check"]).fillna("No")
    final_df["Transparency Pending"] = final_df["po_razin"].map(transparency_data.drop_duplicates(subset="PO&RAZIN", keep="last").set_index("PO&RAZIN")["Transparency Pending"]).fillna("Missing")

    final_df['07. Transparency Label Pending'] = final_df.apply(lambda row:
        "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if (row["Transparency Check"] == "No" or row["Transparency Pending"] == "No") else "Yes"
        ), axis=1
    )

    final_df["08. PRD Pending"] = final_df.apply(lambda row: "No" if row["Batch Pickup Status"] == "Picked" else ("Yes" if pd.isna(row["prd"]) or row["prd"] == "" else "No"), axis=1)

    final_df["09. Under Production"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if row["08. PRD Pending"] == "Yes" else (
                "Yes" if pd.to_datetime(row["prd"], errors="coerce") - pd.Timedelta(days=24) > pd.Timestamp.today() else "No"
            )
        ),
        axis=1
    )
    final_df["10. PRD Confirmation Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if row["09. Under Production"] == "Yes" else (
                "Yes" if (pd.to_datetime(row["prd"], errors="coerce") - pd.Timedelta(days=21) > pd.Timestamp.today()) or (row["prd_reconfirmation"] != "Yes") else "No"
            )
        ),
        axis=1
    )
    final_df["11. IM Sign-Off Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if (row["im_line_signoff"] == "Yes" and row["Batch Compliance"] == "Approved" and row["HS Code"] == "Available") else "Yes"
        ),
        axis=1
    )
    final_df["12. Ready for Batching Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["production_status"] in ["Ready for batching", "Cargo Picked(SM)"] else "Yes"
        ),
        axis=1
    )
    final_df["13. Batch Creation Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if (pd.isna(row["batch_id"]) or row["batch_id"] == "") else "No"
        ),
        axis=1
    )
    final_df["14. SM Sign-Off Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["Batch Sign-Off"] == "Signed-Off" else "Yes"
        ),
        axis=1
    )
    final_df["15. CI Approval Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["Batch Invoice Submission Status"] == "Submitted" else (
                "No" if row["Batch Payment Type"] == "BL" else "Yes"
            )
        ),
        axis=1
    )
    final_df["16. CI Payment Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["Batch Payment Status"] == "Paid" else (
                "No" if row["Batch Payment Type"] == "BL" else "Yes"
            )
        ),
        axis=1
    )

    final_df["17. QC Schedule Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["Batch QC Pending"] == "No" else 
                "Yes" if (
                    (row["Max QC Date"] == "Not Scheduled" or 
                    (isinstance(row["Max QC Date"], pd.Timestamp) and row["Max QC Date"] > (pd.Timestamp.today() - pd.Timedelta(days=1))))
                    or
                    (row["Max QC Date"] == "No Batch" or 
                    (isinstance(row["Max QC Date"], pd.Timestamp) and row["Max QC Date"] > (pd.Timestamp.today() - pd.Timedelta(days=1))))
                ) else "No"
        ),
        axis=1
    )
    final_df["18. FFW Booking Missing"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "No" if row["Incoterms2"] in ["DAP", "DDP"] else (
                "No" if row["VP Booking Status"] == "Booked" else "Yes"
            )
        ),
        axis=1
    )
    final_df["19. Supplier Pickup Date Pending"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if (pd.isna(row["SPD"]) or row["SPD"] == "") or (pd.to_datetime(row["SPD"], errors="coerce") > (pd.Timestamp.today() + pd.Timedelta(days=7))) else "No"
        ),
        axis=1
    )
    final_df["20. Pre Pickup Check"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else (
            "Yes" if row["Batch QC Pending"] == "Yes" else (
                "Yes" if (row["Incoterms2"] != "FOB" and ((pd.isna(row["SPD"]) or row["SPD"]=="") or pd.to_datetime(row["SPD"], errors="coerce") > pd.Timestamp.today())) else (
                    "Yes" if (row["Incoterms2"] == "FOB" and (pd.isna(row["FOB Date"]) or row["FOB Date"]=="")) else "No"
                )
            )
        ),
        axis=1
    )
    final_df["21. FOB Pickup Pending"] = final_df.apply(
        lambda row: "No" if row["Incoterms2"] not in ["FOB", "DAP", "DDP"] else (
            "No" if row["Batch Pickup Status"] == "Picked" else "Yes"
        ),
        axis=1
    )
    final_df["22. Non FOB Pickup Pending"] = final_df["Batch Pickup Status"].apply(
        lambda x: "No" if x == "Picked" else "Yes"
    )
    final_df["23. INB Creation Pending"] = final_df["INB#"].apply(
        lambda x: "Yes" if x == "" or pd.isna(x) else "No"
    )
    final_df["24. Mark In-Transit Pending"] = final_df["Status"].apply(
        lambda x: "Yes" if x == "To Be Shipped" else "No"
    )
    final_df["25. BL Approval Pending"] = final_df.apply(
        lambda row: "No" if row["INB Invoice Submission Status"] == "Submitted" else (
            "No" if row["INB Payment Type"] != "BL" else "Yes"
        ),
        axis=1
    )
    final_df["29. Stock Delivery Pending"] = final_df["Substatus"].apply(
        lambda x: "No" if x == "Delivered" else "Yes"
    )
    final_df["26. BL Payment Pending - In Transit"] = final_df.apply(
        lambda row: "No" if (row["INB Payment Status"] == "Paid" or row["INB Payment Type"] != "BL" or row["29. Stock Delivery Pending"] == "No") else (
            "No" if (isinstance(row["Actual Arrival Date"], pd.Timestamp)) else "Yes"
        ),
        axis=1
    )
    final_df["27. BL Payment Pending - Arrived"] = final_df.apply(
        lambda row: "No" if (row["INB Payment Status"] == "Paid" or row["INB Payment Type"] != "BL" or row["29. Stock Delivery Pending"] == "No") else "Yes",
        axis=1
    )

    # final_df["28. Telex Release Pending"] = final_df.apply(
    #     lambda row: "No" if (row["Shipment Method"] != "Ocean" or row["29. Stock Delivery Pending"] == "No") else (
    #         "Yes" if row["Actual Arrival Date"] == "" else (
    #             "No" if row["FFW Telex Status"] == "Released" else "Yes"
    #         )
    #     ),
    #     axis=1
    # )

    final_df["28. Telex Release Pending"] = final_df.apply(
    lambda row: (
        "No"
        if (row["Shipment Method"] != "Ocean" or
            row["29. Stock Delivery Pending"] == "No" or
            row["Incoterms2"] in ["DDP", "DAP"]
        )
        else (
            "Yes" if (pd.isna(row["Actual Arrival Date"]) or row["Actual Arrival Date"] == "") else ("No" if row["FFW Telex Status"] == "Released" else "Yes")
        )
    ),
    axis=1
    )

    final_df["30. Stock Receiving Pending"] = final_df["Status"].apply(
        lambda x: "No" if x in ["Received", "Partially Received"] else "Yes"
    )
    final_df["31. Dispute - PO Closing Pending"] = final_df["Status"].apply(
        lambda x: "Yes" if x in ["Received", "Partially Received"] else "No"
    )

    final_df["MD Blocker"] = final_df['razin_mp'].map(master_data.set_index('razin_mp')['Action']).fillna('No Blocker')
    final_df["D. Master Data Blocker"] = final_df.apply(
        lambda row: "No" if row["Batch Pickup Status"] == "Picked" else ("No" if row["MD Blocker"] == "No Blocker" else "Yes"),
        axis=1
    )


    pending_columns = [
        'A. Anti PO Line','B. Compliance Blocked','C. Shipped','D. Master Data Blocker','01. PO Approval Pending','02. Supplier Confirmation Pending',
        '03. PI Upload Pending','04. PI Approval Pending','05. PI Payment Pending','06. Packaging Pending','07. Transparency Label Pending','08. PRD Pending',
        '09. Under Production','10. PRD Confirmation Pending','11. IM Sign-Off Pending','12. Ready for Batching Pending','13. Batch Creation Pending',
        '14. SM Sign-Off Pending','15. CI Approval Pending','16. CI Payment Pending','17. QC Schedule Pending','18. FFW Booking Missing',
        '19. Supplier Pickup Date Pending','20. Pre Pickup Check','21. FOB Pickup Pending','22. Non FOB Pickup Pending','23. INB Creation Pending',
        '24. Mark In-Transit Pending','25. BL Approval Pending','26. BL Payment Pending - In Transit','27. BL Payment Pending - Arrived',
        '28. Telex Release Pending','29. Stock Delivery Pending','30. Stock Receiving Pending'
    ]

    def func_current_status(row):
        if row['31. Dispute - PO Closing Pending'] == 'Yes':
            return '31. Dispute - PO Closing Pending'
        for col in pending_columns:
            if row[col] == 'Yes':
                return col
        return None

    final_df['Current Status'] = final_df.apply(func_current_status, axis=1)
    final_df["Status #"] = final_df["Current Status"].str.extract(r'^([^\.]+)')

    final_df["A. Anti PO Line-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "A. Anti PO Line-SS","NA",final_df["Current Status"]
    )
    final_df["B. Compliance Blocked-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "B. Compliance Blocked-SS","NA",final_df["Current Status"]
    )
    final_df["C. Shipped-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "C. Shipped-SS","NA",final_df["Current Status"]
    )
    def func_po_approval_ss(row, ev1_value, g5_value, g6_value):
        if f"{row['Current Status']}-SS" != ev1_value:
            return "NA"
        elif row["final_status"] == "Pending Supervisor Approval":
            return g5_value
        elif row["final_status"] == "Rejected By Supervisor":
            return g6_value
        else:
            return None

    final_df["01. PO Approval Pending-SS"] = final_df.apply(
        lambda row: func_po_approval_ss(row, "01. PO Approval Pending-SS", status_mapping.iloc[4, 1], status_mapping.iloc[5, 1]),
        axis=1
    )
    def func_supplier_confirmation_ss(row, ew1_value, g7_value, g8_value, g9_value):
        if f"{row['Current Status']}-SS" != ew1_value:
            return "NA"
        elif row["Supplier Confirmation VP Check"] == "Not Available on VP":
            return g7_value
        elif row["supplier_confirmation_status"] == "Pending Confirmation":
            return g8_value
        elif row["supplier_confirmation_status"] == "Rejected":
            return g9_value
        else:
            return None

    final_df["02. Supplier Confirmation Pending-SS"] = final_df.apply(
        lambda row: func_supplier_confirmation_ss(row, "02. Supplier Confirmation Pending-SS", status_mapping.iloc[6, 1], status_mapping.iloc[7, 1], status_mapping.iloc[8, 1]),
        axis=1
    )
    final_df["03. PI Upload Pending-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "03. PI Upload Pending-SS","NA",final_df["Current Status"]
    )
    final_df["04. PI Approval Pending-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "04. PI Approval Pending-SS","NA",final_df["VP PI Status"]
    )
    def func_pi_payment_pending_ss(row, ez1_value, g13, g14, g15, g16):
        if f"{row['Current Status']}-SS" != ez1_value:
            return "NA"
        elif row["PI Payment Status"] == "Pay":
            return g13
        elif row["PI Payment Status"] == "On Hold":
            return g14
        elif row["PI Payment Status"] == "Reject":
            return g15
        else:
            return g16

    final_df["05. PI Payment Pending-SS"] = final_df.apply(
        lambda row: func_pi_payment_pending_ss(row, "05. PI Payment Pending-SS", status_mapping.iloc[12, 1], status_mapping.iloc[13, 1], status_mapping.iloc[14, 1], status_mapping.iloc[15, 1]),
        axis=1
    )
    def func_packaging_pending_ss(row, fa1_value, label_df, default_value):
        if f"{row['Current Status']}-SS" != fa1_value:
            return "NA"
        match = label_df.loc[label_df['PORAZIN'] == row['po_razin'], 'Packaging Standard Status']
        return match.values[0] if not match.empty else default_value

    final_df["06. Packaging Pending-SS"] = final_df.apply(
        lambda row: func_packaging_pending_ss(row, "06. Packaging Pending-SS", packaging_data, status_mapping.iloc[17, 1]),
        axis=1
    )
    def func_transparency_label_pending_ss(row, fb1_value, map_g23, map_g24):
        if f"{row['Current Status']}-SS" != fb1_value:
            return "NA"
        return map_g23 if row["Transparency Pending"] == "Missing" else map_g24

    final_df["07. Transparency Label Pending-SS"] = final_df.apply(
        lambda row: func_transparency_label_pending_ss(row, "07. Transparency Label Pending-SS", status_mapping.iloc[22, 1], status_mapping.iloc[24, 1]), axis=1
    )
    final_df["08. PRD Pending-SS"] = np.where(
        final_df["Current Status"] + "-SS" != "08. PRD Pending-SS","NA", final_df["Current Status"]
    )

    def func_prd_confirmation_pending_ss(row, fd1_value, map_g26, map_g27, map_g28):
        if f"{row['Current Status']}-SS" != fd1_value:
            return "NA"
        if row['PRD Delay Decision'] == "No Delay":
            return map_g26
        if row['PRD Delay Decision'] == "SM Action Pending":
            return map_g27
        else:
            return map_g28

    final_df["09. Under Production-SS"] = final_df.apply(
        lambda row: func_prd_confirmation_pending_ss(row, "09. Under Production-SS", status_mapping.iloc[25, 1], status_mapping.iloc[26, 1], status_mapping.iloc[27, 1]
        ),
        axis=1
    )
    def func_prd_confirmation_pending_ss(row, fe1_value, map_g30, map_g31):
        if f"{row['Current Status']}-SS" != fe1_value:
            return "NA"
        return map_g30 if row.get("prd_reconfirmation") != "Yes" else map_g31

    final_df["10. PRD Confirmation Pending-SS"] = final_df.apply(
        lambda row: func_prd_confirmation_pending_ss(
            row, "10. PRD Confirmation Pending-SS",
            status_mapping.iloc[28, 1], status_mapping.iloc[29, 1]
        ),
        axis=1
    )
    def func_im_signoff_pending_ss(row, ff1_value, map_g32, map_g33, map_g34):
        if f"{row['Current Status']}-SS" != ff1_value:
            return "NA"
        elif row.get("Batch Compliance") != "Approved":
            return map_g32
        elif row.get("HS Code") == "HS Code Missing":
            return map_g33
        else:
            return map_g34

    final_df["11. IM Sign-Off Pending-SS"] = final_df.apply(
        lambda row: func_im_signoff_pending_ss(
            row, "11. IM Sign-Off Pending-SS",
            status_mapping.iloc[30, 1], status_mapping.iloc[31, 1], status_mapping.iloc[32, 1] 
        ),
        axis=1
    )

    final_df["12. Ready for Batching Pending-SS"] = final_df.apply(
        lambda row: row["Current Status"] if f"{row['Current Status']}-SS" == "12. Ready for Batching Pending-SS" else "NA",
        axis=1
    )
    def func_batch_creation_pending_ss(row, fh1_value, map_g35, map_g36):
        if f"{row['Current Status']}-SS" != fh1_value:
            return "NA"
        if row["wh_type"] == "AMZ":
            return map_g35
        return map_g36

    final_df["13. Batch Creation Pending-SS"] = final_df.apply(
        lambda row: func_batch_creation_pending_ss(
            row,
            "13. Batch Creation Pending-SS",
            status_mapping.iloc[34, 1],
            status_mapping.iloc[35, 1]
        ),
        axis=1
    )
    final_df["14. SM Sign-Off Pending-SS"] = final_df.apply(
        lambda row: row["Batch Sign-Off"] if f"{row['Current Status']}-SS" == "14. SM Sign-Off Pending-SS" else "NA",
        axis=1
    )
    final_df["15. CI Approval Pending-SS"] = final_df.apply(
        lambda row: row["Current Status"] if f"{row['Current Status']}-SS" == "15. CI Approval Pending-SS" else "NA",
        axis=1
    )
    def func_ci_payment_pending_ss(row, fj1_value, map_g41, map_g42, map_g43, map_g44):
        if f"{row['Current Status']}-SS" != fj1_value:
            return "NA"
        if row["Batch Payment Approval Status"] == "Pay":
            return map_g41
        if row["Batch Payment Approval Status"] == "On Hold":
            return map_g42
        if row["Batch Payment Approval Status"] == "Reject":
            return map_g43
        if row["Batch Payment Approval Status"] == "Not In Payment Sheet":
            return map_g44
        return None

    final_df["16. CI Payment Pending-SS"] = final_df.apply(
        lambda row: func_ci_payment_pending_ss(
            row,
            "16. CI Payment Pending-SS",
            status_mapping.iloc[40, 1],
            status_mapping.iloc[41, 1],
            status_mapping.iloc[42, 1],
            status_mapping.iloc[43, 1]
        ),
        axis=1
    )
    def func_qc_schedule_pending_ss(row, fl1_value, map_g45, map_g46):
        if f"{row['Current Status']}-SS" != fl1_value:
            return "NA"
        if row["Max QC Date"] == "Not Scheduled":
            return map_g45
        return map_g46

    final_df["17. QC Schedule Pending-SS"] = final_df.apply(
        lambda row: func_qc_schedule_pending_ss(
            row,
            "17. QC Schedule Pending-SS",
            status_mapping.iloc[44, 1],
            status_mapping.iloc[45, 1]
        ),
        axis=1
    )
    def func_ffw_booking_missing_ss(row, fm1_value):
        if f"{row['Current Status']}-SS" != fm1_value:
            return "NA"
        return row["Current Status"]

    final_df["18. FFW Booking Missing-SS"] = final_df.apply(
        lambda row: func_ffw_booking_missing_ss(
            row,
            "18. FFW Booking Missing-SS"
        ),
        axis=1
    )

    # def func_supplier_pickup_date_pending_ss(row, fn1_value, map_g48, map_g49, map_g50):
    #     if f"{row['Current Status']}-SS" != fn1_value:
    #         return "NA"
    #     if row["SPD"] == "":
    #         if row["L2 SPD"] in ["Not in SPD Sheet", "No SPD Blocker Mentioned"]:
    #             return map_g48
    #         else:
    #             return map_g50
    #     return map_g49

    final_df['L2 SPD'] = final_df['batch_id'].map(spd_blockers.drop_duplicates(subset="batch_id", keep="first").set_index('batch_id')['Final Status']).fillna("Not in SPD Sheet")

    def func_supplier_pickup_date_pending_ss(row, fn1_value, map_g48_val, map_g49_val, map_g50_val, map_g51_val):
        if f"{row['Current Status']}-SS" != fn1_value:
            return "NA"
        else:
            if (pd.isna(row["SPD"]) or row["SPD"] == "") and row["Booking Form Status"] == "Not Sent":
                return map_g48_val
            elif (pd.isna(row["SPD"]) or row["SPD"] == "") and (row["L2 SPD"] == "Not in SPD Sheet" or row["L2 SPD"] == "No SPD Blocker Mentioned"):
                return map_g49_val
            elif (pd.isna(row["SPD"]) or row["SPD"] == "") and (row["L2 SPD"] != "Not in SPD Sheet" or row["L2 SPD"] != "No SPD Blocker Mentioned"):
                return map_g51_val
            else:
                return map_g50_val

    final_df["19. Supplier Pickup Date Pending-SS"] = final_df.apply(
        lambda row: func_supplier_pickup_date_pending_ss(
            row,
            "19. Supplier Pickup Date Pending-SS",
            status_mapping.iloc[47, 1],
            status_mapping.iloc[48, 1],
            status_mapping.iloc[49, 1],
            status_mapping.iloc[50, 1]
        ),
        axis=1
    )

    def func_pre_pickup_check_ss(row, fo1_value, map_g51, map_g52, map_g53):
        if f"{row['Current Status']}-SS" != fo1_value:
            return "NA"
        if row["Batch QC Pending"] == "Yes":
            return map_g51
        if row["Incoterms2"] == "FOB" and (pd.isna(row["FOB Date"]) or row["FOB Date"] == ""):
            return map_g52
        return map_g53

    final_df["20. Pre Pickup Check-SS"] = final_df.apply(
        lambda row: func_pre_pickup_check_ss(
            row,
            "20. Pre Pickup Check-SS",
            status_mapping.iloc[51, 1],
            status_mapping.iloc[52, 1],
            status_mapping.iloc[53, 1]
        ),
        axis=1
    )

    def func_fob_pickup_pending_ss(row, fp1_value, map_g54, map_g55, map_g56):
        if f"{row['Current Status']}-SS" != fp1_value:
            return "NA"
        if row["Incoterms2"] in ["DDP", "DAP"]:
            return map_g54
        if (pd.notna(row['FOB Date'])  and row['FOB Date']!=""):
            if row["FOB Date"].date() < pd.Timestamp.today().date():
                return map_g54
            elif row["FOB Date"].date() < (pd.Timestamp.today() + pd.Timedelta(days=2)).date():
                return map_g55
        return map_g56

    # final_df['FOB Date'] = pd.to_datetime(final_df['FOB Date'], errors='coerce').dt.date

    final_df["21. FOB Pickup Pending-SS"] = final_df.apply(
        lambda row: func_fob_pickup_pending_ss(
            row,
            "21. FOB Pickup Pending-SS",
            status_mapping.iloc[54, 1], 
            status_mapping.iloc[55, 1], 
            status_mapping.iloc[56, 1] 
        ),
        axis=1
    )

    final_df['Vendor ID'] = pd.to_numeric(final_df['Vendor ID'], errors='coerce').astype('Int64')
    cm_sm_vendor_mapping['Vendor ID'] = pd.to_numeric(cm_sm_vendor_mapping['Vendor ID'], errors='coerce').astype('Int64')
    vendor_mapping = cm_sm_vendor_mapping.drop_duplicates(subset='Vendor ID', keep='first').set_index('Vendor ID')

    final_df['Team'] = final_df['Vendor ID'].map(vendor_mapping['Team'])


    final_df['Reporting Status'] = final_df['Current Status'].map(status_mapping[['Status', 'Reporting Status']].drop_duplicates(subset="Status", keep="first").set_index('Status')['Reporting Status']).fillna("")

    final_df['L2 Compliance'] = final_df['po_razin_id'].map(compliance[["PO&RAZIN&ID", "Final Status"]].drop_duplicates(subset="PO&RAZIN&ID", keep="first").set_index('PO&RAZIN&ID')['Final Status']).fillna("Not in Compliance Sheet")
    final_df['L2 PI'] = final_df['document_number'].map(prepayment[["document number", "Final Status"]].drop_duplicates(subset="document number", keep="first").set_index('document number')['Final Status']).fillna("Not in PI Sheet")
    final_df['L2 PRD'] = final_df['po_razin_id'].map(prd[["otif_id", "Final Status"]].drop_duplicates(subset="otif_id", keep="first").set_index('otif_id')['Final Status']).fillna("Not in PRD Sheet")
    final_df['L2 CPRD'] = final_df['po_razin_id'].map(cprd[["po_razin_id", "Final Status"]].drop_duplicates(subset="po_razin_id", keep="first").set_index('po_razin_id')['Final Status']).fillna("Not in CPRD Sheet")
    final_df['L2 G2'] = final_df['po_razin_id'].map(g2[["otif_id", "Final Status"]].drop_duplicates(subset="otif_id", keep="first").set_index('otif_id')['Final Status']).fillna("Not in G2 Sheet")
    final_df['L2 G4'] = final_df['batch_id'].map(g4[["batch_id", "Final Status"]].drop_duplicates(subset="batch_id", keep="first").set_index('batch_id')['Final Status']).fillna("Not in G4 Sheet")
    final_df['L2 QC'] = final_df['po_razin_id'].map(qc[["PO RAZIN ID", "Final Status2"]].drop_duplicates(subset="PO RAZIN ID", keep="first").set_index('PO RAZIN ID')['Final Status2']).fillna("Not in QC Sheet")
    # final_df['L2 SPD'] = final_df['batch_id'].map(spd_blockers.drop_duplicates(subset="batch_id", keep="first").set_index('batch_id')['Final Status']).fillna("Not in SPD Sheet")
    final_df['L2 Pickup'] = final_df['batch_id'].map(ffw_status[["Batch ID", "Final Blocker Reason"]].drop_duplicates(subset="Batch ID", keep="first").set_index('Batch ID')['Final Blocker Reason']).fillna("Not in FFW Sheet")

    ffw_blockers_map = ffw_blockers.drop_duplicates(subset="Batch ID", keep="first").set_index("Batch ID")["Final Status"].to_dict()
    def func_pickup_blocker(batch_id, status_hash):
        if batch_id == "":
            return "No"
        if status_hash in ["19", "20", "21", "22"]:
            status = ffw_blockers_map.get(batch_id, "No")
            return "No" if status == "Yes" else status
        return "No"
    final_df["Pickup Blocker"] = final_df.apply(
        lambda row: func_pickup_blocker(row["batch_id"],row["Status #"]),
        axis=1
    )


    def func_non_fob_pickup_pending_ss(row, fq1_value, map_g57, map_g58, map_g59):
        if f"{row['Current Status']}-SS" != fq1_value:
            return "NA"
        if (pd.notna(row["Actual pick-up date"]) and row["Actual pick-up date"] != ""):
            return map_g57
        if row["Pickup Blocker"] != "No":
            return map_g58
        return map_g59

    final_df["22. Non FOB Pickup Pending-SS"] = final_df.apply(
        lambda row: func_non_fob_pickup_pending_ss(
            row,
            "22. Non FOB Pickup Pending-SS",
            status_mapping.iloc[57, 1],
            status_mapping.iloc[58, 1],
            status_mapping.iloc[59, 1]
        ),
        axis=1
    )

    def func_inb_creation_pending_ss(current_status, fr1_value, map_g60, map_g61, gate_in_date):
        if f"{current_status}-SS" != fr1_value:
            return "NA"
        return map_g60 if (pd.isna(gate_in_date) or gate_in_date == "") else map_g61

    final_df["23. INB Creation Pending-SS"] = final_df.apply(
        lambda row: func_inb_creation_pending_ss(
            row["Current Status"],
            "23. INB Creation Pending-SS",
            status_mapping.iloc[60, 1],  # Map!G$60
            status_mapping.iloc[61, 1],  # Map!G$61
            row["Gate In Date"]
        ),
        axis=1
    )

    def func_mark_in_transit_pending_ss(current_status, fr1_value, map_g62, map_g63, shipping_status):
        if f"{current_status}-SS" != fr1_value:
            return "NA"
        return map_g62 if shipping_status == "Not Shipped" else map_g63

    final_df["24. Mark In-Transit Pending-SS"] = final_df.apply(
        lambda row: func_mark_in_transit_pending_ss(
            row["Current Status"],
            "24. Mark In-Transit Pending-SS",
            status_mapping.iloc[62, 1],
            status_mapping.iloc[63, 1],
            row["Shipping Status"]
        ),
        axis=1
    )

    def func_bl_approval_pending_ss(current_status, ft1_value, map_g64, map_g65, actual_shipping_date3):
        if f"{current_status}-SS" != ft1_value:
            return "NA"
        return map_g64 if actual_shipping_date3 > (pd.Timestamp.today() - pd.Timedelta(days=5)) else map_g65

    final_df["25. BL Approval Pending-SS"] = final_df.apply(
        lambda row: func_bl_approval_pending_ss(
            row["Current Status"],
            "25. BL Approval Pending-SS",
            status_mapping.iloc[64, 1], 
            status_mapping.iloc[65, 1],
            row["Actual Shipping Date3"]
        ),
        axis=1
    )
    def func_bl_payment_pending_in_transit_ss(current_status, fu1_value, map_g66, map_g67, map_g68, map_g69, inb_payment_approval_status):
        if f"{current_status}-SS" != fu1_value:
            return "NA"
        if inb_payment_approval_status == "Pay":
            return map_g66
        elif inb_payment_approval_status == "On Hold":
            return map_g67
        elif inb_payment_approval_status == "Reject":
            return map_g68
        elif inb_payment_approval_status == "Not In Payment Sheet":
            return map_g69
        return ""

    final_df["26. BL Payment Pending - In Transit-SS"] = final_df.apply(
        lambda row: func_bl_payment_pending_in_transit_ss(
            row["Current Status"],
            "26. BL Payment Pending - In Transit-SS",
            status_mapping.iloc[66, 1],  # Map!G$66
            status_mapping.iloc[67, 1],  # Map!G$67
            status_mapping.iloc[68, 1],  # Map!G$68
            status_mapping.iloc[69, 1],  # Map!G$69
            row["INB Payment Approval Status"]
        ),
        axis=1
    )
    def func_bl_payment_pending_arrived_ss(current_status, fv1_value, map_g70, map_g71, map_g72, map_g73, inb_payment_approval_status):
        if f"{current_status}-SS" != fv1_value:
            return "NA"
        if inb_payment_approval_status == "Pay":
            return map_g70
        elif inb_payment_approval_status == "On Hold":
            return map_g71
        elif inb_payment_approval_status == "Reject":
            return map_g72
        elif inb_payment_approval_status == "Not In Payment Sheet":
            return map_g73
        return ""

    final_df["27. BL Payment Pending - Arrived-SS"] = final_df.apply(
        lambda row: func_bl_payment_pending_arrived_ss(
            row["Current Status"],
            "27. BL Payment Pending - Arrived-SS",
            status_mapping.iloc[70, 1],  # Map!G$70
            status_mapping.iloc[71, 1],  # Map!G$71
            status_mapping.iloc[72, 1],  # Map!G$72
            status_mapping.iloc[73, 1],  # Map!G$73
            row["INB Payment Approval Status"]
        ),
        axis=1
    )
    def func_telex_release_pending_ss(current_status, fw1_value, g74, g75, g76, g77, g78, g79, g80, cv, cy, cz, da):
        if f"{current_status}-SS" != fw1_value:
            return "NA"
        if (pd.isna(cv) or cv == ""):
            if cy == "Not Released":
                return g74
            if cz == "Not Released":
                return g75
            if da == "Not Released":
                return g76
        else:
            if cy == "Not Released":
                return g77
            if cz == "Not Released":
                return g78
            if da == "Not Released":
                return g79
        return g80

    final_df["28. Telex Release Pending-SS"] = final_df.apply(
        lambda row: func_telex_release_pending_ss(
            row["Current Status"],
            "28. Telex Release Pending-SS",
            status_mapping.iloc[74, 1],  # Map!G$74
            status_mapping.iloc[75, 1],  # Map!G$75
            status_mapping.iloc[76, 1],  # Map!G$76
            status_mapping.iloc[77, 1],  # Map!G$77
            status_mapping.iloc[78, 1],  # Map!G$78
            status_mapping.iloc[79, 1],  # Map!G$79
            status_mapping.iloc[80, 1],  # Map!G$80
            row["Actual Arrival Date"],
            row["Supplier Telex Status"],
            row["SM Telex Status"],
            row["FFW Telex Status"]
        ),
        axis=1
    )
    # def func_stock_delivery_pending_ss(current_status, fx1_value, map_g81, map_g82, map_g83, wh_type):
    #     if f"{current_status}-SS" != fx1_value:
    #         return "NA"
    #     return map_g81 if wh_type == "AMZ" else map_g82

    def func_stock_delivery_pending_ss(row, fx1_value, map_g82_val, map_g83_val, map_g84_val):
        if f"{row['Current Status']}-SS" != fx1_value:
            return "NA"
        else:
            if row["Incoterms2"] == "DDP":
                return map_g84_val
            elif row["wh_type"] == "AMZ":
                return map_g82_val
            else:
                return map_g83_val

    final_df["29. Stock Delivery Pending-SS"] = final_df.apply(
        lambda row: func_stock_delivery_pending_ss(
            row,
            "29. Stock Delivery Pending-SS",
            status_mapping.iloc[81, 1],
            status_mapping.iloc[82, 1],
            status_mapping.iloc[83, 1]
        ),
        axis=1
    )

    def func_stock_receiving_pending_ss(current_status, fx1_value, map_g83, map_g84, wh_type):
        if f"{current_status}-SS" != fx1_value:
            return "NA"
        return map_g83 if wh_type == "AMZ" else map_g84

    final_df["30. Stock Receiving Pending-SS"] = final_df.apply(
        lambda row: func_stock_receiving_pending_ss(
            row["Current Status"],
            "30. Stock Receiving Pending-SS",
            status_mapping.iloc[84, 1],
            status_mapping.iloc[85, 1],
            row["wh_type"]
        ),
        axis=1
    )

    def func_po_closing_pending_ss(current_status, fx1_value, map_g85, map_g86, wh_type):
        if f"{current_status}-SS" != fx1_value:
            return "NA"
        return map_g85 if wh_type == "AMZ" else map_g86

    final_df["31. Dispute - PO Closing Pending-SS"] = final_df.apply(
        lambda row: func_po_closing_pending_ss(
            row["Current Status"],
            "31. Dispute - PO Closing Pending-SS",
            status_mapping.iloc[86, 1],
            status_mapping.iloc[87, 1],
            row["wh_type"]
        ),
        axis=1
    )

    final_df["D. Master Data Blocker-SS"] = final_df.apply(
        lambda row: row["Current Status"] if f"{row['Current Status']}-SS" == "D. Master Data Blocker-SS" else "NA",
        axis=1
    )

    sub_status_columns = [
        'A. Anti PO Line-SS','B. Compliance Blocked-SS','C. Shipped-SS','D. Master Data Blocker-SS','01. PO Approval Pending-SS','02. Supplier Confirmation Pending-SS',
        '03. PI Upload Pending-SS','04. PI Approval Pending-SS','05. PI Payment Pending-SS','06. Packaging Pending-SS','07. Transparency Label Pending-SS',
        '08. PRD Pending-SS','09. Under Production-SS','10. PRD Confirmation Pending-SS','11. IM Sign-Off Pending-SS','12. Ready for Batching Pending-SS',
        '13. Batch Creation Pending-SS','14. SM Sign-Off Pending-SS','15. CI Approval Pending-SS','16. CI Payment Pending-SS','17. QC Schedule Pending-SS',
        '18. FFW Booking Missing-SS','19. Supplier Pickup Date Pending-SS','20. Pre Pickup Check-SS','21. FOB Pickup Pending-SS','22. Non FOB Pickup Pending-SS',
        '23. INB Creation Pending-SS','24. Mark In-Transit Pending-SS','25. BL Approval Pending-SS','26. BL Payment Pending - In Transit-SS',
        '27. BL Payment Pending - Arrived-SS','28. Telex Release Pending-SS','29. Stock Delivery Pending-SS','30. Stock Receiving Pending-SS',
        '31. Dispute - PO Closing Pending-SS'
    ]

    def func_sub_status(row):
        col_name = f"{row['Current Status']}-SS"
        if col_name in sub_status_columns:
            return row[col_name]
        return np.nan

    final_df['Sub Status'] = final_df.apply(func_sub_status, axis=1)
    final_df["Sub Status #"] = final_df['Sub Status'].astype(str).str.extract(r'^([^\.]+)')

    telex_supplier_map = telex_supplier[["batch_id", "Final Action"]].drop_duplicates(subset="batch_id", keep="first").set_index("batch_id")["Final Action"].to_dict()
    telex_ffw_map = telex_ffw[["Shipment Number", "Final Blocker Status"]].drop_duplicates(subset="Shipment Number", keep="first").set_index("Shipment Number")["Final Blocker Status"].to_dict()

    def func_l2_telex(telex_status, batch_id, shipment_number):
        if telex_status in ["28a", "28b", "28d", "28e"]:
            return telex_supplier_map.get(batch_id, "Not in Telex Sheet")
        return telex_ffw_map.get(shipment_number, "Not in FFW Telex Sheet")

    final_df["L2 Telex"] = final_df.apply(
        lambda row: func_l2_telex(row["Sub Status #"], row["batch_id"], row["INB#"]),
        axis=1
    )


    def func_l2_final_status(row):
        if row['Pickup Blocker'] != "No":
            return row['Pickup Blocker']
        elif row['Status #'] == "D":
            return row["MD Blocker"]
        elif row['Status #'] == "B" or row['Sub Status #'] == "11a":
            return row["L2 Compliance"]
        elif row['Status #'] == "03" or row['Status #'] == "04":
            return row["L2 PI"]
        elif row['Status #'] == "08":
            return row["L2 PRD"]
        elif row['Sub Status #'] == "10a":
            return row["L2 CPRD"]
        elif row['Status #'] == "12":
            return row["L2 G2"]
        elif row['Sub Status #'] == "14a" or row['Sub Status #'] == "14b":
            return row["L2 G4"]
        elif row['Status #'] == "17" or row['Sub Status #'] == "20a":
            return row["L2 QC"]
        elif row['Sub Status #'] == "19b" or row['Sub Status #'] == "19d":
            return row["L2 SPD"]
        elif (row['Status #'] == "20" or row['Status #'] == "21" or row['Status #'] == "22") and row["production_status"] == "Cargo Picked(SM)":
            return row["production_status"]
        elif (row['Sub Status #'] == "14c" or row['Status #'] == "18" or row['Sub Status #'] == "20b" or row['Status #'] == "22" or row['Status #'] == "23" or row['Status #'] == "24"):
            return row["L2 Pickup"]
        elif row['Sub Status #'] in ["28a", "28b", "28c", "28d", "28e", "28f"]:
            return row["L2 Telex"]
        else:
            return "No L2 Status"

    final_df["L2 Final Status"] = final_df.apply(lambda row: func_l2_final_status(row), axis=1)


    final_df['Accountable'] = final_df['Sub Status'].map(status_mapping[["Sub Status", "Accountable"]].drop_duplicates(subset="Sub Status", keep="first").set_index('Sub Status')['Accountable']).fillna("NA")
    final_df['Responsible'] = final_df['Sub Status'].map(status_mapping[["Sub Status", "Responsible"]].drop_duplicates(subset="Sub Status", keep="first").set_index('Sub Status')['Responsible']).fillna("NA")

    blockers_mapping["Blocker bucket"] = blockers_mapping["Blocker bucket"].str.strip()
    blockers_mapping["POC"] = blockers_mapping["POC"].str.strip()
    excel_xlookup_map = blockers_mapping[["Blocker bucket", "POC"]].drop_duplicates(subset="Blocker bucket", keep="first").set_index("Blocker bucket")["POC"].fillna("NA").to_dict()

    def func_final_responsibility(row):
        # l2_final_status = row["L2 Final Status"]
        # status_hash = row["Status #"]
        # batch_invoice_submission_status = row["Batch Invoice Submission Status"]
        # responsible = row["Responsible"]

        if row["L2 Final Status"] != "No L2 Status":
            return excel_xlookup_map.get(row["L2 Final Status"].strip(), "Fahad Farooq")
        else:
            if row["Status #"] == "25" and row["Batch Invoice Submission Status"] == "Submitted":
                return "NA"
            else:
                return row["Responsible"]

    final_df["Final Responsibility"] = final_df.apply(func_final_responsibility, axis=1)

    def func_final_poc(responsibility, cm, sm):
        if responsibility == "CM":
            return cm
        elif responsibility == "SM":
            return sm
        elif responsibility == "IM":
            return "Ramdas Kamath"
        return responsibility

    final_df["Final POC"] = final_df.apply(
        lambda row: func_final_poc(
            row["Final Responsibility"],
            row["CM"],
            row["SM"]
        ),
        axis=1
    )

    # asin_priority_mapping_map = asin_priority_mapping.set_index("ASINxMP")["OTIF Focus"]
    # final_df["OTIF Focus"] = final_df.apply(
    #     lambda row: asin_priority_mapping_map.get(row["asin_mp"], "Priority 3") if row["Team"] == "CN->US" else "Priority 3",
    #     axis=1
    # )
    final_df["OTIF Focus"] = final_df["asin_mp"].map(asin_priority_mapping.drop_duplicates(subset="ASINxMP", keep="first").set_index("ASINxMP")["OTIF Focus"]).fillna("Low")


    def func_final_team(responsibility):
        if not isinstance(responsibility, str) or len(responsibility.strip()) <= 3:
            return responsibility
        if responsibility in ["Muazam Shahzad", "Arvid Gottschall", "Navneet Singh"]:
            return "FFW"
        if responsibility in ["Chetan Sharma", "Nicolo Serani"]:
            return "Payment"
        if responsibility in ["Vivian Gao", "Joey Wang", "Teresa Xiong", "Chinmay Bahulikar"]:
            return "SM"
        if responsibility == "Young Cao":
            return "QC"
        if responsibility in ["Stefanie Gomes", "Muneera Patton"]:
            return "Compliance"
        if responsibility in ["Darren Fernandes", "August Engler"]:
            return "Packaging"
        if responsibility in ["Elena Anufrieva", "Kavya Eluru"]:
            return "ERP-Tech"
        return "OTIF Central Team"

    final_df["Final Team"] = final_df["Final Responsibility"].apply(func_final_team)

    final_df["Days"] = ""
    final_df["Days Bucket"] = ""

    ## final dataframe -- should column be in a specific order?
    final_df = final_df[[
        'id','date_created','document_number','subsidiary_no_hierarchy','scm_associated_brands','po_vendor','supplier_confirmation_status','final_status','scm_po_scm_memo',
        'marketplace_header','supplier_payment_terms','incoterms','line_id','item','asin','quantity','quantity_fulfilled/received','quantity_on_shipments','quantity_billed',
        'item_rate','currency','item_rate_eur','amount_foreign_currency','first_prd','prd','planned_prd','accepted_prd','prd_status','confirmed_crd','quality_control_date',
        'quality_control_status','im_line_signoff','sm_line_signoff','production_status','batch_id','wh_type','considered_for_anti-po','prd_reconfirmation','prd_change_reason',
        'invoice_number','invoice_status','historical_anti-po','Vendor ID','Placement Batch','OTIF Focus','MP','Pending Units','Pending Value','Supplier Confirmation VP Check',
        'PRD Delay Decision','po_razin','razin_mp','asin_mp','po_razin_id','NS PI Status','VP PI Status','PI Payment Status','PI','CI','BL','BL Days','Line Payment Type',
        'Batch Payment Type','INB Payment Type','Line Invoice Submission Status','Batch Invoice Submission Status','INB Invoice Submission Status','Line Payment Status',
        'Batch Payment Status','INB Payment Status','Line Payment Approval Status','Batch Payment Approval Status','INB Payment Approval Status','Transparency Check',
        'Transparency Pending','HS Code', 'Batch Sign-Off','QC Stage','QC Pending','Batch QC Pending','Max QC Date','Booking Form Status','VP Booking Status','FOB Date','Incoterms2',
        'SPD','SPD Delay Reason','Actual pick-up date','Gate In Date','Actual Shipping Date','Batch Pickup Status','Shipping Status','INB#','Status','Substatus','Shipment Method',
        'Actual Pickup','Actual Shipping Date3','Expected Arrival Date','Actual Arrival Date','Actual Delivery Date','Estimated OTIF Delivery Date', 'Batch Telex', 'Supplier Telex Status',
        'SM Telex Status','FFW Telex Status','CM','SM','Accountable','Responsible','Compliance Status','Batch Compliance','MD Blocker','A. Anti PO Line','B. Compliance Blocked',
        'C. Shipped','D. Master Data Blocker','01. PO Approval Pending','02. Supplier Confirmation Pending','03. PI Upload Pending','04. PI Approval Pending','05. PI Payment Pending',
        '06. Packaging Pending','07. Transparency Label Pending','08. PRD Pending','09. Under Production','10. PRD Confirmation Pending','11. IM Sign-Off Pending',
        '12. Ready for Batching Pending','13. Batch Creation Pending','14. SM Sign-Off Pending','15. CI Approval Pending','16. CI Payment Pending','17. QC Schedule Pending',
        '18. FFW Booking Missing','19. Supplier Pickup Date Pending','20. Pre Pickup Check','21. FOB Pickup Pending','22. Non FOB Pickup Pending','23. INB Creation Pending',
        '24. Mark In-Transit Pending','25. BL Approval Pending','26. BL Payment Pending - In Transit','27. BL Payment Pending - Arrived','28. Telex Release Pending',
        '29. Stock Delivery Pending','30. Stock Receiving Pending','31. Dispute - PO Closing Pending','Current Status','Status #','Sub Status #','Sub Status','A. Anti PO Line-SS',
        'B. Compliance Blocked-SS','C. Shipped-SS','D. Master Data Blocker-SS','01. PO Approval Pending-SS','02. Supplier Confirmation Pending-SS','03. PI Upload Pending-SS',
        '04. PI Approval Pending-SS','05. PI Payment Pending-SS','06. Packaging Pending-SS','07. Transparency Label Pending-SS','08. PRD Pending-SS','09. Under Production-SS',
        '10. PRD Confirmation Pending-SS','11. IM Sign-Off Pending-SS','12. Ready for Batching Pending-SS','13. Batch Creation Pending-SS','14. SM Sign-Off Pending-SS',
        '15. CI Approval Pending-SS','16. CI Payment Pending-SS','17. QC Schedule Pending-SS','18. FFW Booking Missing-SS','19. Supplier Pickup Date Pending-SS',
        '20. Pre Pickup Check-SS','21. FOB Pickup Pending-SS','22. Non FOB Pickup Pending-SS','23. INB Creation Pending-SS','24. Mark In-Transit Pending-SS',
        '25. BL Approval Pending-SS','26. BL Payment Pending - In Transit-SS','27. BL Payment Pending - Arrived-SS','28. Telex Release Pending-SS','29. Stock Delivery Pending-SS',
        '30. Stock Receiving Pending-SS','31. Dispute - PO Closing Pending-SS','Days','Days Bucket','Team','Reporting Status','L2 Compliance','L2 PI','L2 PRD','L2 CPRD','L2 G2',
        'L2 G4','L2 QC','L2 SPD','L2 Pickup','L2 Telex','L2 Final Status','Pickup Blocker','Final Responsibility','Final POC','Final Team'
    ]]

    final_df = final_df.replace(["None", "none", "NaN", "nan", "null", "Null"], "")
    final_df = final_df.fillna("")

    final_df.to_csv('final_df_output.csv')

    columns_to_select = [
        "id","date_created","document_number","scm_associated_brands","po_vendor",
        "supplier_confirmation_status","final_status","scm_po_scm_memo","marketplace_header",
        "supplier_payment_terms","incoterms","line_id","item","asin","quantity",
        "quantity_fulfilled/received","quantity_on_shipments","first_prd","prd","planned_prd",
        "confirmed_crd","quality_control_date","quality_control_status","im_line_signoff",
        "sm_line_signoff","production_status","batch_id","wh_type","considered_for_anti-po",
        "prd_reconfirmation","invoice_number","invoice_status","Placement Batch","OTIF Focus",
        "MP","item_rate_eur","Pending Units","Pending Value","po_razin_id","Line Payment Type",
        "Batch Payment Type","INB Payment Type","Line Invoice Submission Status",
        "Batch Invoice Submission Status","INB Invoice Submission Status","Line Payment Status",
        "Batch Payment Status","INB Payment Status","Batch QC Pending","VP Booking Status",
        "FOB Date","Batch Pickup Status","Shipping Status","INB#","Status","Substatus",
        "Estimated OTIF Delivery Date","Supplier Telex Status","SM Telex Status",
        "FFW Telex Status","CM","SM","Compliance Status","Current Status","Sub Status",
        "Days Bucket","Team","Reporting Status","L2 Final Status","Final POC","Final Team"
    ]

    filtered_df = final_df[columns_to_select].copy()

    date_columns = ["prd", "planned_prd", "confirmed_crd", "quality_control_date", "FOB Date", "Estimated OTIF Delivery Date"]
    number_columns = ["item_rate_eur", "Pending Units", "Pending Value"]

    for col in date_columns:
        filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')

    for col in number_columns:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

    filtered_df = filtered_df[filtered_df["document_number"].notna() & (filtered_df["document_number"].astype(str).str.strip() != "")]

    return filtered_df
