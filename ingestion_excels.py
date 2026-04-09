from imports import *
from sharepoint import SharepointClient

def fetch_from_sharepoint(root_url_param, relative_url_param, tracker, sheet):
    root_url = root_url_param
    relative_url = relative_url_param + tracker
    sharepoint = SharepointClient(root_url)
    sharepoint.init_session()
    df = sharepoint.fetch_sharepoint_excel(relative_url, sheet)
    return df

def fetch_from_sharepoint_excel_large_files(root_url_param, relative_url_param, tracker, sheet):
    root_url = root_url_param
    relative_url = relative_url_param + tracker
    sharepoint = SharepointClient(root_url)
    sharepoint.init_session()
    df = sharepoint.fetch_sharepoint_excel_large_files_v2(relative_url, sheet)
    return df


def main(creds):

    today = datetime.now()
    weekday = today.weekday()
    if weekday == 5:
        xtoday = today - timedelta(days=1)
    elif weekday == 6:
        xtoday = today - timedelta(days=2)
    else:
        xtoday = today

    relevant_today = xtoday

    # file_path = "static/default_mappings.xlsx"
    # status_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="Status",
    #     engine="openpyxl"
    # )
    # blockers_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="Blockers",
    #     engine="openpyxl"
    # )
    # payment_terms_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="Payment Terms",
    #     engine="openpyxl"
    # )
    # cm_sm_vendor_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="CM-SM-Vendor",
    #     engine="openpyxl"
    # )
    # memo_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="Memo-Summary",
    #     engine="openpyxl"
    # )
    # team_priority_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="Team-Priority",
    #     engine="openpyxl"
    # )
    # asin_priority_mapping = pd.read_excel(
    #     file_path,
    #     sheet_name="ASIN-Priority",
    #     engine="openpyxl"
    # )
    # asin_static_payment_status = pd.read_csv("static/asin_static_payment_status.csv")

    root_url = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Manually_Updated/Static Mappings/"

    status_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Status")
    blockers_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Blockers")
    payment_terms_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Payment Terms")
    cm_sm_vendor_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "CM-SM-Vendor")
    memo_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Memo-Summary")
    team_priority_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Team-Priority")
    asin_priority_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "ASIN-Priority")
    buffer_mapping = fetch_from_sharepoint(root_url, relative_url, "default_mappings.xlsx", "Stage Buffers")

    asin_static_payment_status = fetch_from_sharepoint(root_url, relative_url, "asin_static_payment_status.xlsx", "asin_static_payment_status")

   

    ## ------------------------------------- FREIGEGEBENE DOKUMENTE / FREIGHT OPERATIONS ------------------------------------- ##

    ## FFW Reporting
    # root_url = "https://razrgroup.sharepoint.com/teams/logistics-group"
    # relative_url = "/teams/logistics-group/Freigegebene%20Dokumente/Freight%20Operations/"

    # for attempt in range(5):
    #     try:
    #         telex_ffw = fetch_from_sharepoint_excel_large_files(root_url, relative_url, "FFW Reporting.xlsx", "INBSHIP Level")
    #         telex_ffw = telex_ffw[["Shipment Number", "Telex Released/Not Released", "Standard Remarks"]]
    #         telex_ffw = telex_ffw[telex_ffw["Shipment Number"].notna() & (telex_ffw["Shipment Number"] != "")]
    #         telex_ffw['Final Status'] = telex_ffw['Telex Released/Not Released'].str.strip()
    #         telex_ffw['Final Blocker Status'] = telex_ffw['Standard Remarks'].apply(
    #             lambda x: "No FFW Telex Blocker Mentioned" if x == "" or pd.isna(x) else x
    #         )
    #         break
    #     except Exception as e:
    #         if attempt < 4:
    #             # print(f'\nFFW Reporting failed, starting try num: {attempt+2}')
    #             time.sleep(10)
    #         else:
    #             raise
    
    root_url = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Temporary/"

    telex_ffw = fetch_from_sharepoint_excel_large_files(root_url, relative_url, "FFW Reporting.xlsx", "INBSHIP Level")
    telex_ffw = telex_ffw[["Shipment Number", "Telex Released/Not Released", "Standard Remarks"]]
    telex_ffw = telex_ffw[telex_ffw["Shipment Number"].notna() & (telex_ffw["Shipment Number"] != "")]
    telex_ffw['Final Status'] = telex_ffw['Telex Released/Not Released'].str.strip()
    telex_ffw['Final Blocker Status'] = telex_ffw['Standard Remarks'].apply(
        lambda x: "No FFW Telex Blocker Mentioned" if x == "" or pd.isna(x) else x
    )

    ## -------------------------------------------------------------------------------------------------------------------- ##


    ## ------------------------------------- PROCUREMENT TRACKERS / TEMPORARY ------------------------------------- ##

    root_url = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Temporary/"


    ## Razor Mohawk Tracker 0224
    fob_date = fetch_from_sharepoint(root_url, relative_url, "Razor Mohawk Tracker_0224.xlsx", "FOB CN-US") ## pending
    fob_date = fob_date[["BATCH ID", "CFS/CY Cut off", "Expected Date at CFS/CY", "ETD Load Port", "Blocker"]]
    fob_date = fob_date[fob_date["BATCH ID"].notna() & (fob_date["BATCH ID"] != "")]
    fob_date["Final Date"] = fob_date["CFS/CY Cut off"].combine_first(fob_date["Expected Date at CFS/CY"])
    fob_date["Pickup Status"] = fob_date["BATCH ID"].apply(lambda x: "Not Picked" if pd.notna(x) and x != "" else "Picked")


    ## -------------------------------------------------------------------------------------------------------------------- ##


    ## ------------------------------------- PROCUREMENT TRACKERS / MANUALLY_UPDATED ------------------------------------- ##

    root_url = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Manually_Updated/"

    ## Packaging Data
    packaging_data = fetch_from_sharepoint(root_url, relative_url, "Active Packaging Tracker Sheet.xlsx", "PO Label Status")
    packaging_data = packaging_data[["PORAZIN", "L2 Bucket 6 Status"]]
    packaging_data = packaging_data[packaging_data["PORAZIN"].notna() & (packaging_data["PORAZIN"] != "")]
    packaging_data_map = pd.DataFrame({
        "Status": [
            "EAN Pending", "SCM Check Pending", "Compliance Check Pending",
            "Label Creation Pending", "Compliance Approval Pending",
            "NPD 1st PO", "Labels Not Required"
        ],
        "Blocker": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "No"],
        "L2": [
            "06a. EAN Pending", "06b. SCM Check Pending", "06c. Compliance Check Pending",
            "06d. Label Creation Pending", "06e. Compliance Approval Pending",
            "06f. NPD 1st PO", ""
        ]
    })
    packaging_data['Final Status'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["Blocker"]).fillna("Yes")
    packaging_data['Packaging Standard Status'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["L2"]).fillna("06b. SCM Check Pending")
    packaging_data['Check'] = packaging_data['L2 Bucket 6 Status'].map(packaging_data_map.set_index("Status")["Blocker"]).fillna("")

    ## Transparency Data
    transparency_data = fetch_from_sharepoint(root_url, relative_url, "20240822_Ad_hoc_edit_requests.xlsx", "Transparency Label Requests")
    transparency_data = transparency_data[["PO&RAZIN", "Status"]]
    transparency_data = transparency_data[transparency_data["PO&RAZIN"].notna() & (transparency_data["PO&RAZIN"] != "")]
    transparency_data['Transparency Pending'] = transparency_data['Status'].apply(lambda x: "Yes" if x=="Pending" else "No")

    ## Transparency Master
    transparency_master = fetch_from_sharepoint(root_url, relative_url, "TransparencyProducts Razor + Perch.xlsx", "Products")
    transparency_master = transparency_master[["ASIN"]]
    transparency_master = transparency_master[transparency_master["ASIN"].notna() & (transparency_master["ASIN"] != "")]
    transparency_master["Transparency Check"] = "Yes"

    ## QC
    qc = fetch_from_sharepoint(root_url, relative_url, "Pending QC status --2025.xlsx", "Pending QC")
    qc = qc[["PO RAZIN ID", "QC Status Category"]]
    qc = qc[qc["PO RAZIN ID"].notna() & (qc["PO RAZIN ID"] != "")]
    qc['Final Status2'] = qc['QC Status Category'].apply(lambda x: "No QC Blocker Mentioned" if x=="" or pd.isna(x) else x)

    ## Payrun
    payrun = fetch_from_sharepoint(root_url, relative_url, "Approved_Invoice_Master_Tracker.xlsx", "Current_Week_Payrun")
    payrun = payrun[["Invoice No.", "PO No.", "Final_Verdict"]]
    payrun = payrun[payrun["Invoice No."].notna() & (payrun["Invoice No."] != "")]
    payrun['Inv#'] = "Bill #" + payrun["Invoice No."].astype(str)
    payrun['Status'] = payrun["Final_Verdict"].str.strip()

    # ## Compliance L2 Status
    # compliance = fetch_from_sharepoint(root_url, relative_url, "Compliance L2 Status.xlsx", "Compliance")
    # compliance = compliance[["PO&RAZIN&ID", "Blocker Status", "Comments", "SM Resolved"]]
    # compliance = compliance[compliance["PO&RAZIN&ID"].notna() & (compliance["PO&RAZIN&ID"] != "")]
    # compliance["Final Status"] = compliance.apply(
    #     lambda row: "Compliance Blocker Resolved" if (row["SM Resolved"] == "Yes")
    #     else ("No Compliance Blocker Mentioned" if (row["Blocker Status"] == "" or pd.isna(row["Blocker Status"])) else row["Blocker Status"]),
    #     axis=1
    # )

    ## FF E2E Tracker V3
    for attempt in range(5):
        try:
            ffw_status = fetch_from_sharepoint_excel_large_files(root_url, relative_url, "FF E2E Tracker_V3.xlsx", "Main Sheet") ## pending
            ffw_status.columns = ffw_status.iloc[0]
            ffw_status = ffw_status[1:].reset_index(drop=True)
            ffw_status = ffw_status.rename(columns={"SubStatus": "SubStatus.1"})
            ffw_status = ffw_status[["Batch ID", "High level stage", "Batch milestone (Automatic)", "Blocker Reason"]]
            ffw_status = ffw_status[ffw_status["Batch ID"].notna() & (ffw_status["Batch ID"] != "")]
            ffw_status["Final Blocker Reason"] = ffw_status["Batch milestone (Automatic)"].apply(lambda x: "No FFW Comment Mentioned" if x == "" or pd.isna(x) else x)
            break
        except Exception as e:
            if attempt < 4:
                # print(f'FFW Reporting failed, starting try num: {attempt+1}')
                time.sleep(10)
            else:
                raise

    ## -------------------------------------------------------------------------------------------------------------------- ##

    ## ------------------------------------- PROCUREMENT TRACKERS / AUTOMATED ------------------------------------- ##
    
    root_url = "https://razrgroup.sharepoint.com/sites/Razor"

    ## PRD
    sheet_name = f"PRD - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/PRD/"
    prd = fetch_from_sharepoint(root_url, relative_urlx, "PRD_Tracker.xlsx", sheet_name)
    prd.columns = prd.iloc[0]
    prd = prd[1:].reset_index(drop=True)
    prd = prd[["otif_id", "SM: PRD STATUS", "SM Comments"]]
    prd = prd[prd["otif_id"].notna() & (prd["otif_id"] != "")]
    prd['Final Status'] = prd['SM Comments'].apply(lambda x: "No PRD Blocker Mentioned" if x=="" or pd.isna(x) else x)

    ## CPRD
    sheet_name = f"CPRD - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/CPRD/"
    cprd = fetch_from_sharepoint(root_url, relative_urlx, "CPRD Tracker.xlsx", sheet_name)
    cprd = cprd[["po_razin_id", "Standard Comments", "SM Comments"]]
    cprd = cprd[cprd["po_razin_id"].notna() & (cprd["po_razin_id"] != "")]
    cprd['Final Status'] = cprd['Standard Comments'].apply(lambda x: "No CPRD Blocker Mentioned" if x=="" or pd.isna(x) else x)


    ## Pending Pickup Tracker - SPD
    sheet_name = f"SPD - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Pickup/"
    spd_blockers = fetch_from_sharepoint(root_url, relative_urlx, "Pending Pickup Tracker.xlsx", sheet_name)
    spd_blockers = spd_blockers[["batch_id", "Delay Reason", "Additional Comments"]]
    spd_blockers = spd_blockers[spd_blockers["batch_id"].notna() & (spd_blockers["batch_id"] != "")]
    spd_blockers["Final Status"] = spd_blockers["Delay Reason"].apply(lambda x: "No SPD Blocker Mentioned" if pd.isna(x) or x == "" or x == "0" else x)

    ## Pending Pickup Tracker - FFW Blockers
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Pickup/"
    ffw_blockers = fetch_from_sharepoint(root_url, relative_urlx, "Pending Pickup Tracker.xlsx", "FFW Blockers")
    ffw_blockers = ffw_blockers[["Batch ID", "FFW Blocker", "SM_Resolved Status"]]
    ffw_blockers = ffw_blockers[ffw_blockers["Batch ID"].notna() & (ffw_blockers["Batch ID"] != "")]
    ffw_blockers["Final Status"] = ffw_blockers.apply(
        lambda row: "Yes" if (row["FFW Blocker"] == "" or pd.isna(row["FFW Blocker"])) else ("Yes" if str(row["SM_Resolved Status"]).startswith("Yes") else "No"),
        axis=1
    )

    ## Telex Release Tracker
    sheet_name = f"TLX - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Telex/"
    telex_supplier = fetch_from_sharepoint(root_url, relative_urlx, "Telex Release Tracker.xlsx", sheet_name)
    telex_supplier = telex_supplier[["shipment number",  "batch_id","SM Action"]]
    telex_supplier = telex_supplier[telex_supplier["shipment number"].notna() & (telex_supplier["shipment number"] != "")]
    telex_supplier['Status'] = telex_supplier["SM Action"].apply(lambda x: "Released" if x == "Green1:Released by Supplier(Copy BOL available on VP)" else "Not Released")
    shipment_status_map = telex_supplier.groupby('shipment number')['Status'].apply(
        lambda x: "Not Released" if (x == "Not Released").any() else "Released"
    ).to_dict()
    telex_supplier['Final Status'] = telex_supplier['shipment number'].map(shipment_status_map)
    telex_supplier['Final Action'] = telex_supplier["SM Action"].apply(lambda x: "No Telex Blocker Mentioned" if x == "" or pd.isna(x) else x)

    ## Prepayment Tracker
    sheet_name = f"PP - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Payment/"
    prepayment = fetch_from_sharepoint(root_url, relative_urlx, "Prepayment Tracker.xlsx", sheet_name)
    prepayment = prepayment[["document number", "Auto_ PI status", "PI upload blocker"]]
    prepayment = prepayment[prepayment["document number"].notna() & (prepayment["document number"] != "")]
    prepayment['Final Status'] = prepayment['PI upload blocker'].apply(lambda x: "No PI Blocker Mentioned" if x=="" or pd.isna(x) else x)


    ## G2
    sheet_name = f"G2 - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/G2&G4/"
    g2 = fetch_from_sharepoint(root_url, relative_urlx, "G2 & G4 Signoff Tracking.xlsx", sheet_name)
    g2 = g2[["otif_id", "SM Confirm Ready for Batching", "Final Dispute/Blocker"]]
    g2 = g2[g2["otif_id"].notna() & (g2["otif_id"] != "")]
    g2["Final Status"] = g2["Final Dispute/Blocker"].apply(lambda x: "No G2 Blocker Mentioned" if pd.isna(x) or x == "" or x == " " or pd.isna(x) or x == 0 else x)

    ## G4
    sheet_name = f"G4 - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/G2&G4/"
    g4 = fetch_from_sharepoint(root_url, relative_urlx, "G2 & G4 Signoff Tracking.xlsx", sheet_name)
    g4 = g4[["batch_id", "SM G4 Status", "Final Dispute/Blocker"]]
    g4 = g4[g4["batch_id"].notna() & (g4["batch_id"] != "")]
    g4["Final Status"] = g4["Final Dispute/Blocker"].apply(lambda x: "No G4 Blocker Mentioned" if pd.isna(x) or x == "" or x == " " or pd.isna(x) or x == 0 else x).str.strip()

    ## Compliance L2 Status
    sheet_name = f"Compliance - {relevant_today.day:02d}.{relevant_today.month:02d}.{relevant_today.year}"
    relative_urlx = "/sites/Razor/Shared Documents/Chetan_Locale/Procurement Trackers/Compliance/"
    compliance = fetch_from_sharepoint(root_url, relative_urlx, "Compliance L2 Status Tracker.xlsx", sheet_name)
    compliance = compliance[["otif_id", "Blocker Status", "Comments", "SM Resolved"]].rename(columns={"otif_id": "PO&RAZIN&ID"})
    compliance = compliance[compliance["PO&RAZIN&ID"].notna() & (compliance["PO&RAZIN&ID"] != "")]
    compliance["Final Status"] = compliance.apply(
        lambda row: "Compliance Blocker Resolved" if (row["SM Resolved"] == "Yes")
        else ("No Compliance Blocker Mentioned" if (row["Blocker Status"] == "" or pd.isna(row["Blocker Status"])) else row["Blocker Status"]),
        axis=1
    )

    ## -------------------------------------------------------------------------------------------------------------------- ##

    root_url = "https://razrgroup.sharepoint.com/sites/Razor"
    relative_url = "/sites/Razor/Shared Documents/Chetan_Locale/Shipping_Orders/"

    ## Booking Form Data
    booking_form_data = fetch_from_sharepoint(root_url, relative_url, "BookingForm_GeneralTracker.xlsx", "Sent") ## pending
    booking_form_data = booking_form_data[["Batch Id", "Date - Sent", "Status"]].drop_duplicates(subset="Batch Id", keep="last")
    booking_form_data = booking_form_data[booking_form_data["Batch Id"].notna() & (booking_form_data["Batch Id"] != "")]


    return {
        'memo_mapping': memo_mapping,
        'status_mapping': status_mapping,
        'blockers_mapping': blockers_mapping,
        'cm_sm_vendor_mapping': cm_sm_vendor_mapping,
        'asin_priority_mapping': asin_priority_mapping,
        'payment_terms_mapping': payment_terms_mapping,
        'team_priority_mapping': team_priority_mapping,
        'asin_static_payment_status': asin_static_payment_status,
        'ffw_status': ffw_status,
        'fob_date': fob_date,
        'spd_blockers': spd_blockers,
        'ffw_blockers': ffw_blockers,
        'telex_supplier': telex_supplier,
        'telex_ffw': telex_ffw,
        'payrun': payrun,
        'packaging_data': packaging_data,
        'transparency_data': transparency_data,
        'transparency_master': transparency_master,
        'prepayment': prepayment,
        'prd': prd,
        'cprd': cprd,
        'g2': g2,
        'g4': g4,
        'qc': qc,
        'compliance': compliance,
        'booking_form_data': booking_form_data,
        'buffer_mapping': buffer_mapping
    }

