from imports import *
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

def fetch_from_redshift(user_id, password, database, host, port, sql_query):
    conn_params = {
        'user': user_id,
        'password': password,
        'database': database,
        'host': host,
        'port': port
    }
    
    def connect_to_redshift(params):
        """Establish a connection to Redshift."""
        try:
            conn = connect(**params)
            return conn
        except InterfaceError as e:
            print(f"InterfaceError: {e}")
            raise
    
    def fetch_data(conn, query):
        """Run SET and SELECT queries and return a DataFrame."""
        with conn.cursor() as cursor:
            # cursor.execute("SET enable_case_sensitive_identifier TO TRUE;")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
    
    conn = None
    try:
        conn = connect_to_redshift(conn_params)
        df = fetch_data(conn, sql_query)
        num_records_fetched = len(df)  # Store the number of records fetched
    
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame() # Return an empty DataFrame on error
    
    finally:
        if conn:
            conn.close()
            
    return df

po_sql_query = """
    WITH POData AS (
        SELECT
            *,
            DENSE_RANK() OVER (
                PARTITION BY CONCAT(document_number, line_id)
                ORDER BY snapshot_datetime DESC
            ) AS PORank
        FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
    ),
    RankedData AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY CONCAT(document_number, line_id)
                ORDER BY snapshot_datetime DESC
            ) AS row_num
        FROM POData
        WHERE 
            final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
            AND PORank = 1
            AND (quantity - "quantity_fulfilled/received") > 0
            AND (scm_po_scm_memo IS NULL OR scm_po_scm_memo != 'import_ic_flow')
    )
    SELECT
        id,
        TO_DATE(date_created, 'DD.MM.YYYY HH24:MI') AS date_created,
        document_number,
        subsidiary_no_hierarchy,
        scm_associated_brands,
        po_vendor,
        supplier_confirmation_status,
        final_status,
        scm_po_scm_memo,
        marketplace_header,
        supplier_payment_terms,
        incoterms,
        line_id,
        item,
        asin,
        quantity,
        "quantity_fulfilled/received",
        quantity_on_shipments,
        quantity_billed,
        item_rate,
        currency,
        item_rate_eur,
        amount_foreign_currency,
        TO_DATE(first_prd, 'DD.MM.YYYY') AS first_prd,
        prd,
        planned_prd,
        TO_DATE(accepted_prd, 'DD.MM.YYYY') AS accepted_prd,
        prd_status,
        confirmed_crd,
        quality_control_date,
        quality_control_status,
        im_line_signoff,
        sm_line_signoff,
        production_status,
        batch_id,
        wh_type,
        "considered_for_anti-po",
        prd_reconfirmation,
        prd_change_reason,
        invoice_number,
        invoice_status,
        "historical_anti-po"
    FROM RankedData
    WHERE row_num = 1;
    """ 

pl_sql_query = """
    SELECT
        CASE 
            WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
            THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
            ELSE shipment_batch_id_pl_id 
        END AS batch_id,

        CASE 
            WHEN pl_status = 'accepted-ffw' THEN 'Signed-Off'
            WHEN pl_status = 'accepted-sm' THEN '14c. FFW Sign-Off Missing'
            WHEN pl_status IN ('ocr1-accepted', 'uploaded', 'ocr2-rejected', 'ocr2-accepted') 
                THEN '14b. SM Sign-Off Missing'
            ELSE '14a. Documents Missing' 
        END AS final_status

    FROM razor_db.vendor_portal.invoicing_packinglist_uploads_ddb_logs

    WHERE pl_status IN (
            'rejected-ffw', 'accepted-ffw', 'accepted-sm', 
            'ocr1-accepted', 'uploaded', 'ocr2-rejected', 'ocr2-accepted'
        )
        AND LENGTH(
            CASE 
                WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
                THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
                ELSE shipment_batch_id_pl_id 
            END
        ) = 12

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
            CASE 
                WHEN POSITION('#' IN shipment_batch_id_pl_id) > 0 
                THEN LEFT(shipment_batch_id_pl_id, POSITION('#' IN shipment_batch_id_pl_id) - 1)
                ELSE shipment_batch_id_pl_id 
            END
        ORDER BY CAST(created_date AS TIMESTAMP) DESC, CAST(approximate_ts AS TIMESTAMP) DESC
    ) = 1;
    """

batch_sql_query = """
    SELECT 
        BatchData.batch_id,
        MAX(BatchData.vp_booking_status) AS vp_booking_status,
        MAX(BatchData.freight_forwarder) AS freight_forwarder,
        MAX(BatchData.po_number) AS po_number,
        MAX(BatchData.incoterms) AS incoterms,
        MAX(BatchData.shipment_method) AS shipment_method,
        MAX(BatchData.scr_date) AS scr_date,
        MAX(BatchData.scrd_delay_reasons) AS scrd_delay_reasons,
        MAX(BatchData.ccrd_by_freight) AS ccrd_by_freight,
        MAX(BatchData.cfs_cut_off) AS cfs_cut_off,
        MAX(BatchData.expected_pickup_date) AS expected_pickup_date,
        MAX(BatchData.actual_pickup_date) AS actual_pickup_date,
        MAX(BatchData.gate_in_date) AS gate_in_date,
        MAX(BatchData.expected_shipping_date) AS expected_shipping_date,
        MAX(BatchData.actual_shipping_date) AS actual_shipping_date
    FROM razor_db.netsuite.batch_lines AS BatchData
    LEFT JOIN (
        SELECT batch_id, final_status, line_id
        FROM (
            SELECT 
                batch_id,
                final_status,
                line_id,
                DENSE_RANK() OVER (
                    PARTITION BY CONCAT(batch_id, line_id) 
                    ORDER BY snapshot_date DESC
                ) AS PORank
            FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
        ) AS RankedData
        WHERE PORank = 1
    ) AS POData
    ON BatchData.batch_id = POData.batch_id
    WHERE POData.final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
    GROUP BY BatchData.batch_id;
    """

inb_sql_query = """
    SELECT
        INBData.shipment_number,
        INBData.date_created,
        INBData.freight_forwarder,
        INBData.external_document_number,
        INBData.status,
        INBData.substatus,
        INBData.market_place,
        INBData.po,
        POData.line_id,
        INBData.item,
        INBData.scm_associated_brand,
        INBData.quantity_expected,
        INBData.quantity_received,
        INBData.quantity_remaining_to_be_received,
        INBData.scm_destination_warehouse,
        INBData.shipment_method,
        INBData.shipment_type,
        INBData.cargo_ready_date,
        INBData.expected_pick_up_date,
        INBData.actual_cargo_pick_up_date,
        INBData.expected_shipping_date,
        INBData.actual_shipping_date,
        INBData.expected_arrival_date,
        INBData.actual_arrival_date,
        INBData.expected_delivery_date,
        INBData.actual_delivery_date,
        INBData.header_snapshot_date,
        INBData.line_snapshot_date
    FROM (
        SELECT
            INBH.shipment_number,
            INBH.date_created,
            INBH.freight_forwarder,
            INBH.external_document_number,
            INBH.status,
            INBH.substatus,
            INBH.market_place,
            INBL.po,
            CASE
                WHEN CHARINDEX('_', INBL.join_collum) > 0 THEN
                    RIGHT(INBL.join_collum, LEN(INBL.join_collum) - CHARINDEX('_', INBL.join_collum))
                ELSE NULL
            END AS line_id,
            INBL.item,
            INBL.scm_associated_brand,
            INBL.quantity_expected,
            INBL.quantity_received,
            INBL.quantity_remaining_to_be_received,
            INBH.scm_destination_warehouse,
            INBH.shipment_method,
            INBH.shipment_type,
            INBH.cargo_ready_date,
            INBH.expected_pick_up_date,
            INBH.actual_cargo_pick_up_date,
            INBH.expected_shipping_date,
            INBH.actual_shipping_date,
            INBH.expected_arrival_date,
            INBH.actual_arrival_date,
            INBH.expected_delivery_date,
            INBH.actual_delivery_date,
            INBL.po_line_unique_key,
            INBH.snapshot_date AS header_snapshot_date,
            INBL.snapshot_date AS line_snapshot_date
        FROM razor_db.public.rgbit_netsuite_inbound_shipments_header AS INBH
        INNER JOIN razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey AS INBL
            ON INBH.shipment_number = INBL.shipment_number
    ) AS INBData
    LEFT JOIN (
        SELECT
            document_number,
            line_id,
            item,
            quantity,
            "quantity_fulfilled/received",
            po_line_unique_key,
            DENSE_RANK() OVER (
                PARTITION BY CONCAT(document_number, line_id)
                ORDER BY snapshot_datetime DESC
            ) AS PORank
        FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
    ) AS POData
        ON INBData.po_line_unique_key = POData.po_line_unique_key
    WHERE (POData.quantity - POData."quantity_fulfilled/received") > 0
    AND INBData.shipment_number IS NOT NULL
    AND INBData.shipment_number <> '';
    """

telex_sql_query = """
    SELECT
        shipment_number,
        batch_id,
        MAX(batch_telex_date) AS telex_release_date_supplier,
        MAX(telex_release_date) AS telex_release_date_ffwp
    FROM (
        SELECT
            shp_head.id,
            shp_head.recordtype,
            TO_DATE(shp_head.date_created, 'dd.mm.yyyy') AS shipment_created_date,
            shp_head.shipment_number,
            shp_head.external_document_number,
            shp_head.status,
            shp_head.substatus,
            shp_head.current_owner,
            shp_head.market_place,
            shp_head.freight_forwarder,
            shp_head.vendor_other,
            shp_head.shipment_method,
            shp_head.shipment_type,
            shp_head.palletized,
            shp_head.spd,
            shp_head.fba_id,
            shp_head.port_of_departure_pod,
            shp_head.port_of_arrival_poa,
            shp_head.currency_cy,
            shp_head.cy_to_cy_rate,
            shp_head.currency_qsc,
            shp_head.quoted_shipping_cost,
            shp_head.currency_lmc,
            shp_head.last_mile_cost,
            shp_head.cargo_ready_date,
            shp_head.expected_shipping_date,
            shp_head.expected_arrival_date,
            shp_head.expected_delivery_date,
            shp_head.actual_cargo_pick_up_date,
            shp_head.actual_shipping_date,
            shp_head.actual_arrival_date,
            shp_head.actual_delivery_date,
            CASE
                WHEN gross_volume_cbm IS NULL OR gross_volume_cbm = '' OR gross_volume_cbm = 0 
                THEN net_volume_cbm_auto_calculated_custom
                ELSE gross_volume_cbm
            END AS gross_volume_cbm,
            net_volume_cbm_auto_calculated_custom,
            net_weight_kg,
            net_weight_kg_auto_calculated_custom,
            shp_head.vessel_number,
            shp_head.bol_awb_cim_cmr_no,
            shp_head.container_number_container_type,
            shp_head.container_teu,
            shp_head.container_quantity,
            shp_head.commercial_invoice,
            shp_head.packing_lists,
            shp_head.bol_awb_cim_cmr,
            shp_head.customs_declarations,
            shp_head.proof_of_delivery,
            shp_head.scm_destination_warehouse,
            shp_head.receiving_warehouse_type,
            shp_head.unis_facility,
            shp_head.unis_asn_ref,
            shp_head.sent_to_fiege,
            shp_head.everstox_asn_ref,
            shp_head.everstox_status,
            shp_head.scm_memo,
            shp_head.scm_link_to_inbship,
            shp_head.scm_weeks_to_arrival,
            shp_head.scm_warehousing_memo,
            shp_head.scm_applied_purchase_order,
            shp_head.scm_volume_deviation,
            shp_head.scm_weight_deviation,
            shp_head.scm_cy2cycost_per_cbm,
            shp_head.type_of_dispute,
            shp_head.datascope_claim_ref,
            shp_head.substatus_wh,
            TO_DATE(shp_head.telex_release_date, 'dd.mm.yyyy') AS telex_release_date,
            TO_DATE(shp_head.gate_in, 'dd.mm.yyyy') AS gate_in,
            TO_DATE(shp_head.gate_out, 'dd.mm.yyyy') AS gate_out,
            TO_DATE(shp_head.customs_clearance_date, 'dd.mm.yyyy') AS customs_clearance_date,
            shp_head.customs_clearance,
            shp_head.snapshot_date,
            shp_head.ff_remarks,
            shp_line.*,
            TO_DATE(bcd.booking_confirmation_date, 'dd.mm.yyyy') AS booking_confirmation_date,
            TO_DATE(bcd.actual_inbound_date, 'dd.mm.yyyy') AS actual_inbound_date,
            PO_LINE.BL_VALUE,
            PO_LINE.BL_Days,
            PO_LINE.asin,
            PO_LINE.batch_id,
            PO_LINE.amount,
            invoice_tbl.document_number AS Inv_number,
            invoice_tbl.Inv_Amount,
            invoice_tbl.inv_Status,
            invoice_tbl.Inv_Qty,
            batch_telex.batch_telex_date,
            CURRENT_DATE AS data_update_date
        FROM razor_db.public.rgbit_netsuite_inbound_shipments_header shp_head
        INNER JOIN (
            SELECT
                shipment_number AS ship_num,
                PO,
                item,
                Vendor,
                quantity_expected,
                quantity_received,
                po_line_unique_key
            FROM razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey
        ) shp_line
            ON shp_head.shipment_number = shp_line.ship_num
        LEFT JOIN (
            SELECT
                record AS shipment_number,
                MIN(booking_confirmation_date) AS booking_confirmation_date,
                MIN(actual_inbound_date) AS actual_inbound_date
            FROM (
                SELECT *,
                    CASE 
                        WHEN field = 'Status' AND new_value IN ('partiallyReceived', 'received') AND old_value = 'inTransit'
                            THEN date 
                    END AS actual_inbound_date,
                    CASE 
                        WHEN field = 'External Document Number' AND new_value IS NOT NULL AND old_value IS NULL
                            THEN date 
                    END AS booking_confirmation_date
                FROM razor_db.public.rgbit_shipment_system_notes_raw_ns
            )
            GROUP BY 1
        ) bcd
            ON shp_head.shipment_number = bcd.shipment_number
        LEFT JOIN (
            SELECT DISTINCT
                asin,
                supplier_payment_terms,
                CAST(SPLIT_PART(supplier_payment_terms, '%', 1) AS INT) AS pi_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 2), 'd ', 2), '') AS INT), 0) AS ci_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 3), 'd ', 2), '') AS INT), 0) AS bl_value,
                NVL(CAST(NULLIF(SPLIT_PART(SPLIT_PART(SPLIT_PART(supplier_payment_terms, '%', 4), ' ', 3), 'd', 1), '') AS INT), 0) AS BL_Days,
                po_line_unique_key,
                document_number,
                item,
                line_id,
                batch_id,
                item_rate_eur AS amount
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        ) PO_LINE
            ON shp_line.po_line_unique_key = PO_LINE.po_line_unique_key
        LEFT JOIN (
            SELECT
                po_line_unique_key,
                LISTAGG(document_number, ', ') AS document_number,
                SUM(maximum_of_amount_eur_consolidated) AS Inv_Amount,
                SUM(absolute_value_of_maximum_of_quantity) AS Inv_Qty,
                LISTAGG(status, ', ') AS INV_STATUS
            FROM razor_db.public.rgbit_po_invoice_mapping
            GROUP BY po_line_unique_key
        ) invoice_tbl
            ON po_line.po_line_unique_key = invoice_tbl.po_line_unique_key
        LEFT JOIN (
            SELECT
                Batch_ID,
                MAX(telex_release_date) AS batch_telex_date
            FROM (
                SELECT *,
                    SPLIT_PART(vendor_id_po_number, '#', 2) AS PO_Number,
                    SPLIT_PART(shipment_batch_id_pl_id, '#', 1) AS Batch_ID,
                    TO_DATE(created_date, 'yyyy.mm.dd') AS telex_release_date
                FROM razor_db.vendor_portal.invoicing_packinglist_uploads_ddb_logs
                WHERE telex_Status = 'uploaded'
            )
            GROUP BY Batch_ID
        ) batch_telex
            ON po_line.batch_id = batch_telex.batch_id
    ) final
    GROUP BY shipment_number, batch_id;
    """

pi_sql_query = """
    SELECT 
        approximate_ts,
        vendor_id_po_number,
        invoice_status
    FROM (
        SELECT  
            TO_TIMESTAMP(snapshot_date, 'YYYY-MM-DD HH24:MI:SS') AS snapshot_date,
            sequence_number,
            TO_TIMESTAMP(approximate_ts, 'YYYY-MM-DD HH24:MI:SS') AS approximate_ts,
            vendor_id_po_number,
            invoice_type_invoice_id,
            created_date,
            invoice_pdf_link,
            netsuite_invoice_id,
            invoice_status,
            uploaded_by,
            auto_comments,
            ROW_NUMBER() OVER (
                PARTITION BY vendor_id_po_number, extracted_part 
                ORDER BY approximate_ts DESC
            ) AS rn
        FROM (
            SELECT 
                *,
                SPLIT_PART(invoice_type_invoice_id, '#', 1) AS extracted_part
            FROM razor_db.vendor_portal.invoicing_invoice_uploads_ddb_logs
        )
    ) 
    WHERE rn = 1
    ORDER BY approximate_ts DESC;
    """

pi_ns_sql_query = """
    SELECT
        DISTINCT FinalData.document_number,
        FinalData.status,
        FinalData.snapshot_date,
        FinalData.po_number
    FROM (
        SELECT 
            PayData.*, 
            POData.final_status
        FROM (
            -- Latest snapshot per document
            SELECT *, 
                RIGHT(s_c_po, 8) AS po_number
            FROM (
                SELECT *, 
                    DENSE_RANK() OVER (
                        PARTITION BY document_number 
                        ORDER BY snapshot_date DESC
                    ) AS PayRank
                FROM razor_db.public.rgbit_po_line_status_pocohort
            ) AS RankedPayData
            WHERE po_number IS NOT NULL
            AND PayRank = 1
            AND document_number NOT LIKE '%cancelled%'
        ) AS PayData
        LEFT JOIN (
            -- Latest final status from PO
            SELECT 
                document_number AS po_document_number, 
                final_status
            FROM (
                SELECT 
                    document_number, 
                    final_status,
                    DENSE_RANK() OVER (
                        PARTITION BY document_number 
                        ORDER BY snapshot_date DESC
                    ) AS PORank
                FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
            ) AS RankedPO
            WHERE PORank = 1
        ) AS POData
        ON PayData.po_number = POData.po_document_number
    ) AS FinalData
    WHERE FinalData.final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed')
    ORDER BY CASE 
        WHEN LOWER(TRIM(FinalData.status)) = 'paid in full' THEN 1
        WHEN LOWER(TRIM(FinalData.status)) = 'pending approval' THEN 2
        WHEN LOWER(TRIM(FinalData.status)) = 'open' THEN 3
        ELSE 4
    END;
    """

supplier_confirmation_sql_query = """
    SELECT 
        snapshot_date,
        approximate_ts,
        po_number,
        po_status
    FROM (
        SELECT 
            ROW_NUMBER() OVER (
                PARTITION BY po_number 
                ORDER BY approximate_ts
            ) AS row_num,
            *
        FROM razor_db.vendor_portal.purchaseorders_headers_ddb_logs
    ) AS subquery
    WHERE row_num = 1
    ORDER BY approximate_ts DESC;
    """

master_data_sql_query = """
    WITH RankedData AS (
        SELECT 
            MasterData.*,
            ROW_NUMBER() OVER (
                PARTITION BY MasterData.razin, MasterData.market_place
                ORDER BY MasterData.snapshot_date DESC
            ) AS rn
        FROM razor_db.core.razin_product_stage_master_mapping AS MasterData
        INNER JOIN (
            SELECT 
                item,
                marketplace_header,
                quantity,
                "quantity_fulfilled/received"
            FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
        ) AS POData
        ON MasterData.razin = POData.item
        AND MasterData.market_place = POData.marketplace_header
        WHERE 
            (POData.quantity - COALESCE(POData."quantity_fulfilled/received", 0)) > 0
            AND MasterData.block_reason_code IS NOT NULL
            AND TRIM(MasterData.block_reason_code) <> ''
    )

    SELECT 
        razin,
        market_place,
        operating_status,
        block_reason_code,
        preferred_supplier_open_po_stock_impact
    FROM RankedData
    WHERE rn = 1;
    """

compliance_query = """
    SELECT
        id as record_id, '' as deal_name, compliance_test_results as deal_stage,
        razin, marketplace, compliance_status, vendor
    FROM razor_db.core.razin_mp_vendor_master_data;
    """

dod_query = """
    SELECT 
        po_number || razin || line_id::text AS po_razin_id,
        batch_id,
        shipment_number,
        supplier_payment_terms,
        historical_anti_po,
        po_created_date,
        po_approval_date,
        supplier_confirmation_date,
        pi_invoice_approval_date,
        pi_payment_date,
        receive_first_prd_date,
        prd_reconfirmed_date,
        po_im_date_value,
        po_sm_date_value,
        batch_created_ts,
        sm_signoff_ts,
        ci_invoice_approval_date,
        ci_payment_date,
        qc_schedule_date,
        ffw_booking_ts,
        spd_ts,
        stock_pickup_date,
        shipment_creation_date,
        shipment_in_transit_date,
        bi_invoice_approval_date,
        bi_payment_date,
        ffwp_telex_release_date,
        shipment_stock_delivery_date,
        item_receipt_date,
        actual_cargo_pick_up_date,
        actual_shipping_date,
        actual_arrival_date,
        actual_delivery_date,
        first_prd_date,
        final_prd_date,
        planned_prd,
        batch_spd,
        qi_date
    FROM (
        SELECT 
            DoDData.*,
            POData.quantity,
            POData."quantity_fulfilled/received"
        FROM razor_db.procurement.otif_tracker AS DoDData
        LEFT JOIN (
            SELECT 
                document_number,
                line_id,
                item,
                quantity,
                "quantity_fulfilled/received",
                DENSE_RANK() OVER (
                    PARTITION BY document_number || line_id::text
                    ORDER BY snapshot_datetime DESC
                ) AS PORank
            FROM razor_db.netsuite.otif_purchase_order_line_items_with_key
        ) AS POData
        ON DoDData.po_number = POData.document_number
        AND DoDData.line_id = POData.line_id
        WHERE PORank = 1
    ) AS FinalData
    WHERE final_status NOT IN ('Closed','Legacy Closed','Fully Billed')
    AND (quantity - "quantity_fulfilled/received") > 0
    AND (scm_po_scm_memo IS NULL OR scm_po_scm_memo != 'import_ic_flow');
    """

hs_codes_query = """
    SELECT 
        name,
        unpivoted.Attribute,
        unpivoted.Value
    FROM (
        SELECT 
            name,
            'hs_code_eu' AS Attribute, hs_code_eu AS Value
        FROM razor_db.core.razin_master_data
        UNION ALL
        SELECT 
            name,
            'hs_code_uk' AS Attribute, hs_code_uk AS Value
        FROM razor_db.core.razin_master_data
        UNION ALL
        SELECT 
            name,
            'hs_code_us' AS Attribute, hs_code_us AS Value
        FROM razor_db.core.razin_master_data
        UNION ALL
        SELECT 
            name,
            'hs_code_ca' AS Attribute, hs_code_ca AS Value
        FROM razor_db.core.razin_master_data
    ) AS unpivoted
    WHERE 
        unpivoted.Value IS NOT NULL 
        AND unpivoted.Value <> '';
    """

def main(creds):
    user = creds['user']
    password = creds['password']
    host = creds['host']
    port = int(creds['port'])
    database = creds['database']

    # Define a list of queries to be executed concurrently
    queries = {
        'po_data': po_sql_query,
        'pl_data': pl_sql_query,
        'batch_data': batch_sql_query,
        'inb_data': inb_sql_query,
        'telex_tableau': telex_sql_query,
        'pi_data': pi_sql_query,
        'pi_ns_data': pi_ns_sql_query,
        'supplier_confirmation': supplier_confirmation_sql_query,
        'master_data': master_data_sql_query,
        'compliance_hubspot': compliance_query,
        'dod_data': dod_query,
        'hs_codes_data': hs_codes_query
    }

    results = {}
    # Use ThreadPoolExecutor for concurrent execution
    with ThreadPoolExecutor(max_workers=len(queries)) as executor:
        # Submit each fetch operation as a separate task
        future_to_query_name = {
            executor.submit(fetch_from_redshift, user, password, database, host, port, sql_query): name
            for name, sql_query in queries.items()
        }

        for future in concurrent.futures.as_completed(future_to_query_name):
            query_name = future_to_query_name[future]
            try:
                data = future.result()
                results[query_name] = data
            except Exception as exc:
                print(f'{query_name} generated an exception: {exc}')
                results[query_name] = pd.DataFrame()

    # Post-processing of fetched dataframes
    if 'batch_data' in results and not results['batch_data'].empty:
        results['batch_data']['Booking Status'] = results['batch_data'].apply(
            lambda row: "Not Booked" if row["vp_booking_status"] == "Cancelled"
            else "Booked" if (pd.notna(row["vp_booking_status"]) and row["vp_booking_status"] != "")
            else "Booked" if (pd.notna(row["freight_forwarder"]) and row["freight_forwarder"] != "")
            else "Not Booked", axis=1)

    if 'inb_data' in results and not results['inb_data'].empty:
        results['inb_data']['PO&RAZIN&ID'] = results['inb_data']['po'].astype(str) + results['inb_data']['item'].astype(str) + results['inb_data']['line_id'].astype(str)

    if 'telex_tableau' in results and not results['telex_tableau'].empty:
        results['telex_tableau']['Batch Status'] = results['telex_tableau']["telex_release_date_supplier"].apply(lambda x: "Not Released" if x == "" or pd.isna(x) else "Released")
        
        shipment_status_map = results['telex_tableau'].groupby('shipment_number')['Batch Status'].apply(
            lambda x: "Not Released" if (x == "Not Released").any() else "Released"
        ).to_dict()

        def final_status_supplier(row):
            if pd.isna(row['shipment_number']) or row['shipment_number'] == "":
                return row['Batch Status']
            return shipment_status_map.get(row['shipment_number'], row['Batch Status'])

        results['telex_tableau']['Final Status (Supplier)'] = results['telex_tableau'].apply(final_status_supplier, axis=1)

        results['telex_tableau']['Final Status (FFW)'] = results['telex_tableau']["telex_release_date_ffwp"].apply(lambda x: "Not Released" if x == "" or pd.isna(x) else "Released")

    if 'pi_data' in results and not results['pi_data'].empty:
        results['pi_data']['PO#'] = results['pi_data']['vendor_id_po_number'].apply(lambda x: x[x.find("#")+1:x.find("#")+9] if "#" in x else "")
        pi_data_map = pd.DataFrame({
            "invoice_status": [
                "rejected", "ocr2-rejected", "ocr1-rejected", "cancelled", "nan", "-", "invalid",
                "ocr1-accepted", "ocr2-accepted", "uploaded",
                "accepted", "pending-ns",
                "open-ns", "paid", "rejected-ns"
            ],
            "Status": [
                "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending",
                "03. PI Upload Pending", "03. PI Upload Pending", "03. PI Upload Pending",
                "04a. SM Review Pending", "04a. SM Review Pending", "04a. SM Review Pending",
                "04b. Accounting Approval Pending", "04b. Accounting Approval Pending",
                "05b. Pending Approval", "05a. Approved", "05b. Pending Approval"
            ]
        })
        results['pi_data']["status"] = results['pi_data']["invoice_status"].map(pi_data_map.set_index("invoice_status")["Status"]).fillna("03. PI Upload Pending")

    # if 'master_data' in results and not results['master_data'].empty:
    #     results['master_data']["razin_mp"] = results['master_data']["razin"].astype(str) + results['master_data']["market_place"].astype(str)
    #     results['master_data']["Action"] = results['master_data']["preferred_supplier_open_po_stock_impact"].replace({
    #         "None": "No Blocker",
    #         "Reroute to non-Blocked Geo or Cancel PO": "Reroute or Cancel"
    #     }).fillna(results['master_data']["preferred_supplier_open_po_stock_impact"])

    if 'master_data' in results and not results['master_data'].empty:
        results['master_data']["razin_mp"] = results['master_data']["razin"].astype(str) + results['master_data']["market_place"].astype(str)

        def determine_action(row):
            impact = row["preferred_supplier_open_po_stock_impact"]
            status = row["operating_status"]
            
            if impact == "None":
                return "No Blocker"
            elif impact == "Reroute to non-Blocked Geo or Cancel PO":
                return "Reroute or Cancel"
            elif impact == "On Hold" and status == "F":
                return "Cancel PO"
            else:
                return impact

        results['master_data']["Action"] = results['master_data'].apply(determine_action, axis=1)

    if 'compliance_hubspot' in results and not results['compliance_hubspot'].empty:
        results['compliance_hubspot'] = results['compliance_hubspot'][["deal_stage", "razin", "marketplace", "compliance_status", "vendor"]]
        eu_markets = {"FR", "BE", "ES", "PL", "NL", "SE", "IT", "DE"}
        results['compliance_hubspot']["Final MP"] = results['compliance_hubspot']["marketplace"].apply(lambda x: "Pan-EU" if x in eu_markets else x)
        results['compliance_hubspot']["RAZIN&MP"] = results['compliance_hubspot']["razin"].astype(str).str.strip() + results['compliance_hubspot']["Final MP"].astype(str)
        results['compliance_hubspot']["Vendor Code"] = results['compliance_hubspot']["vendor"].str.extract(r"^(\S+)", expand=False).fillna("")
        results['compliance_hubspot']["RAZIN&MP&Vendor"] = results['compliance_hubspot']["razin"].astype(str).str.strip() + results['compliance_hubspot']["Final MP"].astype(str) + results['compliance_hubspot']["Vendor Code"].astype(str)

    if ('dod_data' in results and not results['dod_data'].empty) and ('po_data' in results and not results['po_data'].empty):
        results['po_data']['po_razin_idx'] = (
            results['po_data']['document_number'].astype(str).str.strip() +
            results['po_data']['item'].astype(str).str.strip() +
            results['po_data']['line_id'].astype(str).str.strip()
        )
        valid_po_ids = set(results['po_data']['po_razin_idx'])
        results['dod_data'] = results['dod_data'][results['dod_data']['po_razin_id'].isin(valid_po_ids)]

    if 'hs_codes_data' in results and not results['hs_codes_data'].empty:
        attribute_to_mp = {
            "hs_code_eu": "Pan-EU",
            "hs_code_uk": "UK",
            "hs_code_ca": "CA",
            "hs_code_us": "US"
        }
        results['hs_codes_data']["MP"] = results['hs_codes_data']["attribute"].map(attribute_to_mp)
        results['hs_codes_data']["RAZINxMP"] = results['hs_codes_data']["name"].astype(str).str.strip() + results['hs_codes_data']["MP"].astype(str)
        missing_values = {"NONE", "PROHIBITED", "PROHIBITED (ADD/CVD)", "Anti Dumping Applicable"}
        results['hs_codes_data']["HS Code Status"] = results['hs_codes_data']["value"].apply(lambda x: "HS Code Missing" if str(x).strip().upper() in {v.upper() for v in missing_values} else "Available")
    
    return results