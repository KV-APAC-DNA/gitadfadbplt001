update META_RAW.parameters
set parameter_value = 'SELECT DISTRIBUTOR_ID as "Distributor_ID",SALES_ORDER_NUMBER as "Sales_Order_Number",SALES_ORDER_DATE as "Sales_Order_Date", TYPE as "Type",CUSTOMER_CODE as "Customer_Code",DISTRIBUTOR_WH_ID as "Distributor_WH_ID",SAP_MATERIAL_ID as "SAP_Material_ID", PRODUCT_CODE as "Product_Code",PRODUCT_EAN_CODE as "Product_EAN_Code",PRODUCT_DESCRIPTION as "Product_Description",GROSS_ITEM_PRICE as "Gross_Item_Price",QUANTITY as "Quantity",UOM as "UOM",QUANTITY_IN_PIECES as "Quantity_In_Pieces",QUANTITY_AFTER_CONVERSION as "Quantity_After_Conversion",SUB_TOTAL_1 as "Sub_Total_1",
DISCOUNT as "Discount",SUB_TOTAL_2 as "Sub_Total_2",BOTTOM_LINE_DISCOUNT as "Bottom_Line_Discount",TOTAL_AMT_AFTER_TAX as "Total_Amt_After_Tax",TOTAL_AMT_BEFORE_TAX as "Total_Amt_Before_Tax",SALES_EMPLOYEE as "Sales_Employee",CRT_DTTM as "Insert_dt",
''N'' as "ProcessedToMDS" FROM PROD_DNA_CORE.MYSEDW_ACCESS.SDL_VW_MY_DAILY_SELLOUT_SALES_FACT_MDS_SYNC'
where parameter_group_id in (2212) and parameter_name = 'source_query';

update META_RAW.parameters
set parameter_value = 'SELECT DISTRIBUTOR_ID as "Distributor_ID",SALES_ORDER_NUMBER as "Sales_Order_Number",SALES_ORDER_DATE as "Sales_Order_Date", TYPE as "Type",CUSTOMER_CODE as "Customer_Code",DISTRIBUTOR_WH_ID as "Distributor_WH_ID",SAP_MATERIAL_ID as "SAP_Material_ID", PRODUCT_CODE as "Product_Code",PRODUCT_EAN_CODE as "Product_EAN_Code",PRODUCT_DESCRIPTION as "Product_Description",GROSS_ITEM_PRICE as "Gross_Item_Price",QUANTITY as "Quantity",UOM as "UOM",QUANTITY_IN_PIECES as "Quantity_In_Pieces",QUANTITY_AFTER_CONVERSION as "Quantity_After_Conversion",SUB_TOTAL_1 as "Sub_Total_1",
DISCOUNT as "Discount",SUB_TOTAL_2 as "Sub_Total_2",BOTTOM_LINE_DISCOUNT as "Bottom_Line_Discount",TOTAL_AMT_AFTER_TAX as "Total_Amt_After_Tax",TOTAL_AMT_BEFORE_TAX as "Total_Amt_Before_Tax",SALES_EMPLOYEE as "Sales_Employee",CRT_DTTM as "Insert_dt",
''N'' as "ProcessedToMDS" FROM PROD_DNA_CORE.MYSEDW_ACCESS.SDL_VW_MY_MONTHLY_SELLOUT_SALES_FACT_MDS_SYNC' 
where parameter_group_id in (2211) and parameter_name = 'source_query';
