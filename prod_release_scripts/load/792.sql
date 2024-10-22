UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select MNTH_ID as "Effective_Sales_Cycle", STORE_CD as "StoreCode", STORE_NM as "StoreName", ITEM_CD as "Item_Code", ITEM_NM as "LongName", FILE_NAME as "FileName", ITEM_CATEGORY as "Category", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_711'
WHERE PARAMETER_ID = 27776
and PARAMETER_GROUP_ID = 2267;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select SLS_AREA as "SArea", PLANT as "BP", CUST_NM as "CustName", CHNL as "Channel", SLS_OFF as "SalesOff", SLS_GRP as "SalesGrp", ADDRESS as "Address", CITY as "City", POSTAL_CD as "PostalCode", DSM as "Dsm", MATL_GRP as "MaterialGrp", UOM_CONV as "iVAL", MATL_NUM as "MaterialNo", OLD_MATL_NUM as "OldIID", MATL_DESC as "MaterialDescription",sysdate() as "DateLoaded", FILE_NAME as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_DYNA'
WHERE PARAMETER_ID = 27784
and PARAMETER_GROUP_ID = 2268;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select PO_NUMBER as "PO#", VENDOR_CODE as "VendorCode", VENDOR_NAME as "VendorName", FROM_STORE as "FromStore", TO_STORE as "ToStore", STORE_NAME as "StoreName", SKU as "SKU", SKU_DESC as "Description", QTY as "QTY", RCR_NUMBER as "RCRNumber", SUBSTRING(FILE_NAME,0,9) as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_PUREGOLD'
WHERE PARAMETER_ID = 27792
and PARAMETER_GROUP_ID = 2269;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select BUYCOST as "BuyCost", SITECODE as "SiteCode", SITENAME as "SiteName", SITEADDRESS as "SiteAddress", FILE_NAME as "FileName", ''N'' as "LoadedToMDSCust" 
from PHLSDL_RAW.SDL_PH_MDS_POS_SM_PO'
WHERE PARAMETER_ID = 27818
and PARAMETER_GROUP_ID = 2272;
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select ARTICLENUMBER as "ArticleNumber", ARTICLEDESCRIPTION as "ArticleDescription", UPC as "UPC", UOM as "UOM", ''1077AA132'' as "ParentCustomerCode", FILE_NAME as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_SM_GR'
WHERE PARAMETER_ID = 27808
and PARAMETER_GROUP_ID = 2271;
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'SELECT STORE_CD as "Store Code", STORE_NM as "Store Description", POS_PROD_CD as "SKU Code",UPC, POS_PROD_NM as "ProductDescription", AMT as "GROSS SALES TY", QTY as "UNITS SOLD TY", FILE_NM as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
FROM PHLSDL_RAW.SDL_PH_MDS_POS_ROB'
WHERE PARAMETER_ID = 27800
and PARAMETER_GROUP_ID = 2270;
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select JJ_MNTH_ID as "Effective_Sales_Cycle", POS_PROD_CD as "SKU Code", POS_PROD_NM as "SKU Description", UPC as "UPC", SUPPLIER_PROD_CD as "Supplier Product Code", STORE_CD as "Store Code", STORE_NM as "Store Description", QTY as "Units Sold TY", AMT as "Net Sales TY", FILE_NM as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_South_star'
WHERE PARAMETER_ID = 27826
and PARAMETER_GROUP_ID = 2273;
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select STORE_CD as "Store", STORE_NM as "StoreName", POS_PROD_CD as "SKUCode", POS_PROD_NM as "SKUName", AMT as "Gross Sales Retail TY", QTY as "Sales Units TY", FILE_NM as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_WATSONS'
WHERE PARAMETER_ID = 27834
and PARAMETER_GROUP_ID = 2274;
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select STORE_CD as "LOCCOD", STORE_NM as "LOCNAM", POS_PROD_CD as "SKU", POS_PROD_NM as "ITMDES", QTY as "SLDQTY", AMT as "EXTPRC", FILE_NM as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
from PHLSDL_RAW.SDL_PH_MDS_POS_WALTERMART'
WHERE PARAMETER_ID = 27842
and PARAMETER_GROUP_ID = 2275;
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select PRODUCT_NAME as "Product_Name", SUBSTR(PRODUCT_CODE,1,100) as "Product_Code", CONSUMERS_BARCODE as "Consumers_Barcode", SHIPPERS_BARCODE as "Shippers_Barcode", DZPERCASE as "DzPerCase", LISTPRICECASE as "ListPriceCase", LISTPRICEDZ as "ListPriceDz", LISTPRICEUNIT as "ListPriceUnit", SRP as "SRP", LEGEND as "Legend", FILENAME as "FileName", ''N'' as "LoadedInMDS"
from PHLSDL_RAW.SDL_PH_MDS_POS_PRICELIST'
WHERE PARAMETER_ID = 27850
and PARAMETER_GROUP_ID = 2276;
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select LDT_SAP_ID, DT_ID, "Country_Code" as "Country_Code","Outlet_ID" as "Outlet_ID", "Outlet_Name" as "Outlet_Name", "Address_1" as "Address_1", "Address_2" as "Address_2", "Telephone" as "Telephone", "FAX" as "FAX", "City" as "City", "PostCode" as "PostCode", "Region" as "Region", "Channel_Group" as "Channel_Group", "Sub_Channel" as "Sub_Channel", "Sales_Route_ID" as "Sales_Route_ID", "Sales_Route_Name" as "Sales_Route_Name", "SaleGroup" as "SaleGroup", "SalesRep_ID" as "SalesRep_ID", "SaleRep_Name" as "SaleRep_Name", "GPS_Lat" as "GPS_Lat", "GPS_Long" as "GPS_Long", "Status" as "Status", "District" as "District", "Province" as "Province", "Sup_Code" as "Sup_Code", "Sup_Name" as "Sup_Name", "Store_Prioritization" as "Store_Prioritization", "FILE_NAME" as "File_Name", sysdate() as "DateLoaded", ''N'' as "LoadedToMDS"
from PHLSDL_RAW.SDL_PH_MDS_GT_CUSTOMER'
WHERE PARAMETER_ID = 27858
and PARAMETER_GROUP_ID = 2277; 
UPDATE META_RAW.PROCESS
SET SOURCE_ID = 9
WHERE usecase_id in (537,538,539)
and SEQUENCE_ID = 2;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'select STORE_CD as "Store Code", STORE_NM as "Store Description", POS_PROD_CD as "SKU Code",UPC, POS_PROD_NM as "ProductDescription", AMT as "GROSS SALES TY", QTY as "UNITS SOLD TY", FILE_NM as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedToMDSProduct"
FROM PHLSDL_RAW.SDL_PH_MDS_POS_ROB'
WHERE PARAMETER_ID = 27800
and PARAMETER_GROUP_ID = 2270;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'ap_ph_metadata/transaction/dms/Customer/'
WHERE PARAMETER_ID = 27618
and PARAMETER_GROUP_ID = 2253;
 
 
UPDATE META_RAW.PARAMETERS
SET PARAMETER_VALUE = 'PHLSDL_RAW.PH_POS_PRICELIST_PREPROCESSING'
WHERE PARAMETER_ID = 27603
and PARAMETER_GROUP_ID = 2252;
 
 
ALTER PROCEDURE IF EXISTS PHLSDL_RAW.PH_POS_PRICELIST(ARRAY) RENAME TO PHLSDL_RAW.PH_POS_PRICELIST_PREPROCESSING;
