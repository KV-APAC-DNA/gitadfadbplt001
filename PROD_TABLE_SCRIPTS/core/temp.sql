CREATE TABLE OS_WKS.WKS_MY_AS_WATSONS_INVENTORY (		--// CREATE TABLE os_wks.wks_my_as_watsons_inventory (
    customer_code varchar(255),		--//  ENCODE lzo // character varying
    store_code varchar(255),		--//  ENCODE lzo // character varying
    year varchar(255),		--//  ENCODE lzo // character varying
    mth_code varchar(255),		--//  ENCODE lzo // character varying
    material_code varchar(255),		--//  ENCODE lzo // character varying
    inv_qty_pc varchar(255),		--//  ENCODE lzo // character varying
    inv_value varchar(255),		--//  ENCODE lzo // character varying
    filename varchar(255),		--//  ENCODE lzo // character varying
    crtd_dttm timestamp without time zone		--//  ENCODE az64
)
;		--// DISTSTYLE AUTO;
