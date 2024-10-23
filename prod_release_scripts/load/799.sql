update META_RAW.PARAMETERS SET PARAMETER_VALUE='select ARTICLENUMBER as "ArticleNumber", ARTICLEDESCRIPTION as "ArticleDescription", UPC as "UPC", UOM as "UOM", ''1077AA132'' as "ParentCustomerCode", FILE_NAME as "File_Name", ''N'' as "LoadedToMDSCust", ''N'' as "LoadedtoMDSProcuct"
from PHLSDL_RAW.SDL_PH_MDS_POS_SM_GR' WHERE PARAMETER_ID='27808';
