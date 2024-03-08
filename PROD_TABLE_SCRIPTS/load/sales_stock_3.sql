CREATE OR REPLACE PROCEDURE ASPSDL_RAW.DEV_SALESSTOCK_PREPROCESSING("PARAM" ARRAY)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
from snowflake.snowpark.functions import col,lit,current_timestamp
import pandas as pd
from datetime import datetime
from snowflake.snowpark import Row 

def main(session: snowpark.Session,Param): 
    

	
    try:


        file_name       = Param[0]
        stage_name      = Param[1]
        temp_stage_path = Param[2]
        db_name         = stage_name.split(''.'')[0]
        target_table    = db_name+"."+Param[3]
        target_raw_table= target_table+"_raw"
        all_months      = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"] 
        sls_stk_mth     = {"01":["JAN_SLS","JAN_STOCK"],
                           "02":["FEB_SLS","FEB_STOCK"],
                           "03":["MAR_SLS","MAR_STOCK"],
                           "04":["APR_SLS","APR_STOCK"],
                           "05":["MAY_SLS","MAY_STOCK"],
                           "06":["JUN_SLS","JUN_STOCK"],
                           "07":["JUL_SLS","JUL_STOCK"],
                           "08":["AUG_SLS","AUG_STOCK"],
                           "09":["SEP_SLS","SEP_STOCK"],
                           "10":["OCT_SLS","OCT_STOCK"],
                           "11":["NOV_SLS","NOV_STOCK"],
                           "12":["DEC_SLS","DEC_STOCK"]}
        header_dict     = {}
        transformed_rows = []
        final_df = None
        


        df_schema=StructType([
            StructField("temp_col",StringType()),
            StructField("dcl_code",StringType()),
            StructField("sap_code",StringType()),
            StructField("reference",StringType()),
            StructField("product_desc",StringType()),
            StructField("size",StringType()),
            StructField("rsp",StringType()),
            StructField("c_sls_qty",StringType()),
            StructField("c_sls_amt",StringType()),
            StructField("c_stock_qty",StringType()),
            StructField("c_stock_amt",StringType()),
            StructField("buffer",StringType()),
            StructField("mix",StringType()),
            StructField("r_3m",StringType()),
            StructField("comparison",StringType()),
            StructField("jan_sls",StringType()),
            StructField("jan_stock",StringType()),
            StructField("feb_sls",StringType()),
            StructField("feb_stock",StringType()),
            StructField("mar_sls",StringType()),
            StructField("mar_stock",StringType()),
            StructField("apr_sls",StringType()),
            StructField("apr_stock",StringType()),
            StructField("may_sls",StringType()),
            StructField("may_stock",StringType()),
            StructField("jun_sls",StringType()),
            StructField("jun_stock",StringType()),
            StructField("jul_sls",StringType()),
            StructField("jul_stock",StringType()),
            StructField("aug_sls",StringType()),
            StructField("aug_stock",StringType()),
            StructField("sep_sls",StringType()),
            StructField("sep_stock",StringType()),
            StructField("oct_sls",StringType()),
            StructField("oct_stock",StringType()),
            StructField("nov_sls",StringType()),
            StructField("nov_stock",StringType()),
            StructField("dec_sls",StringType()),
            StructField("dec_stock",StringType())
            ])

        
        session.use_schema(stage_name.split(''.'')[0])

        
        sheet_dict={''LTM'': ''LOTTE MAIN'',''LTJ'': ''LOTTE JEJU'', ''SLM'': ''SHILLA MAIN'', ''SLJ'': ''SHILLA JEJU'',\
            ''HDC'': ''HDC'', ''SGM'': ''SHINSEGAE MAIN'',''SGB'':''SHINSEGAE BUSAN'' ,''HYUNDAI_DDM'': ''HYUNDAI DDM'',\
            ''HYUNDAI_COEX'': ''HYUNDAI COEX'', ''DONGWHA'': ''DONGWHA''}
                    

        
        for retailer_name,location_name in sheet_dict.items():
            stage_path="@{0}/{1}/{2}.csv".format(stage_name,temp_stage_path,retailer_name)
            print(''stage_path printing....'',stage_path)
            transformed_rows.clear()
            final_df = None
            df=None
            transformed_df =None

            try :
                df = session.read\
                .schema(df_schema)\
                .option("skip_header",3)\
                .option("field_delimiter", "\u0001")\
                .csv(stage_path)
            except Exception as e:
                error_message = f"Error: Sheet {retailer_name} is missing in excel OR {str(e)}"
                
            df_filter=df.filter(col("rsp") != "")


            for i in range(1,4):
                header_df = session.read.option("INFER_SCHEMA", True).option("field_delimiter", "\u0001").csv(stage_path)
                header_pandas=header_df.to_pandas()
                h_key="header_"+str(i)
                h_val=header_pandas.iloc[int(i)].tolist()
                header_dict[h_key]=h_val
                


            all_present = all(element in header_dict[''header_2''] for element in all_months)

            if all_present:
                for  row in df_filter.collect():
                    for month,value in sls_stk_mth.items():

                        new_row = row.as_dict()
                        new_row["month"] = month
                        new_row["sls_qty"] = row[value[0]]
                        new_row["stock_qty"] = row[value[1]]
                            

                        new_row["location_name"] = location_name
                        new_row["retailer_name"] = retailer_name
                        new_row["year"] = file_name.split(''_'')[1].split(''.'')[0]
                        new_row["file_name"] = file_name
    
                        final_row=Row(**new_row)
                            

                        transformed_rows.append(final_row)
      
                transformed_df = session.createDataFrame(transformed_rows)

                if transformed_df.count() == 0:
                    raise Exception("The excel data file is empty for sheet : ",retailer_name,". Please place a valid file!")
                else :
                     pass


                main_df=transformed_df.select(''location_name'',''retailer_name'',''year'',''month'',''dcl_code'',''sap_code'',\
                                                  ''reference'',''product_desc'',''size'',''rsp'',''c_sls_qty'',''c_sls_amt'',\
                                                  ''c_stock_qty'',''c_stock_amt'',''buffer'',''mix'',''r_3m'',''comparison'',\
                                                  ''sls_qty'',''stock_qty'',''file_name'')


                if not final_df:
                    final_df = main_df
                else:
                    final_df = final_df.unionByName(main_df)  




                final_df= final_df.filter(final_df["dcl_code"].isNotNull())


                if final_df.count() == 0 :
                    raise Exception("The excel data file is empty ! . Please place a valid file!")
                else :

                    final_df.write.mode("append").saveAsTable(target_table)  
            

                    final_raw_df=final_df.withColumn(''crt_dttm'',lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) 
                    final_raw_df.write.mode("append").saveAsTable(target_raw_table)        

                    final_df.write.copy_into_location("@"+stage_name+"/"+temp_stage_path+"/success/"+file_name,file_format_type="csv",header=True,OVERWRITE=True)
            else:
                raise Exception("Please check if we have all months data in excel")                 

        return "Success"

    except KeyError as key_error:

        error_message = f"KeyError: {str(key_error)}"
        return error_message

    
    except Exception as e:

        error_message = f"Error: {str(e)}"
        return error_message';
