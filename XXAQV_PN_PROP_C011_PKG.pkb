create or replace PACKAGE BODY xxaqv_pn_prop_pkg AS
--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Package Body                                                                            *
-- * Application Module : Arqiva Custom Application (xxaqv)                                                       *
-- * Packagage Name     : XXAQV_PN_PROP_PKG                                                                       *
-- * Script Name        : XXAQV_PN_PROP_PKG.pkb                                                                   *
-- * Purpose            : Used for migrating properties from 12.1.3 to 12.2.9                                     *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 1.0         CTS               19/05/2020     Initial Version                                                 *
-- ****************************************************************************************************************/

  /* Global Variables
  */

   gv_module_name         VARCHAR2(80) := 'XXAQV_PN_PROP_PKG';
   gv_staging_table       VARCHAR2(80) := 'XXAQV_PN_PROP_C011_STG';
   gv_staging_table1      VARCHAR2(80) := 'XXAQV_PN_LOC_C011_STG';
   gv_module              VARCHAR2(75) := NULL;
   gv_conv_mode           VARCHAR2(50) := NULL;
   gv_debug_flag          VARCHAR2(10) := NULL;
   gn_commit_cnt          NUMBER;
   gv_legacy_id_from      VARCHAR2(40) := NULL;
   gv_legacy_id_to        VARCHAR2(40) := NULL;
   gv_process_flag        VARCHAR2(5);
   gn_retcode             NUMBER;
   gv_err_msg             VARCHAR2(4000);
   gn_request_id          NUMBER       := fnd_global.conc_request_id;
   gn_user_id             NUMBER       := fnd_global.user_id;
   gn_login_id            NUMBER       := fnd_global.login_id;
   gn_org_id              NUMBER;      
   gd_sys_date            DATE         := sysdate;
   gv_load_success        VARCHAR2(80) := 'Load Success';
   gv_validate_success    VARCHAR2(80) := 'Validate Success';
   gv_validate_error      VARCHAR2(80) := 'Validate Error';
   gv_import_success      VARCHAR2(80) := 'Import Success';
   gv_import_error        VARCHAR2(80) := 'Import Error';
   gn_batch_run           NUMBER;
   gn_debug_level         NUMBER := 3; -- 0-only exceptions -- 1-Info Debugs -- 2-all debugs messages -- 3 No logs -- use wtih p_debug_flag   
   gv_log_type            VARCHAR2(10) := 'A'; -- E:exceptions ; A:All debugs   


   -- Global PLSQL Types
  TYPE gt_xxaqv_pn_prop_type IS
    TABLE OF xxaqv_pn_prop_c011_stg%rowtype INDEX BY BINARY_INTEGER;
  TYPE gt_xxaqv_pn_loc_type IS
    TABLE OF xxaqv_pn_loc_c011_stg%rowtype INDEX BY BINARY_INTEGER;


   -- Global Records
   gt_xxaqv_pn_prop_tab     gt_xxaqv_pn_prop_type;
   gt_xxaqv_pn_loc_tab      gt_xxaqv_pn_loc_type;


--/****************************************************************************************************************
-- * Procedure : print_debug                                                                                      *
-- * Purpose   : This procedure will print debug messages to program log file                                     *
-- ****************************************************************************************************************/

   PROCEDURE print_debug ( p_err_msg    IN   VARCHAR2
                         , p_log_type   IN   VARCHAR2 DEFAULT 'A')
   IS
   BEGIN
      IF gn_debug_level = 0 AND p_log_type = 'E'
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs(p_err_msg);
      ELSIF gn_debug_level = 1 THEN
         xxaqv_conv_cmn_utility_pkg.print_logs(p_err_msg);
      END IF;
   END print_debug;


--/****************************************************************************************************************
-- * Procedure : print_report                                                                                     *
-- * Purpose   : This procedure will print process report to concurrent program output                            *
-- ****************************************************************************************************************/

   PROCEDURE print_report 
   IS

    CURSOR lcu_prop_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_PROP_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'IMPORT' , gv_import_error);

    CURSOR lcu_loc_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_LOC_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'TIEBACK' , gv_import_error);								 

      lcu_prop_data  lcu_prop_errors%rowtype;	
      lcu_loc_data   lcu_loc_errors%rowtype;	  

      ln_total_cnt   NUMBER := 0;
      ln_error_cnt   NUMBER := 0;
	  ln_total_cnt1  NUMBER := 0;
      ln_error_cnt1  NUMBER := 0;
   BEGIN
   IF gv_module = 'PROPERTY'
   THEN
      SELECT COUNT(*)
        INTO ln_total_cnt
        FROM XXAQV_PN_PROP_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt
        FROM XXAQV_PN_PROP_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'IMPORT' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Property Import Report *******************************','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs('','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Date:',30)                   || to_char(sysdate,'DD-Mon-RRRR HH24:MI:SS'),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Module:',30)                 || gv_module,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Conversion Mode:' ,30)       || gv_conv_mode,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier from:',30) || gv_legacy_id_from,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier to:',30)   || gv_legacy_id_to,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Commit Count:',30)           || gn_commit_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Debug Flag:',30)             || gv_debug_flag,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Total Records:',30)          || ln_total_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Successful Records:',30)     || (ln_total_cnt - ln_error_cnt),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Error Records:',30)          || ln_error_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Load Rate %age:',30)         || (((ln_total_cnt - ln_error_cnt) / ln_total_cnt) * 100),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs('','O');
      xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      IF ln_error_cnt > 0 
      THEN
	       xxaqv_conv_cmn_utility_pkg.print_logs('','O');

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Property Name|') || ('Property Code|')|| ('Error Message|'),'O');
		  OPEN lcu_prop_errors;
		  LOOP
		      FETCH lcu_prop_errors INTO lcu_prop_data;
			  EXIT WHEN lcu_prop_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_prop_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_prop_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_prop_data.property_name|| '|')
                                           				  || (lcu_prop_data.property_code|| '|')
				                                          ||  i.err_msg
                                                          , 'O');			    
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_prop_errors;		      


      ELSIF ln_error_cnt = 0 
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs('','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('**************************** No Errors To Report *******************************','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('' ,'O');
         xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      END IF;
 END IF;

 IF gv_module='PN_LOC'
 THEN
      SELECT COUNT(*)
        INTO ln_total_cnt1
        FROM XXAQV_PN_LOC_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt1
        FROM XXAQV_PN_LOC_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'TIEBACK' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Location Import Report *******************************','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs('','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Date:',30)                   || to_char(sysdate,'DD-Mon-RRRR HH24:MI:SS'),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Module:',30)                 || gv_module,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Conversion Mode:' ,30)       || gv_conv_mode,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier from:',30) || gv_legacy_id_from,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier to:',30)   || gv_legacy_id_to,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Commit Count:',30)           || gn_commit_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Debug Flag:',30)             || gv_debug_flag,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Total Records:',30)          || ln_total_cnt1,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Successful Records:',30)     || (ln_total_cnt1 - ln_error_cnt1),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Error Records:',30)          || ln_error_cnt1,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Load Rate %age:',30)         || (((ln_total_cnt1 - ln_error_cnt1) / ln_total_cnt1) * 100),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs('','O');
      xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      IF ln_error_cnt > 0 
      THEN
	       xxaqv_conv_cmn_utility_pkg.print_logs('','O');

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Property Name|') || ('Location Code|')|| ('Error Message|'),'O');
		  OPEN lcu_loc_errors;
		  LOOP
		      FETCH lcu_loc_errors INTO lcu_loc_data;
			  EXIT WHEN lcu_loc_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_loc_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_loc_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_loc_data.property_name|| '|')
                                           				  || (lcu_loc_data.location_code|| '|')
				                                          ||  i.err_msg
                                                          , 'O');			    
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_loc_errors;		      


      ELSIF ln_error_cnt = 0 
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs('','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('**************************** No Errors To Report *******************************','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('' ,'O');
         xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      END IF; 

 END IF;
   END print_report;

--/*****************************************************************************************************************
-- * Procedure  : validate_location_park                                                                           *
-- * Purpose    : Procedure to validate location park id                                                           *
-- *****************************************************************************************************************/

   PROCEDURE validate_location_park ( p_location_park   IN    VARCHAR2
                                  , x_location_park_id  OUT   VARCHAR2
                                  , x_retcode           OUT   NUMBER
                                  , x_err_msg           OUT   VARCHAR2
   )    IS
   BEGIN
      x_retcode   := 0;
      x_err_msg   := NULL;
      SELECT location_park_id
        INTO x_location_park_id
        FROM pn_location_parks
       WHERE upper(trim(name)) = upper(trim(p_location_park));

    EXCEPTION
      WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'LOCATION_PARK_ID: No Data Found for LOCATION PARK NAME ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'LOCATION_PARK_ID: Multiple records with same LOCATION PARK NAME ' ;
      WHEN OTHERS THEN
         x_retcode   := 1;
         x_err_msg   := 'LOCATION_PARK_ID: Unexpected error: ' || sqlerrm;
   END validate_location_park;

--/*****************************************************************************************************************
-- * Procedure  : validate_property_name                                                                           *
-- * Purpose    : Procedure to validate location park id                                                           *
-- *****************************************************************************************************************/

   PROCEDURE validate_property_name ( p_property_code   IN    VARCHAR2
                                    , x_property_id     OUT   NUMBER
                                    , x_retcode         OUT   NUMBER
                                    , x_err_msg         OUT   VARCHAR2
   )    IS
   BEGIN
      x_retcode   := 0;
      x_err_msg   := NULL;
      SELECT property_id
        INTO x_property_id
        FROM pn_properties_all
       WHERE property_code = p_property_code;

    EXCEPTION
      WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'Property_name: No Data Found for Property NAME ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'Property_name: Multiple records with same Property NAME ' ;
      WHEN OTHERS THEN
         x_retcode   := 1;
         x_err_msg   := 'Property_name: Unexpected error: ' || sqlerrm;
   END validate_property_name;


--/*****************************************************************************************************************
-- * Procedure  : validate_source                                                                                  *
-- * Purpose    : Procedure to validate location source                                                            *
-- *****************************************************************************************************************/

   PROCEDURE validate_source ( p_source       IN    VARCHAR2                                    
                             , x_source_exist OUT   VARCHAR2
                             , x_err_msg      OUT   VARCHAR2
   )    IS
    ln_source NUMBER := 0;
   BEGIN
    --  x_retcode   := 0;  
      x_err_msg   := NULL;
	 
  
     SELECT  COUNT(1)
      INTO ln_source      
	  FROM fnd_lookup_values
      WHERE upper(lookup_code) = upper(p_source);

      IF ln_source > 0 THEN
         x_source_exist   := 'Y';
         x_err_msg    := NULL;
      ELSE
         x_source_exist   := 'N';
         x_err_msg    := 'Source Empty';
      END IF;

    EXCEPTION
     /* WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'Source: No Data Found for Source  ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'Source: Multiple records with same SOURCE NAME ' ;*/
      WHEN OTHERS THEN
         x_source_exist   := 'N';
         x_err_msg   := 'Source: Unexpected error: ' || sqlerrm;
   END validate_source;
--/****************************************************************************************************************
-- * Procedure  : extract_prop_data                                                                               *
-- * Purpose    : This Procedure is used to load property data into staging Table                                 *
-- ****************************************************************************************************************/

   PROCEDURE extract_prop_data ( x_retcode        OUT   NUMBER
                               , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_prop 
      IS SELECT regexp_replace(ppa.property_name,'[^[!-~]]*'
                 ,' '
              )                   property_name
              , ppa.property_code          property_code             
              , ppa.zone                   zone                  
              , regexp_replace(
                 ppa.district
                 ,'[^[!-~]]*'
                 ,' '
              )                            district			  
              , ppa.country                country               
              , regexp_replace(
			  ppa.description,'[^[!-~]]*'
                 ,' '
              )              description           
              , ppa.portfolio              portfolio             
              , ppa.tenure                 tenure                
              , ppa.class                  class                 
              , ppa.property_status        property_status       
              , ppa.condition              condition             
              , ppa.active_property        active_property       
              , ppa.attribute_category     attribute_category    
              , ppa.attribute1             attribute1            
              , ppa.attribute2             attribute2            
              , ppa.attribute3             attribute3            
              , ppa.attribute4             attribute4            
              , ppa.attribute5             attribute5            
              , ppa.attribute6             attribute6            
              , ppa.attribute7             attribute7            
              , ppa.attribute8             attribute8            
              , ppa.attribute9             attribute9            
              , ppa.attribute10            attribute10           
              , ppa.attribute11            attribute11           
              , ppa.attribute12            attribute12           
              , ppa.attribute13            attribute13           
              , ppa.attribute14            attribute14           
              , ppa.attribute15            attribute15                    
              , hra.name                   ou_name               
              , plp.name                   location_park_name    
           FROM pn_properties_all@xxaqv_conv_cmn_dblink  ppa
              , pn_location_parks@xxaqv_conv_cmn_dblink  plp
              , hr_operating_units@xxaqv_conv_cmn_dblink hra
          WHERE ppa.attribute2 not like 'CX%'
            AND ppa.location_park_id = plp.location_park_id(+)
            AND ppa.org_id           = hra.organization_id
            AND (ppa.active_property  is null
             OR ppa.active_property  ='A')
            AND ppa.property_id between nvl(gv_legacy_id_from,ppa.property_id)and nvl(gv_legacy_id_to,ppa.property_id);			 

     -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN

      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: property_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: property_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_prop_tab.delete;

    FOR i IN lcu_prop
    LOOP

	print_debug(i.property_name);
	gt_xxaqv_pn_prop_tab(ln_line_count).property_name            := i.property_name;
	--gt_xxaqv_pn_prop_tab(ln_line_count).property_id              := pn_properties_s.nextval;
	gt_xxaqv_pn_prop_tab(ln_line_count).property_code            := i.property_code;     
	gt_xxaqv_pn_prop_tab(ln_line_count).zone                     := i.zone;                 
	gt_xxaqv_pn_prop_tab(ln_line_count).district                 := i.district;             
	gt_xxaqv_pn_prop_tab(ln_line_count).country                  := i.country;              
	gt_xxaqv_pn_prop_tab(ln_line_count).description              := i.description;          
	gt_xxaqv_pn_prop_tab(ln_line_count).portfolio                := i.portfolio;            
	gt_xxaqv_pn_prop_tab(ln_line_count).tenure                   := i.tenure;               
	gt_xxaqv_pn_prop_tab(ln_line_count).class                    := i.class;                
	gt_xxaqv_pn_prop_tab(ln_line_count).property_status          := i.property_status;      
	gt_xxaqv_pn_prop_tab(ln_line_count).condition                := i.condition;            
	gt_xxaqv_pn_prop_tab(ln_line_count).active_property          := i.active_property;      
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute_category       := i.attribute_category;   
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute1               := i.attribute1;           
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute2               := NULL;           
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute3               := i.attribute3;           
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute4               := i.attribute4;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute5               := i.attribute5;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute6               := i.attribute6;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute7               := i.attribute7;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute8               := i.attribute8;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute9               := i.attribute9;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute10              := i.attribute10;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute11              := i.attribute11;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute12              := i.attribute12;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute13              := i.attribute13;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute14              := i.attribute14;          
	gt_xxaqv_pn_prop_tab(ln_line_count).attribute15              := i.attribute15;          
	gt_xxaqv_pn_prop_tab(ln_line_count).ou_name                  := i.ou_name;              
	gt_xxaqv_pn_prop_tab(ln_line_count).location_park_name       := i.location_park_name;
    gt_xxaqv_pn_prop_tab(ln_line_count).creation_date            := SYSDATE;        
    gt_xxaqv_pn_prop_tab(ln_line_count).last_update_date         := SYSDATE;       
    gt_xxaqv_pn_prop_tab(ln_line_count).last_update_login        := gn_login_id;      
    gt_xxaqv_pn_prop_tab(ln_line_count).last_updated_by          := gn_user_id;       
    gt_xxaqv_pn_prop_tab(ln_line_count).created_by               := gn_user_id;            
    gt_xxaqv_pn_prop_tab(ln_line_count).process_flag             := gv_load_success;	
	ln_line_count                                                := ln_line_count + 1;

	END LOOP;
     BEGIN
          FORALL i IN gt_xxaqv_pn_prop_tab.first..gt_xxaqv_pn_prop_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_PROP_C011_STG VALUES gt_xxaqv_pn_prop_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_PROP_C011_STG:Property Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Property : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Property dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Property Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Property Unexpected Error:' 
                              || to_char(sqlcode) 
                              || '-' 
                              || sqlerrm;
         END;
 EXCEPTION
      WHEN OTHERS 
      THEN
         print_debug(gv_module
                    ||':'
                    ||'EXTRACT_DATA: Unexpected error in main Extract:' 
                    || sqlerrm  ,gv_log_type  );
    print_debug(
                  'Backtrace EXTRACT_DATA: ' || dbms_utility.format_error_stack
                  ,gv_log_type
               );
               print_debug(
                  'Backtrace EXTRACT_DATA: ' || dbms_utility.format_error_backtrace
                  ,gv_log_type
               );					
         x_retcode   := 1;
         x_err_msg   := gv_module
                        ||':'
                        ||'EXTRACT_DATA: Unexpected error in main Extract:' 
                        || to_char(sqlcode) 
                        || '-' 
                        || sqlerrm;
   END extract_prop_data;
--/****************************************************************************************************************
-- * Procedure  : extract_loc_data                                                                                *
-- * Purpose    : This Procedure is used to load Location data into staging Table                                 *
-- ****************************************************************************************************************/

   PROCEDURE extract_loc_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_loc 
      IS
	  SELECT pla.location_type_lookup_code   location_type_lookup_code
       , pla.location_code                   location_code
       , pla.location_alias                  location_alias
       , pla.building                        building
       , pla.lease_or_owned                  lease_or_owned
       , pla.floor                           floor
       , pla.office                          office
       , paa.address_line1                   address_line1
       , paa.address_line2                   address_line2
       , paa.address_line3                   address_line3
       , paa.address_line4                   address_line4
       , paa.county                          county
       , paa.city                            city
       , paa.state                           state
       , paa.province                        province
       , paa.zip_code                        zip_code
       , paa.country                         country
       , paa.address_style                   address_style
       , pla.max_capacity                    max_capacity
       , pla.optimum_capacity                optimum_capacity
       , pla.rentable_area                   rentable_area
       , pla.usable_area                     usable_area
       , pla.allocate_cost_center_code       allocate_cost_center_code
       , pla.attribute_category              attribute_category
       , pla.attribute1                      attribute1
       , pla.attribute2                      attribute2
       , pla.attribute3                      attribute3
       , pla.attribute4                      attribute4
       , pla.attribute5                      attribute5
       , pla.attribute6                      attribute6
       , pla.attribute7                      attribute7
       , pla.attribute8                      attribute8
       , pla.attribute9                      attribute9
       , pla.attribute10                     attribute10
       , pla.attribute11                     attribute11
       , pla.attribute12                     attribute12
       , pla.attribute13                     attribute13
       , pla.attribute14                     attribute14
       , pla.attribute15                     attribute15
       , paa.addr_attribute_category         addr_attribute_category
       , paa.addr_attribute1                 addr_attribute1
       , paa.addr_attribute2                 addr_attribute2
       , paa.addr_attribute3                 addr_attribute3
       , paa.addr_attribute4                 addr_attribute4
       , paa.addr_attribute5                 addr_attribute5
       , paa.addr_attribute6                 addr_attribute6
       , paa.addr_attribute7                 addr_attribute7
       , paa.addr_attribute8                 addr_attribute8
       , paa.addr_attribute9                 addr_attribute9
       , paa.addr_attribute10                addr_attribute10
       , paa.addr_attribute11                addr_attribute11
       , paa.addr_attribute12                addr_attribute12
       , paa.addr_attribute13                addr_attribute13
       , paa.addr_attribute14                addr_attribute14
       , paa.addr_attribute15                addr_attribute15
       , nvl(pla.source,'PROPERTY MANAGER IMPORT')     source
       , pla.status                          status
       , pla.space_type_lookup_code          space_type_lookup_code
       , pla.gross_area                      gross_area
       , pla.assignable_area                 assignable_area
       , pla.class                           class
       , pla.status_type                     status_type
       , pla.suite                           suite
       , pla.common_area                     common_area
       , pla.common_area_flag                common_area_flag
       , pla.function_type_lookup_code       function_type_lookup_code
       , pla.standard_type_lookup_code       standard_type_lookup_code
       , pla.active_start_date               active_start_date
       , pla.active_end_date                 active_end_date
       , regexp_replace(
	   ppa.property_name
	   ,'[^[!-~]]*'
          ,' '
              )                             property_name
       , ppa.property_code                  property_code
  FROM pn_locations_all@xxaqv_conv_cmn_dblink      pla
       , pn_addresses_all@xxaqv_conv_cmn_dblink    paa
       , pn_properties_all@xxaqv_conv_cmn_dblink   ppa
 WHERE pla.address_id   = paa.address_id
   AND ppa.property_id  = pla.property_id
   AND pla.attribute2 NOT LIKE 'CX%'
   AND pla.status       = 'A';	  

     -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN

      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: LOCATION_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: LOCATION_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_loc_tab.delete;

    FOR i IN lcu_loc
    LOOP	  
	  gt_xxaqv_pn_loc_tab(ln_line_count).property_name                     := i.property_name;
	  gt_xxaqv_pn_loc_tab(ln_line_count).property_code                     := i.property_code;
	  gt_xxaqv_pn_loc_tab(ln_line_count).batch_name                        := 'TESTBATCH';
	  gt_xxaqv_pn_loc_tab(ln_line_count).location_type_lookup_code         := i.location_type_lookup_code; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).location_code                     := i.location_code; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).location_alias                    := i.location_alias; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).building                          := i.building; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).lease_or_owned                    := i.lease_or_owned; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).floor                             := i.floor; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).office                            := i.office; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).address_line1                     := i.address_line1; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).address_line2                     := i.address_line2; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).address_line3                     := i.address_line3; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).address_line4                     := i.address_line4; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).county                            := i.county; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).city                              := i.city; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).state                             := i.state; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).province                          := i.province; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).zip_code                          := i.zip_code; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).country                           := i.country; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).address_style                     := i.address_style; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).max_capacity                      := i.max_capacity ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).optimum_capacity                  := i.optimum_capacity; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).rentable_area                     := i.rentable_area; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).usable_area                       := i.usable_area; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).allocate_cost_center_code         := i.allocate_cost_center_code; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute_category                := i.attribute_category; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute1                        := i.attribute1; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute2                        := null; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute3                        := i.attribute3; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute4                        := i.attribute4; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute5                        := i.attribute5; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute6                        := i.attribute6; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute7                        := i.attribute7; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute8                        := i.attribute8; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute9                        := i.attribute9; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute10                       := i.attribute10; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute11                       := i.attribute11; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute12                       := i.attribute12; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute13                       := i.attribute13; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute14                       := i.attribute14; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).attribute15                       := i.attribute15; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute_category           := i.addr_attribute_category; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute1                   := i.addr_attribute1 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute2                   := i.addr_attribute2 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute3                   := i.addr_attribute3 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute4                   := i.addr_attribute4 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute5                   := i.addr_attribute5 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute6                   := i.addr_attribute6 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute7                   := i.addr_attribute7 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute8                   := i.addr_attribute8 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute9                   := i.addr_attribute9 ; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute10                  := i.addr_attribute10; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute11                  := i.addr_attribute11; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute12                  := i.addr_attribute12; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute13                  := i.addr_attribute13; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute14                  := i.addr_attribute14; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).addr_attribute15                  := i.addr_attribute15; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).source                            := i.source; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).status_type                       := i.status; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).space_type_lookup_code            := i.space_type_lookup_code; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).gross_area                        := i.gross_area; 
	  gt_xxaqv_pn_loc_tab(ln_line_count).assignable_area                   := i.assignable_area;                   
	  gt_xxaqv_pn_loc_tab(ln_line_count).class                             := i.class;                             
	  gt_xxaqv_pn_loc_tab(ln_line_count).status_type                       := i.status_type;                       
	  gt_xxaqv_pn_loc_tab(ln_line_count).suite                             := i.suite;                             
	  gt_xxaqv_pn_loc_tab(ln_line_count).common_area                       := i.common_area;                       
	  gt_xxaqv_pn_loc_tab(ln_line_count).common_area_flag                  := i.common_area_flag;                  
	  gt_xxaqv_pn_loc_tab(ln_line_count).function_type_lookup_code         := i.function_type_lookup_code;         
	  gt_xxaqv_pn_loc_tab(ln_line_count).standard_type_lookup_code         := i.standard_type_lookup_code;         
	  gt_xxaqv_pn_loc_tab(ln_line_count).active_start_date                 := i.active_start_date;                 
	  gt_xxaqv_pn_loc_tab(ln_line_count).active_end_date                   := i.active_end_date;
      gt_xxaqv_pn_loc_tab(ln_line_count).creation_date                     := SYSDATE;        
      gt_xxaqv_pn_loc_tab(ln_line_count).last_update_date                  := SYSDATE;       
      gt_xxaqv_pn_loc_tab(ln_line_count).last_update_login                 := gn_login_id;      
      gt_xxaqv_pn_loc_tab(ln_line_count).last_updated_by                   := gn_user_id;       
      gt_xxaqv_pn_loc_tab(ln_line_count).created_by                        := gn_user_id;            
      gt_xxaqv_pn_loc_tab(ln_line_count).process_flag                      := gv_load_success;      	  
      ln_line_count                                                        := ln_line_count + 1;
	END LOOP;

	 BEGIN
          FORALL i IN gt_xxaqv_pn_loc_tab.first..gt_xxaqv_pn_loc_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_LOC_C011_STG VALUES gt_xxaqv_pn_loc_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_LOC_C011_STG:Location Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Location : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Location dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Property Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Location Unexpected Error:' 
                              || to_char(sqlcode) 
                              || '-' 
                              || sqlerrm;
         END;
 EXCEPTION
      WHEN OTHERS 
      THEN
         print_debug(gv_module
                    ||':'
                    ||'EXTRACT_DATA: Unexpected error in main Extract:' 
                    || sqlerrm  ,gv_log_type  );
    print_debug(
                  'Backtrace EXTRACT_DATA: ' || dbms_utility.format_error_stack
                  ,gv_log_type
               );
               print_debug(
                  'Backtrace EXTRACT_DATA: ' || dbms_utility.format_error_backtrace
                  ,gv_log_type
               );					
         x_retcode   := 1;
         x_err_msg   := gv_module
                        ||':'
                        ||'EXTRACT_DATA: Unexpected error in main Extract:' 
                        || to_char(sqlcode) 
                        || '-' 
                        || sqlerrm;
   END extract_loc_data;

--/****************************************************************************************************************
-- * Procedure : validate_staging_records                                                                         *
-- * Purpose   : This Procedure validate the records in the staging table.                                        *
-- ****************************************************************************************************************/	

   PROCEDURE validate_prop_records ( x_retcode        OUT   NUMBER
                                    , x_err_msg        OUT   VARCHAR2 )
     IS	

     --Local Variables
     gn_created_by                    NUMBER         := fnd_global.user_id;
     l_val_status                     VARCHAR2(10)   := NULL;
     l_val_flag                       VARCHAR2(100);
     ln_error_message                 VARCHAR2(4000) := NULL;
     ln_mstr_flag                     VARCHAR2(10);
	 ln_org_id                        NUMBER;
	 lv_error_message                 VARCHAR2(4000) := NULL;


     CURSOR lcu_prop 
	 IS
	 SELECT property_name
          , property_code                                
          , ou_name               
          , location_park_name 
		  , process_flag
		  , error_message
		  , x_location_park_id
		  , x_org_id
          , rowid		  
       FROM XXAQV_PN_PROP_C011_STG
      WHERE process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

      TYPE lcu_prop_type IS
         TABLE OF lcu_prop%rowtype INDEX BY BINARY_INTEGER;
      lcu_prop_tab     lcu_prop_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: property_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: property_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --

      OPEN lcu_prop;
      LOOP
         lcu_prop_tab.DELETE;
         FETCH lcu_prop BULK COLLECT INTO lcu_prop_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_prop_tab.count = 0;
         --
         FOR i IN lcu_prop_tab.first..lcu_prop_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;

            --validate location_park_id
           IF lcu_prop_tab(i).location_park_name IS NOT NULL THEN
               validate_location_park( p_location_park      => lcu_prop_tab(i).location_park_name
                                     , x_location_park_id   => lcu_prop_tab(i).x_location_park_id
                                     , x_retcode            => gn_retcode
                                     , x_err_msg            => lv_error_message
               );                    

               IF gn_retcode <> 0 THEN
                  lcu_prop_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;			
           END IF;
           --validate org_id
         ln_org_id  := xxaqv_conv_cmn_utility_pkg.validate_oper_unit(lcu_prop_tab(i).ou_name);
         IF ln_org_id = 0 THEN
            lcu_prop_tab(i).process_flag   := gv_validate_error;
            gv_process_flag                      := 'E';        
            gv_err_msg        := gv_err_msg|| ';' || 'Invalid Operating Unit';           
         ELSE
            lcu_prop_tab(i).x_org_id := ln_org_id;
         END IF;		   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_prop_tab(i).error_message := gv_err_msg;
			    	lcu_prop_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_prop_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_prop_tab.first..lcu_prop_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_PROP_C011_STG
               SET x_location_park_id    = lcu_prop_tab(i).x_location_park_id
                 , x_org_id              = lcu_prop_tab(i).x_org_id
                 , process_flag          = lcu_prop_tab(i).process_flag
				 , error_message         = lcu_prop_tab(i).error_message
            WHERE
                1 = 1
                AND ROWID = lcu_prop_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_PROP_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
            COMMIT;        

           EXCEPTION
            WHEN ex_dml_errors THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
               --
               print_debug( 'VALIDATE_RECORDS: Number of FORALL Update failures: ' || ln_error_count,gv_log_type );
               --
               FOR i IN 1..ln_error_count LOOP print_debug('VALIDATE_RECORDS: FORALL Update Error: ' || i || ' Array Index: ' || SQL%bulk_exceptions(i)
               .error_index || 'Message: ' || sqlerrm(-SQL%bulk_exceptions(i).error_code));
               END LOOP;

            WHEN OTHERS THEN
               print_debug( 'VALIDATE_RECORDS: FORALL Update Unexpected Error: ' || sqlerrm ,gv_log_type);
               x_retcode   := 1;
               x_err_msg   := 'VALIDATE_RECORDS: FORALL Update Unexpected Error:' || to_char(sqlcode) || '-' || sqlerrm;
         END;

      END LOOP; -- open cursor

      CLOSE lcu_prop;
      print_debug('VALIDATE_RECORDS: END Validate Property table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_prop_records;		 

--/****************************************************************************************************************
-- * Procedure : validate_loc_records                                                                             *
-- * Purpose   : This Procedure validate the location records in the staging table.                               *
-- ****************************************************************************************************************/	

   PROCEDURE validate_loc_records ( x_retcode        OUT   NUMBER
                                    , x_err_msg        OUT   VARCHAR2 )
     IS	

     --Local Variables
     gn_created_by                    NUMBER         := fnd_global.user_id;
     l_val_status                     VARCHAR2(10)   := NULL;
     l_val_flag                       VARCHAR2(100);
     ln_error_message                 VARCHAR2(4000) := NULL;
     ln_mstr_flag                     VARCHAR2(10);
	  ln_org_id                        NUMBER;
	  lv_error_message                 VARCHAR2(4000) := NULL;
     ln_source                        VARCHAR2(200);
     lv_source_exist                  VARCHAR2(20);


     CURSOR lcu_loc 
	 IS
	 SELECT property_name
	      , property_code
	      , x_property_id
	      , rowid
          , process_flag
		  , error_message
        , source
      FROM XXAQV_PN_LOC_C011_STG
	  WHERE process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

 TYPE lcu_loc_type IS
         TABLE OF lcu_loc%rowtype INDEX BY BINARY_INTEGER;
      lcu_loc_tab        lcu_loc_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: LOCATION_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: LOCATION_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --

      OPEN lcu_loc;
      LOOP
         lcu_loc_tab.DELETE;
         FETCH lcu_loc BULK COLLECT INTO lcu_loc_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_loc_tab.count = 0;
         --
         FOR i IN lcu_loc_tab.first..lcu_loc_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;
         lv_source_exist   := NULL;

               validate_property_name( p_property_code      => lcu_loc_tab(i).property_code
                                     , x_property_id        => lcu_loc_tab(i).x_property_id
                                     , x_retcode            => gn_retcode
                                     , x_err_msg            => lv_error_message
               );                    
               IF gn_retcode <> 0 THEN
                  lcu_loc_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;	

                      validate_source( p_source      => lcu_loc_tab(i).source                                   
                                     , x_source_exist            => lv_source_exist
                                     , x_err_msg            => lv_error_message
               );                    
               IF lv_source_exist ='N' THEN
                  lcu_loc_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;			   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_loc_tab(i).error_message := gv_err_msg;
			    	lcu_loc_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_loc_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_loc_tab.first..lcu_loc_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_LOC_C011_STG
               SET x_property_id         = lcu_loc_tab(i).x_property_id
			   	 , process_flag          = lcu_loc_tab(i).process_flag
				 , error_message         = lcu_loc_tab(i).error_message			   			   
            WHERE
                1 = 1
                AND ROWID = lcu_loc_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_LOC_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
            COMMIT;        

           EXCEPTION
            WHEN ex_dml_errors THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
               --
               print_debug( 'VALIDATE_RECORDS: Number of FORALL Update failures: ' || ln_error_count,gv_log_type );
               --
               FOR i IN 1..ln_error_count LOOP print_debug('VALIDATE_RECORDS: FORALL Update Error: ' || i || ' Array Index: ' || SQL%bulk_exceptions(i)
               .error_index || 'Message: ' || sqlerrm(-SQL%bulk_exceptions(i).error_code));
               END LOOP;

            WHEN OTHERS THEN
               print_debug( 'VALIDATE_RECORDS: FORALL Update Unexpected Error: ' || sqlerrm ,gv_log_type);
               x_retcode   := 1;
               x_err_msg   := 'VALIDATE_RECORDS: FORALL Update Unexpected Error:' || to_char(sqlcode) || '-' || sqlerrm;
         END;

      END LOOP; -- open cursor

      CLOSE lcu_loc;
      print_debug('VALIDATE_RECORDS: END Validate Location table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_loc_records;

--/****************************************************************************************************************
-- * Procedure  : IMPORT_PROP_DATA                                                                                *
-- * Purpose    : This Procedure is used to import the data for the properties                                    *
-- ****************************************************************************************************************/  

   PROCEDURE import_prop_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2)
   IS

   ln_row_id   VARCHAR2(500) :=null;
   ln_property_id NUMBER :=null;
-- This Cursor is used to retrieve information from Staging Table --
      CURSOR lcu_select IS
	  SELECT regexp_replace(property_name,'[^[!-~]]*'
                 ,' '
              )                   property_name
           , property_code             
           , zone                  
            , regexp_replace(
                 district
                 ,'[^[!-~]]*'
                 ,' '
              )                            district            
           , country               
           , regexp_replace(
			  description,'[^[!-~]]*'
                 ,' '
              )              description          
           , portfolio             
           , tenure                
           , class                 
           , property_status       
           , condition             
           , active_property       
           , attribute_category    
           , attribute1            
           , attribute2            
           , attribute3            
           , attribute4            
           , attribute5            
           , attribute6            
           , attribute7            
           , attribute8            
           , attribute9            
           , attribute10           
           , attribute11           
           , attribute12           
           , attribute13           
           , attribute14           
           , attribute15                    
           , x_org_id               
           , x_location_park_id 
           , property_id
           , last_update_date		 
           , last_updated_by   
           , creation_date     
           , created_by        
		     , last_update_login 
           , rowid
        FROM XXAQV_PN_PROP_C011_STG
       WHERE process_flag   = 'Validate Success';
	   --and rownum<15;
  BEGIN


      FOR lcu_r_cur_select IN lcu_select 
      LOOP 
	  ln_row_id :=null;
	   ln_property_id := null;
         pnt_properties_pkg.INSERT_ROW 
(
              X_ORG_ID              => lcu_r_cur_select.x_org_id  
            , X_ROWID               => ln_row_id
            , X_PROPERTY_ID         => ln_property_id  
            , X_LAST_UPDATE_DATE    => lcu_r_cur_select.last_update_date  
            , X_LAST_UPDATED_BY     => lcu_r_cur_select.last_updated_by  
            , X_CREATION_DATE       => lcu_r_cur_select.CREATION_DATE  
            , X_CREATED_BY          => lcu_r_cur_select.CREATED_BY  
            , X_LAST_UPDATE_LOGIN   => lcu_r_cur_select.LAST_UPDATE_LOGIN  
            , X_PROPERTY_NAME       => lcu_r_cur_select.PROPERTY_NAME  
            , X_PROPERTY_CODE       => lcu_r_cur_select.PROPERTY_CODE  
            , X_LOCATION_PARK_ID    => lcu_r_cur_select.X_LOCATION_PARK_ID  
            , X_ZONE                => lcu_r_cur_select.ZONE  
            , X_DISTRICT            => lcu_r_cur_select.DISTRICT  
            , X_COUNTRY             => lcu_r_cur_select.COUNTRY  
            , X_DESCRIPTION         => lcu_r_cur_select.DESCRIPTION  
            , X_PORTFOLIO           => lcu_r_cur_select.PORTFOLIO  
            , X_TENURE              => lcu_r_cur_select.TENURE  
            , X_CLASS               => lcu_r_cur_select.CLASS  
            , X_PROPERTY_STATUS     => lcu_r_cur_select.PROPERTY_STATUS  
            , X_CONDITION           => lcu_r_cur_select.CONDITION  
            , X_ACTIVE_PROPERTY     => lcu_r_cur_select.ACTIVE_PROPERTY  
            , X_ATTRIBUTE_CATEGORY  => lcu_r_cur_select.ATTRIBUTE_CATEGORY  
            , X_ATTRIBUTE1          => lcu_r_cur_select.ATTRIBUTE1  
            , X_ATTRIBUTE2          => lcu_r_cur_select.ATTRIBUTE2  
            , X_ATTRIBUTE3          => lcu_r_cur_select.ATTRIBUTE3  
            , X_ATTRIBUTE4          => lcu_r_cur_select.ATTRIBUTE4  
            , X_ATTRIBUTE5          => lcu_r_cur_select.ATTRIBUTE5  
            , X_ATTRIBUTE6          => lcu_r_cur_select.ATTRIBUTE6  
            , X_ATTRIBUTE7          => lcu_r_cur_select.ATTRIBUTE7  
            , X_ATTRIBUTE8          => lcu_r_cur_select.ATTRIBUTE8  
            , X_ATTRIBUTE9          => lcu_r_cur_select.ATTRIBUTE9  
            , X_ATTRIBUTE10         => lcu_r_cur_select.ATTRIBUTE10  
            , X_ATTRIBUTE11         => lcu_r_cur_select.ATTRIBUTE11  
            , X_ATTRIBUTE12         => lcu_r_cur_select.ATTRIBUTE12  
            , X_ATTRIBUTE13         => lcu_r_cur_select.ATTRIBUTE13  
            , X_ATTRIBUTE14         => lcu_r_cur_select.ATTRIBUTE14  
            , X_ATTRIBUTE15         => lcu_r_cur_select.ATTRIBUTE15);

         UPDATE XXAQV_PN_PROP_C011_STG 
            SET property_id   = ln_property_id
			  , process_flag  = gv_import_success 
		  WHERE property_code =lcu_r_cur_select.PROPERTY_CODE ; 
      COMMIT;


      END LOOP;
      --COMMIT;

	  UPDATE XXAQV_PN_PROP_C011_STG 
	     SET process_flag = gv_import_error 
	   WHERE property_id is null ; 
	  COMMIT;
     EXCEPTION

      WHEN OTHERS 
      THEN
         print_debug( 'IMPORT: Exception processing Properties Data:' || sqlerrm, gv_log_type );
         x_retcode   := 1;
         x_err_msg   := 'IMPORT: Unexpected error: ' || sqlerrm;
   END import_prop_data;	  


--/****************************************************************************************************************
-- * Procedure : load_loc_data                                                                                    *
-- * Purpose   : This procedure will load the records from staging table to interface table.                      *
-- ****************************************************************************************************************/

   PROCEDURE load_loc_data ( x_retcode        OUT   NUMBER
                           , x_err_msg        OUT   VARCHAR2 )
   IS

   CURSOR lcu_loc
   IS
   SELECT  batch_name                          
         , entry_type                          
         , r12_location_id                     
         , location_type_lookup_code           
         , location_code                       
         , lease_or_owned                      
         , floor                               
         , office                              
         , address_line1                       
         , address_line2                       
         , address_line3                       
         , address_line4                       
         , county                              
         , city                                
         , state                               
         , province                            
         , zip_code                            
         , country                             
         , address_style                       
         , max_capacity                        
         , optimum_capacity                    
         , rentable_area                       
         , usable_area                         
         , allocate_cost_center_code           
         , uom_code	                          
         , parent_location_id                  
         , last_update_date                    
         , last_update_login                   
         , created_by                          
         , creation_date                       
         , last_updated_by                     
         , attribute_category                  
         , attribute1                          
         , attribute2                          
         , attribute3                          
         , attribute4                          
         , attribute5                          
         , attribute6                          
         , attribute7                          
         , attribute8                          
         , attribute9                          
         , attribute10                         
         , attribute11                         
         , attribute12                         
         , attribute13                         
         , attribute14                         
         , attribute15                         
         , addr_attribute_category             
         , addr_attribute1                     
         , addr_attribute2                     
         , addr_attribute3                     
         , addr_attribute4                     
         , addr_attribute5                     
         , addr_attribute6                     
         , addr_attribute7                     
         , addr_attribute8                     
         , addr_attribute9                     
         , addr_attribute10                    
         , addr_attribute11                    
         , addr_attribute12                    
         , addr_attribute13                    
         , addr_attribute14                    
         , addr_attribute15                    
         , transferred_to_cad                  
         , transferred_to_pn                   
         , error_message                       
         , source                              
         , request_id                          
         , program_application_id              
         , program_id                          
         , program_update_date                 
         , space_type_lookup_code              
         , gross_area                          
         , assignable_area                     
         , class                               
         , status_type                         
         , suite                               
         , common_area                         
         , common_area_flag                    
         , function_type_lookup_code           
         , location_alias                      
         , x_property_id                       
         , standard_type_lookup_code           
         , active_start_date                   
         , active_end_date                     
         , change_mode                         
         , change_date                         
         , new_active_start_date               
         , new_active_end_date                 
         , site_id                             
         , building                            
         , property_name                       
         , process_flag                        
      FROM XXAQV_PN_LOC_C011_STG
	  WHERE process_flag = gv_validate_success;

	  BEGIN

	  DELETE FROM pn_locations_itf
	  WHERE transferred_to_pn ='N';

	  COMMIT;

	  FOR i in lcu_loc
	  LOOP
	  INSERT INTO pn_locations_itf
	  ( batch_name                          
         , entry_type                          
         , location_id                     
         , location_type_lookup_code           
         , location_code                       
         , lease_or_owned                      
         , floor                               
         , office                              
         , address_line1                       
         , address_line2                       
         , address_line3                       
         , address_line4                       
         , county                              
         , city                                
         , state                               
         , province                            
         , zip_code                            
         , country                             
         , address_style                       
         , max_capacity                        
         , optimum_capacity                    
         , rentable_area                       
         , usable_area                         
         , allocate_cost_center_code           
         , uom_code	                          
         , parent_location_id                  
         , last_update_date                    
         , last_update_login                   
         , created_by                          
         , creation_date                       
         , last_updated_by                     
         , attribute_category                  
         , attribute1                          
         , attribute2                          
         , attribute3                          
         , attribute4                          
         , attribute5                          
         , attribute6                          
         , attribute7                          
         , attribute8                          
         , attribute9                          
         , attribute10                         
         , attribute11                         
         , attribute12                         
         , attribute13                         
         , attribute14                         
         , attribute15                         
         , addr_attribute_category             
         , addr_attribute1                     
         , addr_attribute2                     
         , addr_attribute3                     
         , addr_attribute4                     
         , addr_attribute5                     
         , addr_attribute6                     
         , addr_attribute7                     
         , addr_attribute8                     
         , addr_attribute9                     
         , addr_attribute10                    
         , addr_attribute11                    
         , addr_attribute12                    
         , addr_attribute13                    
         , addr_attribute14                    
         , addr_attribute15                                                       
         , source                                                      
         , space_type_lookup_code              
         , gross_area                          
         , assignable_area                     
         , class                               
         , status_type                         
         , suite                               
         , common_area                         
         , common_area_flag                    
         , function_type_lookup_code           
         , location_alias                      
         , property_id                       
         , standard_type_lookup_code           
         , active_start_date                   
         , active_end_date                     
         , change_mode                         
         , change_date                         
         , new_active_start_date               
         , new_active_end_date                 
         , site_id                             
         , building                            

	  )VALUES(
	       i.batch_name                          
         , 'A'       --entry_type A-> new, U->modify, R->Replace                   
         , PN_LOCATIONS_S.NEXTVAL                    
         , i.location_type_lookup_code           
         , i.location_code                       
         , i.lease_or_owned                      
         , i.floor                               
         , i.office                              
         , i.address_line1                       
         , i.address_line2                       
         , i.address_line3                       
         , i.address_line4                       
         , i.county                              
         , i.city                                
         , i.state                               
         , i.province                            
         , i.zip_code                            
         , i.country                             
         , i.address_style                       
         , i.max_capacity                        
         , i.optimum_capacity                    
         , i.rentable_area                       
         , i.usable_area                         
         , i.allocate_cost_center_code           
         , i.uom_code	                          
         , null --parent_location_id                  
         , i.last_update_date                    
         , i.last_update_login                   
         , i.created_by                          
         , i.creation_date                       
         , i.last_updated_by                     
         , i.attribute_category                  
         , i.attribute1                          
         , i.attribute2                          
         , i.attribute3                          
         , i.attribute4                          
         , i.attribute5                          
         , i.attribute6                          
         , i.attribute7                          
         , i.attribute8                          
         , i.attribute9                          
         , i.attribute10                         
         , i.attribute11                         
         , i.attribute12                         
         , i.attribute13                         
         , i.attribute14                         
         , i.attribute15                         
         , i.addr_attribute_category             
         , i.addr_attribute1                     
         , i.addr_attribute2                     
         , i.addr_attribute3                     
         , i.addr_attribute4                     
         , i.addr_attribute5                     
         , i.addr_attribute6                     
         , i.addr_attribute7                     
         , i.addr_attribute8                     
         , i.addr_attribute9                     
         , i.addr_attribute10                    
         , i.addr_attribute11                    
         , i.addr_attribute12                    
         , i.addr_attribute13                    
         , i.addr_attribute14                    
         , i.addr_attribute15                                         
         , i.source                                                                    
         , i.space_type_lookup_code              
         , i.gross_area                          
         , i.assignable_area                     
         , i.class                               
         , null--status_type                         
         , i.suite                               
         , i.common_area                         
         , i.common_area_flag                    
         , i.function_type_lookup_code           
         , i.location_alias                      
         , i.x_property_id                       
         , i.standard_type_lookup_code           
         , i.active_start_date                   
         , i.active_end_date                     
         , i.change_mode                         
         , i.change_date                         
         , i.new_active_start_date               
         , i.new_active_end_date                 
         , i.site_id                             
         , i.building                            

	  );
  COMMIT;
   END LOOP;
     EXCEPTION
      WHEN OTHERS THEN
         print_debug(
            'Load_interface  Unexpected Error: ' || sqlerrm
            ,gv_log_type
         );
         x_retcode   := 1;
         x_err_msg   := 'Load Interface: Unexpected Error: ' || sqlerrm;


      END load_loc_data;
--/****************************************************************************************************************
-- * Procedure : tie_back_loc                                                                                 *
-- * Purpose   : This procedure will tie back base table data to staging table.                                   *
-- ****************************************************************************************************************/

   PROCEDURE tie_back_loc ( x_retcode        OUT   NUMBER
                          , x_err_msg        OUT   VARCHAR2 )
   IS
      CURSOR lcu_success IS
      SELECT location_alias
	       , location_id
	       --, property_name
           , property_id
		   , transferred_to_pn
        FROM pn_locations_itf
		where transferred_to_pn='Y'
		AND error_message is null;

      CURSOR lcu_error IS

      SELECT location_alias
	       , location_id
	       --, property_name
           , property_id
		   , transferred_to_pn
		   , error_message
        FROM pn_locations_itf
		where transferred_to_pn='N';


      TYPE lcu_success_typ IS
         TABLE OF lcu_success%rowtype INDEX BY BINARY_INTEGER;
      lcu_success_tab   lcu_success_typ;

	   TYPE lcu_error_typ IS
         TABLE OF lcu_error%rowtype INDEX BY BINARY_INTEGER;
      lcu_error_tab   lcu_error_typ;

      ln_counter        BINARY_INTEGER := 0;
      ln_error_count    BINARY_INTEGER := 0;

	   ln_counter1        BINARY_INTEGER := 0;
      ln_error_count1     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
	  BEGIN
   BEGIN
      print_debug('TIEBACK_STAGING: Start Tieback staging and interface table data for Success Records');

          --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --
      print_debug('TIEBACK_STAGING: Updating Success Records');
      OPEN lcu_success;
      LOOP
         lcu_success_tab.DELETE;
         ln_counter := 0;
         FETCH lcu_success BULK COLLECT INTO lcu_success_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_success_tab.count = 0;
         BEGIN
            FORALL i IN lcu_success_tab.first..lcu_success_tab.last SAVE EXCEPTIONS
               UPDATE XXAQV_PN_LOC_C011_STG
                  SET process_flag         = gv_import_success
				  , r12_location_id        = lcu_success_tab(i).location_id
                WHERE 1 = 1
                  AND process_flag         = gv_validate_success
                  AND x_property_id        = lcu_success_tab(i).property_id;
            print_debug('TIEBACK_STAGING: XXAQV_PN_LOC_C011_STG:Success Records Updated: ' || SQL%rowcount);
            COMMIT;


         EXCEPTION
            WHEN ex_dml_errors THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
           --
               print_debug('TIEBACK_STAGING: Number of FORALL Update Success: ' || ln_error_count,gv_log_type );

               FOR i IN 1..ln_error_count LOOP print_debug('TIEBACK_STAGING: FORALL Update Success Error: ' || i || ' Array Index: ' || SQL%bulk_exceptions
               (i).error_index || 'Message: ' || sqlerrm(-SQL%bulk_exceptions(i).error_code));
               END LOOP;

            WHEN OTHERS THEN
               print_debug('TIEBACK_STAGING: FORALL Update Success Unexpected Error: ' || sqlerrm,gv_log_type );
               x_retcode   := 1;
               x_err_msg   := 'TIEBACK_STAGING: FORALL Update Success Unexpected Error:' || to_char(sqlcode) || '-' || sqlerrm;
         END;

      END LOOP;

      CLOSE lcu_success;
END;


 BEGIN
      print_debug('TIEBACK_STAGING: Start Tieback staging and interface table data for error records');

          --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --
      print_debug('TIEBACK_STAGING: Updating Error Records');
      OPEN lcu_error;
      LOOP
         lcu_error_tab.DELETE;
         ln_counter1 := 0;
         FETCH lcu_error BULK COLLECT INTO lcu_error_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_error_tab.count = 0;
         BEGIN
            FORALL i IN lcu_error_tab.first..lcu_error_tab.last SAVE EXCEPTIONS
               UPDATE XXAQV_PN_LOC_C011_STG
                  SET process_flag         = gv_import_error
				  , r12_location_id        = lcu_error_tab(i).location_id
				  , error_message          = lcu_error_tab(i).error_message
                WHERE 1 = 1
                  AND process_flag         = gv_validate_success
                  AND x_property_id        = lcu_error_tab(i).property_id;
            print_debug('TIEBACK_STAGING: XXAQV_PN_LOC_C011_STG:Error Records Updated: ' || SQL%rowcount);
            COMMIT;


         EXCEPTION
            WHEN ex_dml_errors THEN
               x_retcode        := 1;
               ln_error_count1   := SQL%bulk_exceptions.count;
           --
               print_debug('TIEBACK_STAGING: Number of FORALL Update error: ' || ln_error_count1,gv_log_type );

               FOR i IN 1..ln_error_count1 LOOP print_debug('TIEBACK_STAGING: FORALL Update error Error: ' || i || ' Array Index: ' || SQL%bulk_exceptions
               (i).error_index || 'Message: ' || sqlerrm(-SQL%bulk_exceptions(i).error_code));
               END LOOP;

            WHEN OTHERS THEN
               print_debug('TIEBACK_STAGING: FORALL Update error Unexpected Error: ' || sqlerrm,gv_log_type );
               x_retcode   := 1;
               x_err_msg   := 'TIEBACK_STAGING: FORALL Update error Unexpected Error:' || to_char(sqlcode) || '-' || sqlerrm;
         END;

      END LOOP;

      CLOSE lcu_error;
END;


   EXCEPTION
      WHEN OTHERS THEN
         print_debug('TIEBACK_STAGING: Unexpected Error: '    || sqlerrm,gv_log_type);
         x_retcode   := 'TIEBACK_STAGING: Unexpected error: ' || to_char(sqlcode) || '-' || sqlerrm;
         x_retcode   := 1;

   END tie_back_loc;

--/****************************************************************************************************************
-- * Procedure  : MAIN                                                                                            *
-- * Purpose    : This Procedure is the main procedure                                                            *
-- ****************************************************************************************************************/     

PROCEDURE main( errbuff            OUT   VARCHAR2
              , retcode            OUT   NUMBER
              , p_module           IN    VARCHAR2
              , p_conv_mode        IN    VARCHAR2
              , p_legacy_id_from   IN    VARCHAR2
              , p_legacy_id_to     IN    VARCHAR2
              , p_commit_cnt       IN    NUMBER
              , p_debug_flag       IN    VARCHAR2 
   ) IS

    lv_err_msg   VARCHAR2(4000);
    ln_retcode   NUMBER;
    ex_errors    EXCEPTION;

    BEGIN    
        print_debug( 'MAIN: START  Process' , gv_log_type   );       

        ln_retcode        := 0;
        lv_err_msg        := NULL;
        gv_module         := p_module;
        gv_conv_mode      := p_conv_mode;
        gv_legacy_id_from := p_legacy_id_from; 
        gv_legacy_id_to   := p_legacy_id_to; 
        gn_commit_cnt     := p_commit_cnt;  
        gv_debug_flag     := p_debug_flag;
        gv_process_flag   := 'S';
        gn_retcode        := 0;
        gv_err_msg        := NULL;
      --
       IF gv_debug_flag = 'YES' THEN
         gn_debug_level := 1;
      ELSIF gv_debug_flag = 'NO' THEN
         gn_debug_level   := 0;
         gv_log_type      := 'E';
      END IF;

    IF gv_module='PROPERTY'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_PROP_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_prop_data( x_retcode       => ln_retcode
                                     , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_prop_records( x_retcode       => ln_retcode
                                  , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'IMPORT' 
        THEN
             import_prop_data ( x_retcode       => ln_retcode
                              , x_err_msg       => lv_err_msg);

             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


    END IF;

	IF gv_module='PN_LOC'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_LOC_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_loc_data( x_retcode       => ln_retcode
                                    , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_loc_records( x_retcode       => ln_retcode
                                 , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'LOAD' 
        THEN
             load_loc_data ( x_retcode       => ln_retcode
                           , x_err_msg       => lv_err_msg);

             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;

         IF gv_conv_mode = 'TIEBACK' 
            THEN
             tie_back_loc ( x_retcode       => ln_retcode
                           , x_err_msg       => lv_err_msg);
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
          END IF;
    END IF;
      --invoke report procedure

      print_report;
      --
      errbuff  := lv_err_msg;
      retcode  := ln_retcode;
      print_debug( 'MAIN: END property Import Process'
                 , gv_log_type   );
   EXCEPTION
      WHEN ex_errors 
      THEN
         print_debug( 'MAIN: Exception processing property Data ex_errors:' || lv_err_msg
                    , gv_log_type );
         errbuff   := 'MAIN: Exception processing property Data:' || lv_err_msg;
         retcode   := 2;
      WHEN OTHERS 
      THEN
         print_debug( 'MAIN: Exception processing property Data:' || lv_err_msg
                    , gv_log_type );
         errbuff   := 'MAIN: Unexpected Exception processing property Data:' || to_char(sqlcode) || '-' || sqlerrm;
         retcode   := 2;
   END main;
   --
END xxaqv_pn_prop_pkg;
/
SHOW ERRORS
/