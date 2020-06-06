create or replace PACKAGE BODY xxaqv_pn_lease_cont_pkg AS
--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Package Body                                                                            *
-- * Application Module : Arqiva Custom Application (xxaqv)                                                       *
-- * Packagage Name     : XXAQV_PN_LEASE_CONT_PKG                                                                 *
-- * Script Name        : XXAQV_PN_LEASE_CONT_PKG.pkb                                                             *
-- * Purpose            : Used for migrating lease contacts from 12.1.3 to 12.2.9                                 *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 1.0         CTS               29/05/2020     Initial Version                                                 *
-- ****************************************************************************************************************/

  /* Global Variables
  */

   gv_module_name         VARCHAR2(80) := 'XXAQV_PN_LEASE_CONT_PKG';
   gv_staging_table       VARCHAR2(80) := 'XXAQV_PN_COMP_C011_STG';
   gv_staging_table1      VARCHAR2(80) := 'XXAQV_PN_COMP_SITES_C011_STG';
   gv_staging_table2      VARCHAR2(80) := 'XXAQV_PN_CONT_C011_STG';
   gv_staging_table3      VARCHAR2(80) := 'XXAQV_PN_PHONE_C011_STG';
   gv_module              VARCHAR2(75) := NULL;
   gv_conv_mode           VARCHAR2(50) := NULL;
   gv_debug_flag          VARCHAR2(10) := NULL;
   gn_commit_cnt          NUMBER;
   gv_legacy_id_from      VARCHAR2(40) := NULL;
   gv_legacy_id_to        VARCHAR2(40) := NULL;
   gv_process_flag        VARCHAR2(5);
   gn_retcode             NUMBER;
   gv_db_name             VARCHAR2(80);
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
  TYPE gt_xxaqv_pn_comp_type IS
    TABLE OF xxaqv_pn_comp_c011_stg%rowtype INDEX BY BINARY_INTEGER;
  TYPE gt_xxaqv_pn_comp_sites_type IS
    TABLE OF xxaqv_pn_comp_sites_c011_stg%rowtype INDEX BY BINARY_INTEGER;
  TYPE gt_xxaqv_pn_cont_type IS
    TABLE OF xxaqv_pn_cont_c011_stg%rowtype INDEX BY BINARY_INTEGER;
  TYPE gt_xxaqv_pn_phone_type IS
    TABLE OF xxaqv_pn_phone_c011_stg%rowtype INDEX BY BINARY_INTEGER;


   -- Global Records
   gt_xxaqv_pn_comp_tab            gt_xxaqv_pn_comp_type;
   gt_xxaqv_pn_comp_sites_tab      gt_xxaqv_pn_comp_sites_type;
   gt_xxaqv_pn_cont_tab            gt_xxaqv_pn_cont_type;
   gt_xxaqv_pn_phone_tab           gt_xxaqv_pn_phone_type;   


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

    CURSOR lcu_comp_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_COMP_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'IMPORT' , gv_import_error);

    CURSOR lcu_comp_sites_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_COMP_SITES_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'IMPORT' , gv_import_error);	

    CURSOR lcu_cont_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_CONT_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'IMPORT' , gv_import_error);

    CURSOR lcu_phone_errors
	IS
	 SELECT * 
	   FROM XXAQV_PN_PHONE_C011_STG 
	  WHERE 1=1
        AND process_flag = DECODE(gv_conv_mode
                                 ,'MAP'     , gv_validate_error
                                 ,'IMPORT' , gv_import_error);									 

      lcu_comp_data         lcu_comp_errors%rowtype;	
      lcu_comp_sites_data   lcu_comp_sites_errors%rowtype;
      lcu_cont_data         lcu_cont_errors%rowtype;	
      lcu_phone_data        lcu_phone_errors%rowtype;	  

      ln_total_cnt   NUMBER := 0;
      ln_error_cnt   NUMBER := 0;
	  ln_total_cnt1  NUMBER := 0;
      ln_error_cnt1  NUMBER := 0;
      ln_total_cnt2  NUMBER := 0;
      ln_error_cnt2  NUMBER := 0;
	  ln_total_cnt3  NUMBER := 0;
      ln_error_cnt3  NUMBER := 0;	  
	  
   BEGIN
   IF gv_module = 'COMPANY'
   THEN
      SELECT COUNT(*)
        INTO ln_total_cnt
        FROM XXAQV_PN_COMP_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt
        FROM XXAQV_PN_COMP_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'IMPORT' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Company Import Report *******************************','O' );
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

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Company Name|') || ('Error Message|'),'O');
		  OPEN lcu_comp_errors;
		  LOOP
		      FETCH lcu_comp_errors INTO lcu_comp_data;
			  EXIT WHEN lcu_comp_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_comp_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_comp_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_comp_data.name|| '|')
                                           				  
				                                          ||  i.err_msg
                                                          , 'O');			    
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_comp_errors;		      


      ELSIF ln_error_cnt = 0 
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs('','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('**************************** No Errors To Report *******************************','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('' ,'O');
         xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      END IF;
 END IF;

 IF gv_module='COMPANY_SITES'
 THEN
      SELECT COUNT(*)
        INTO ln_total_cnt1
        FROM XXAQV_PN_COMP_SITES_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt1
        FROM XXAQV_PN_COMP_SITES_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'IMPORT' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Company Sites Import Report *******************************','O' );
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

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Company Name|') || ('Company Site Name|')|| ('Error Message|'),'O');
		  OPEN lcu_comp_sites_errors;
		  LOOP
		      FETCH lcu_comp_sites_errors INTO lcu_comp_sites_data;
			  EXIT WHEN lcu_comp_sites_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_comp_sites_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_comp_sites_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_comp_sites_data.company_name|| '|')
                                           				  || (lcu_comp_sites_data.name|| '|')
				                                          ||  i.err_msg
                                                          , 'O');			    
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_comp_sites_errors;		      


      ELSIF ln_error_cnt = 0 
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs('','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('**************************** No Errors To Report *******************************','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('' ,'O');
         xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      END IF; 

 END IF;
 
  IF gv_module='COMPANY_CONTACTS'
 THEN
      SELECT COUNT(*)
        INTO ln_total_cnt2
        FROM XXAQV_PN_CONT_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt2
        FROM XXAQV_PN_CONT_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'IMPORT' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Contacts Import Report *******************************','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs('','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Date:',30)                   || to_char(sysdate,'DD-Mon-RRRR HH24:MI:SS'),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Module:',30)                 || gv_module,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Conversion Mode:' ,30)       || gv_conv_mode,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier from:',30) || gv_legacy_id_from,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier to:',30)   || gv_legacy_id_to,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Commit Count:',30)           || gn_commit_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Debug Flag:',30)             || gv_debug_flag,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Total Records:',30)          || ln_total_cnt2,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Successful Records:',30)     || (ln_total_cnt2 - ln_error_cnt2),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Error Records:',30)          || ln_error_cnt2,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Load Rate %age:',30)         || (((ln_total_cnt2 - ln_error_cnt2) / ln_total_cnt2) * 100),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs('','O');
      xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      IF ln_error_cnt > 0 
      THEN
	       xxaqv_conv_cmn_utility_pkg.print_logs('','O');

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Company Name|') || ('Company Site Name|')||('First Name|')||('Last Name|')||
		  ('Email Address|')||('Error Message|'),'O');
		  OPEN lcu_cont_errors;
		  LOOP
		      FETCH lcu_cont_errors INTO lcu_cont_data;
			  EXIT WHEN lcu_cont_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_cont_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_cont_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_cont_data.company_name|| '|')
                                           				  || (lcu_cont_data.company_site_name|| '|')
														  || (lcu_cont_data.first_name|| '|')
														  || (lcu_cont_data.last_name|| '|')
														  || (lcu_cont_data.email_address|| '|')
				                                          ||  i.err_msg
                                                          , 'O');
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_cont_errors;		      


      ELSIF ln_error_cnt = 0 
      THEN
         xxaqv_conv_cmn_utility_pkg.print_logs('','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('**************************** No Errors To Report *******************************','O');
         xxaqv_conv_cmn_utility_pkg.print_logs('' ,'O');
         xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      END IF; 

 END IF;
 
  IF gv_module='COMPANY_PHONE'
 THEN
      SELECT COUNT(*)
        INTO ln_total_cnt3
        FROM XXAQV_PN_PHONE_C011_STG;

      SELECT COUNT(*)
        INTO ln_error_cnt3
        FROM XXAQV_PN_PHONE_C011_STG
       WHERE 1 = 1         
         AND process_flag = DECODE(gv_conv_mode
                                  ,'MAP'    ,gv_validate_error
                                  ,'IMPORT' ,gv_import_error); 

      xxaqv_conv_cmn_utility_pkg.print_logs('**************************** Phone Import Report *******************************','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs('','O' );
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Date:',30)                   || to_char(sysdate,'DD-Mon-RRRR HH24:MI:SS'),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Module:',30)                 || gv_module,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Conversion Mode:' ,30)       || gv_conv_mode,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier from:',30) || gv_legacy_id_from,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Legacy Identifier to:',30)   || gv_legacy_id_to,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Commit Count:',30)           || gn_commit_cnt,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Debug Flag:',30)             || gv_debug_flag,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Total Records:',30)          || ln_total_cnt3,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Successful Records:',30)     || (ln_total_cnt3 - ln_error_cnt3),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Error Records:',30)          || ln_error_cnt3,'O');
      xxaqv_conv_cmn_utility_pkg.print_logs(rpad('Load Rate %age:',30)         || (((ln_total_cnt3 - ln_error_cnt3) / ln_total_cnt3) * 100),'O');
      xxaqv_conv_cmn_utility_pkg.print_logs('','O');
      xxaqv_conv_cmn_utility_pkg.print_logs('***********************************************************************************','O');
      IF ln_error_cnt > 0 
      THEN
	       xxaqv_conv_cmn_utility_pkg.print_logs('','O');

		  xxaqv_conv_cmn_utility_pkg.print_logs(  ('Company Name|') || ('Company Site Name|')||('First Name|')||('Last Name|')||
		  ('Email Address|')||('Phone Number|')||('Error Message|'),'O');
		  OPEN lcu_phone_errors;
		  LOOP
		      FETCH lcu_phone_errors INTO lcu_phone_data;
			  EXIT WHEN lcu_phone_errors%NOTFOUND;

			  FOR i IN (SELECT regexp_substr(lcu_phone_data.error_message,'[^;]+',1,level) err_msg
			              FROM dual 
					CONNECT BY level <= greatest(coalesce(regexp_count(lcu_phone_data.error_message,';') + 1,0)))
			  LOOP
			          xxaqv_conv_cmn_utility_pkg.print_logs( (lcu_phone_data.company_name|| '|')
                                           				  || (lcu_phone_data.company_site_name|| '|')
														  || (lcu_phone_data.first_name|| '|')
														  || (lcu_phone_data.last_name|| '|')
														  || (lcu_phone_data.email_address|| '|')
														  || (lcu_phone_data.phone_number|| '|')
				                                          ||  i.err_msg
                                                          , 'O');			    
			  END LOOP;

		   END LOOP;
		   CLOSE lcu_phone_errors;		      


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
-- * Procedure  : validate_company_id                                                                           *
-- * Purpose    : Procedure to validate company id                                                           *
-- *****************************************************************************************************************/

   PROCEDURE validate_company_id ( p_company_name   IN    VARCHAR2
                                 , p_company_number IN    VARCHAR2
                                 , x_company_id     OUT   NUMBER
                                 , x_retcode        OUT   NUMBER
                                 , x_err_msg        OUT   VARCHAR2
   )    IS
   BEGIN
      x_retcode   := 0;
      x_err_msg   := NULL;
      
	  SELECT company_id
        INTO x_company_id
        FROM pn_companies_all
       WHERE trim(upper(name)) = trim(upper(p_company_name))
	     AND company_number    = p_company_number;

    EXCEPTION
      WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'Company_name: No Data Found for Company NAME ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'Company_name: Multiple records with same Company NAME ' ;
      WHEN OTHERS THEN
         x_retcode   := 1;
         x_err_msg   := 'Company_name: Unexpected error: ' || sqlerrm;
   END validate_company_id;

--/*****************************************************************************************************************
-- * Procedure  : validate_company_site_id                                                                         *
-- * Purpose    : Procedure to validate company site id                                                            *
-- *****************************************************************************************************************/

   PROCEDURE validate_company_site_id( p_company_name          IN    VARCHAR2
                                     , p_company_site_name     IN    VARCHAR2
									 , p_company_number        IN    VARCHAR2									 									    
									 , p_address_line1         IN    VARCHAR2
									 , p_address_line2         IN    VARCHAR2
									 , p_address_line3         IN    VARCHAR2
									 , p_address_line4         IN    VARCHAR2
									 , p_county                IN    VARCHAR2
									 , p_city                  IN    VARCHAR2
									 , p_state                 IN    VARCHAR2
									 , p_province              IN    VARCHAR2
									 , p_zip_code              IN    VARCHAR2
									 , p_country               IN    VARCHAR2
                                     , p_lease_role_type       IN    VARCHAR2
                                     , x_company_site_id       OUT   NUMBER
                                     , x_retcode               OUT   NUMBER
                                     , x_err_msg               OUT   VARCHAR2
   )    IS
   BEGIN
      x_retcode   := 0;
      x_err_msg   := NULL;
    
       SELECT pcs.company_site_id
         INTO x_company_site_id
         FROM pn_companies           pc
            , pn_company_sites_all   pcs
            , pn_addresses_all       paa
        WHERE pc.company_id                         = pcs.company_id
		  AND pcs.address_id                        = paa.address_id
          AND TRIM(upper(pc.name))                  = TRIM(upper(p_company_name))
          AND TRIM(upper(pcs.name))                 = TRIM(upper(p_company_site_name))
          AND nvl(paa.address_line1,'XX')           = nvl(p_address_line1,'XX')
		  AND nvl(paa.address_line2,'XX')           = nvl(p_address_line2,'XX')
		  AND nvl(paa.address_line3,'XX')           = nvl(p_address_line3,'XX')
		  AND nvl(paa.address_line4,'XX')           = nvl(p_address_line4,'XX')
		  AND nvl(paa.county,'XX')                  = nvl(p_county,'XX')
		  AND nvl(paa.city,'XX')                    = nvl(p_city,'XX')
		  AND nvl(paa.state,'XX')                   = nvl(p_state,'XX')
		  AND nvl(paa.province,'XX')                = nvl(p_province,'XX')
		  AND nvl(paa.zip_code,'XX')                = nvl(p_zip_code,'XX')
		  AND nvl(paa.country,'XX')                 = nvl(p_country,'XX')
		  AND pc.company_number                     = p_company_number
		  AND pcs.lease_role_type                   = p_lease_role_type;

    EXCEPTION
      WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'Site_name: No Data Found for COMPANY SITE NAME ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'Site_name: Multiple records with same COMPANY SITE NAME ' ;
      WHEN OTHERS THEN
         x_retcode   := 1;
         x_err_msg   := 'Site_name: Unexpected error: ' || sqlerrm;
   END validate_company_site_id;

--/*****************************************************************************************************************
-- * Procedure  : validate_contact_id                                                                           *
-- * Purpose    : Procedure to validate contact id                                                           *
-- *****************************************************************************************************************/

   PROCEDURE validate_contact_id ( p_company_number      IN    VARCHAR2
                                 , p_company_site_name   IN    VARCHAR2
								 , p_first_name          IN    VARCHAR2
                                 , p_last_name           IN    VARCHAR2
								 , p_primary_flag        IN    VARCHAR2
								 , p_email_address       IN    VARCHAR2
								 , p_address_line1       IN    VARCHAR2
								 , p_address_line2       IN    VARCHAR2
								 , p_address_line3       IN    VARCHAR2
								 , p_address_line4       IN    VARCHAR2
								 , p_county              IN    VARCHAR2
								 , p_city                IN    VARCHAR2
								 , p_state               IN    VARCHAR2
								 , p_province            IN    VARCHAR2
								 , p_zip_code            IN    VARCHAR2
								 , p_country             IN    VARCHAR2
								 , p_job_title           IN    VARCHAR2
                                 , x_contact_id          OUT   NUMBER
                                 , x_retcode             OUT   NUMBER
                                 , x_err_msg             OUT   VARCHAR2
   )    IS
   BEGIN
      x_retcode   := 0;
      x_err_msg   := NULL;
     
	 SELECT pca.contact_id
  INTO x_contact_id
  FROM pn_companies            pc
       , pn_company_sites_all  pcs
       , pn_contacts           pca
	   , pn_addresses_all      paa
 WHERE pc.company_id                   = pcs.company_id
   AND pcs.company_site_id             = pca.company_site_id
   AND pcs.address_id                  = paa.address_id
   AND pc.company_number               = p_company_number
   AND TRIM(upper(pcs.name))           = TRIM(upper(p_company_site_name))
   AND nvl(pca.first_name,'XX')        = nvl(p_first_name,'XX')
   AND nvl(pca.last_name,'XX')         = nvl(p_last_name,'XX')
   AND nvl(pca.job_title,'XX')         = nvl(p_job_title,'XX')
   AND nvl(paa.address_line1,'XX')     = nvl(p_address_line1,'XX')
   AND nvl(paa.address_line2,'XX')     = nvl(p_address_line2,'XX') 
   AND nvl(paa.address_line3,'XX')     = nvl(p_address_line3,'XX') 
   AND nvl(paa.address_line4,'XX')     = nvl(p_address_line4,'XX') 
   AND nvl(paa.county,'XX')            = nvl(p_county,'XX')        
   AND nvl(paa.city,'XX')              = nvl(p_city,'XX')          
   AND nvl(paa.state,'XX')             = nvl(p_state,'XX')         
   AND nvl(paa.province,'XX')          = nvl(p_province,'XX')      
   AND nvl(paa.zip_code,'XX')          = nvl(p_zip_code,'XX')      
   AND nvl(paa.country,'XX')           = nvl(p_country,'XX')       
   AND nvl(pca.email_address,'XX')     = nvl(p_email_address,'XX')
   AND pca.primary_flag                = p_primary_flag;

    EXCEPTION
      WHEN no_data_found THEN
         x_retcode   := 1;
         x_err_msg   := 'Contact: No Data Found for Contact ' ;
      WHEN too_many_rows THEN
         x_retcode   := 1;
         x_err_msg   := 'Contact: Multiple records with same Contact ' ;
      WHEN OTHERS THEN
         x_retcode   := 1;
         x_err_msg   := 'Contact: Unexpected error: ' || sqlerrm;
   END validate_contact_id;


--/****************************************************************************************************************
-- * Procedure  : extract_comp_data                                                                               *
-- * Purpose    : This Procedure is used to load company data into staging Table                                 *
-- ****************************************************************************************************************/

   PROCEDURE extract_comp_data ( x_retcode        OUT   NUMBER
                               , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_comp
      IS SELECT 
       regexp_replace(
			   comp.name,'[^[!-~]]*'
                 ,' '
              )                  name
	   , comp.company_number       company_number
       , comp.enabled_flag         enabled_flag
       , comp.parent_company_id    parent_company_id
       , comp.attribute_category   attribute_category
       , comp.attribute1           attribute1
       , comp.attribute2           attribute2
       , comp.attribute3           attribute3
       , comp.attribute4           attribute4
       , comp.attribute5           attribute5
       , comp.attribute6           attribute6
       , comp.attribute7           attribute7
       , comp.attribute8           attribute8
       , comp.attribute9           attribute9
       , comp.attribute10          attribute10
       , comp.attribute11          attribute11
       , comp.attribute12          attribute12
       , comp.attribute13          attribute13
       , comp.attribute14          attribute14
       , comp.attribute15          attribute15
       , pcomp.name                parent_company_name
       , hra.name                 ou_name
  FROM pn_companies_all@xxaqv_conv_cmn_dblink     pcomp
       , pn_companies@xxaqv_conv_cmn_dblink       comp
       , hr_operating_units@xxaqv_conv_cmn_dblink hra
 WHERE comp.parent_company_id = pcomp.company_id (+)
   AND comp.org_id= hra.organization_id
 --  AND comp.enabled_flag='Y'
   AND comp.company_id between nvl(gv_legacy_id_from,comp.company_id)and nvl(gv_legacy_id_to,comp.company_id);			 

   -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN
     
	 FND_GLOBAL.SET_NLS_CONTEXT('AMERICAN');
	 MO_GLOBAL.SET_POLICY_CONTEXT@xxaqv_conv_cmn_dblink('S','103');
	 
      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: Company_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: Company_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_comp_tab.delete;

    FOR i IN lcu_comp
    LOOP

	gt_xxaqv_pn_comp_tab(ln_line_count).company_number         := i.company_number;
	gt_xxaqv_pn_comp_tab(ln_line_count).name                   := i.name;                      
	gt_xxaqv_pn_comp_tab(ln_line_count).enabled_flag           := i.enabled_flag;              
	gt_xxaqv_pn_comp_tab(ln_line_count).parent_company_id      := i.parent_company_id;         
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute_category     := i.attribute_category;        
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute1             := i.attribute1;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute2             := i.attribute2;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute3             := i.attribute3;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute4             := i.attribute4;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute5             := i.attribute5;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute6             := i.attribute6;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute7             := i.attribute7;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute8             := i.attribute8;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute9             := i.attribute9;                
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute10            := i.attribute10;               
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute11            := i.attribute11;               
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute12            := i.attribute12;               
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute13            := i.attribute13;               
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute14            := i.attribute14;               
	gt_xxaqv_pn_comp_tab(ln_line_count).attribute15            := i.attribute15;               
	gt_xxaqv_pn_comp_tab(ln_line_count).parent_company_name    := i.parent_company_name;                         
	gt_xxaqv_pn_comp_tab(ln_line_count).ou_name                := i.ou_name;               
    gt_xxaqv_pn_comp_tab(ln_line_count).process_flag           := gv_load_success;
	gt_xxaqv_pn_comp_tab(ln_line_count).last_update_date       := SYSDATE;    
	gt_xxaqv_pn_comp_tab(ln_line_count).last_updated_by        := gn_user_id;    
	gt_xxaqv_pn_comp_tab(ln_line_count).creation_date          := SYSDATE;    
	gt_xxaqv_pn_comp_tab(ln_line_count).created_by             := gn_user_id;    
	gt_xxaqv_pn_comp_tab(ln_line_count).last_update_login 	   := gn_login_id;
	ln_line_count                                              := ln_line_count + 1;
END LOOP;
     BEGIN
          FORALL i IN gt_xxaqv_pn_comp_tab.first..gt_xxaqv_pn_comp_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_COMP_C011_STG VALUES gt_xxaqv_pn_comp_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_COMP_C011_STG:Company Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Company : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Company dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Company Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Company Unexpected Error:' 
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
   END extract_comp_data;	
	
--/****************************************************************************************************************
-- * Procedure  : extract_comp_sites_data                                                                         *
-- * Purpose    : This Procedure is used to load company sites data into staging Table                            *
-- ****************************************************************************************************************/

   PROCEDURE extract_comp_sites_data ( x_retcode        OUT   NUMBER
                               , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_comp_sites
      IS SELECT  csite.name             name 
       , csite.enabled_flag             enabled_flag
       , csite.company_site_code        company_site_code
       , csite.lease_role_type          lease_role_type
       , csite.attribute_category       attribute_category
       , csite.attribute1               attribute1
       , csite.attribute2               attribute2
       , csite.attribute3               attribute3
       , csite.attribute4               attribute4
       , csite.attribute5               attribute5
       , csite.attribute6               attribute6
       , csite.attribute7               attribute7
       , csite.attribute8               attribute8
       , csite.attribute9               attribute9
       , csite.attribute10              attribute10
       , csite.attribute11              attribute11
       , csite.attribute12              attribute12
       , csite.attribute13              attribute13
       , csite.attribute14              attribute14
       , csite.attribute15              attribute15
       , addr.address_line1             address_line1
       , addr.address_line2             address_line2
       , addr.address_line3             address_line3
       , addr.address_line4             address_line4
       , addr.county                    county
       , addr.city                      city
       , addr.state                     state
       , addr.province                  province
       , addr.zip_code                  zip_code
       , addr.country                   country
       , addr.addr_attribute_category   addr_attribute_category
       , addr.addr_attribute1           addr_attribute1
       , addr.addr_attribute2           addr_attribute2
       , addr.addr_attribute3           addr_attribute3
       , addr.addr_attribute4           addr_attribute4
       , addr.addr_attribute5           addr_attribute5
       , addr.addr_attribute6           addr_attribute6
       , addr.addr_attribute7           addr_attribute7
       , addr.addr_attribute8           addr_attribute8
       , addr.addr_attribute9           addr_attribute9
       , addr.addr_attribute10          addr_attribute10
       , addr.addr_attribute11          addr_attribute11
       , addr.addr_attribute12          addr_attribute12
       , addr.addr_attribute13          addr_attribute13
       , addr.addr_attribute14          addr_attribute14
       , addr.addr_attribute15          addr_attribute15
       , terr.address_style             address_style
       , terr.territory_short_name      territory_short_name
       , rlook.meaning                  lease_role
       , pnp_util_func.get_concatenated_address(
   addr.address_style
   , addr.address_line1
   , addr.address_line2
   , addr.address_line3
   , addr.address_line4
   , addr.city
   , addr.county
   , addr.state
   , addr.province
   , addr.zip_code
   , terr.territory_short_name
) concatenated_address
       
       , hra.name                      ou_name
       , regexp_replace(
			   pcomp.name,'[^[!-~]]*'
                 ,' '
              )                                     company_name
	   , pcomp.company_number          company_number
  FROM fnd_territories_vl@xxaqv_conv_cmn_dblink     terr
       , pn_addresses_all@xxaqv_conv_cmn_dblink     addr
       , fnd_lookups@xxaqv_conv_cmn_dblink          rlook
       , pn_company_sites@xxaqv_conv_cmn_dblink     csite
       , hr_operating_units@xxaqv_conv_cmn_dblink   hra
       , pn_companies@xxaqv_conv_cmn_dblink         pcomp
 WHERE csite.address_id = addr.address_id (+)
   AND addr.country       = terr.territory_code (+)
   AND rlook.lookup_type  = 'PN_LEASE_ROLE_TYPE'
   AND rlook.lookup_code  = csite.lease_role_type
   AND csite.org_id = hra.organization_id
   AND csite.company_id = pcomp.company_id
  -- AND csite.enabled_flag= 'Y'
   AND csite.company_site_id between nvl(gv_legacy_id_from,csite.company_site_id)and nvl(gv_legacy_id_to,csite.company_site_id);			 

   -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN

     FND_GLOBAL.SET_NLS_CONTEXT('AMERICAN');
	 MO_GLOBAL.SET_POLICY_CONTEXT@xxaqv_conv_cmn_dblink('S','103');
      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: Company_site_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: Company_site_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_comp_sites_tab.delete;

    FOR i IN lcu_comp_sites
    LOOP

	
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).name                     := i.name;                                     
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).enabled_flag             := i.enabled_flag;                    
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).company_site_code        := i.company_site_code;               
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).lease_role_type          := i.lease_role_type;                 
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute_category       := i.attribute_category;              
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute1               := i.attribute1;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute2               := i.attribute2;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute3               := i.attribute3;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute4               := i.attribute4;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute5               := i.attribute5;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute6               := i.attribute6;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute7               := i.attribute7;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute8               := i.attribute8;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute9               := i.attribute9;                      
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute10              := i.attribute10;                     
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute11              := i.attribute11;                     
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute12              := i.attribute12;                     
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute13              := i.attribute13;                     
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute14              := i.attribute14;                                       
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).attribute15              := i.attribute15;               
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).address_line1            := i.address_line1;            
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).address_line2            := i.address_line2;                
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).address_line3            := i.address_line3;                
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).address_line4            := i.address_line4;                
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).county                   := i.county;                       
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).city                     := i.city;                         
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).state                    := i.state;                        
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).province                 := i.province;                     
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).zip_code                 := i.zip_code;                     
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).country                  := i.country;                      
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute_category  := i.addr_attribute_category;      
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute1          := i.addr_attribute1 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute2          := i.addr_attribute2 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute3          := i.addr_attribute3 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute4          := i.addr_attribute4 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute5          := i.addr_attribute5 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute6          := i.addr_attribute6 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute7          := i.addr_attribute7 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute8          := i.addr_attribute8 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute9          := i.addr_attribute9 ;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute10         := i.addr_attribute10;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute11         := i.addr_attribute11;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute12         := i.addr_attribute12;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute13         := i.addr_attribute13;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute14         := i.addr_attribute14;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).addr_attribute15         := i.addr_attribute15;             
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).address_style            := i.address_style ;               
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).territory_short_name     := i.territory_short_name;         
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).lease_role               := i.lease_role;                   
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).concatenated_address     := i.concatenated_address;                                
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).ou_name                  := i.ou_name;                      
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).company_name             := i.company_name;
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).company_number           := i.company_number;	
    gt_xxaqv_pn_comp_sites_tab(ln_line_count).process_flag             := gv_load_success;
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).last_update_date         := SYSDATE;    
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).last_updated_by          := gn_user_id;    
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).creation_date            := SYSDATE;    
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).created_by               := gn_user_id;    
	gt_xxaqv_pn_comp_sites_tab(ln_line_count).last_update_login 	   := gn_login_id;
	ln_line_count                                                      := ln_line_count + 1;
END LOOP;
     BEGIN
          FORALL i IN gt_xxaqv_pn_comp_sites_tab.first..gt_xxaqv_pn_comp_sites_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_COMP_SITES_C011_STG VALUES gt_xxaqv_pn_comp_sites_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_COMP_SITES_C011_STG:Company Sites Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Company Sites : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Company Sites dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Company Sites Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Company Sites Unexpected Error:' 
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
   END extract_comp_sites_data;	

--/****************************************************************************************************************
-- * Procedure  : extract_cont_data                                                                               *
-- * Purpose    : This Procedure is used to load contact data into staging Table                                  *
-- ****************************************************************************************************************/

   PROCEDURE extract_cont_data ( x_retcode        OUT   NUMBER
                               , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_cont
      IS 
	  SELECT  co.status          status
       , co.last_name            last_name
       , co.first_name           first_name
       , co.job_title            job_title
       , co.mail_stop            mail_stop
       , co.email_address        email_address
       , co.primary_flag         primary_flag
       , co.attribute_category   attribute_category
       , co.attribute1           attribute1
       , co.attribute2           attribute2
       , co.attribute3           attribute3
       , co.attribute4           attribute4
       , co.attribute5           attribute5
       , co.attribute6           attribute6
       , co.attribute7           attribute7
       , co.attribute8           attribute8
       , co.attribute9           attribute9
       , co.attribute10          attribute10
       , co.attribute11          attribute11
       , co.attribute12          attribute12
       , co.attribute13          attribute13
       , co.attribute14          attribute14
       , co.attribute15          attribute15
       , jlook.meaning           job_title_meaning
       ,hra.name                 ou_name
       , pcs.name                company_site_name
       , regexp_replace(
		pcomp.name,'[^[!-~]]*'
        ,' '
          )                       company_name
	   , pcomp.company_number     company_number
	   --, pcs.concatenated_address concatenated_address
	   , pcs.lease_role_type      lease_role_type
	   , paa.address_line1        address_line1
	   , paa.address_line2        address_line2
	   , paa.address_line3        address_line3
	   , paa.address_line4        address_line4
	   , paa.county               county       
	   , paa.city                 city         
	   , paa.state                state        
	   , paa.province             province     
	   , paa.zip_code             zip_code     
	   , paa.country              country      
  FROM fnd_lookups@xxaqv_conv_cmn_dblink          jlook
     , pn_contacts@xxaqv_conv_cmn_dblink          co
     , hr_operating_units@xxaqv_conv_cmn_dblink   hra
     , pn_company_sites_all@xxaqv_conv_cmn_dblink pcs
     , pn_companies@xxaqv_conv_cmn_dblink         pcomp
	 , pn_addresses_all@xxaqv_conv_cmn_dblink     paa
 WHERE jlook.lookup_type (+) = 'PN_JOB_TITLE'
   AND jlook.lookup_code (+) = co.job_title
   AND co.org_id             = hra.organization_id
   AND co.company_site_id    = pcs.company_site_id
   AND pcs.address_id        = paa.address_id
   AND pcs.company_id        = pcomp.company_id
   AND co.status             = 'A'
   AND co.contact_id between nvl(gv_legacy_id_from,co.contact_id)and nvl(gv_legacy_id_to,co.contact_id);			 

   -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN

      FND_GLOBAL.SET_NLS_CONTEXT('AMERICAN');
	 MO_GLOBAL.SET_POLICY_CONTEXT@xxaqv_conv_cmn_dblink('S','103');
      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: contact_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: contact_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_cont_tab.delete;

    FOR i IN lcu_cont
    LOOP

	
	gt_xxaqv_pn_cont_tab(ln_line_count).status                            := i.status;                           
	gt_xxaqv_pn_cont_tab(ln_line_count).last_name                         := i.last_name;               
	gt_xxaqv_pn_cont_tab(ln_line_count).first_name                        := i.first_name;              
	gt_xxaqv_pn_cont_tab(ln_line_count).job_title                         := i.job_title;               
	gt_xxaqv_pn_cont_tab(ln_line_count).mail_stop                         := i.mail_stop;               
	--gt_xxaqv_pn_cont_tab(ln_line_count).email_address                     := i.email_address;
IF gv_db_name <> 'PRERP01' and i.email_address IS NOT NULL THEN
                  gt_xxaqv_pn_cont_tab(ln_line_count).email_address := 'XX' || i.email_address;
               ELSE
                  gt_xxaqv_pn_cont_tab(ln_line_count).email_address := i.email_address;
               END IF;	
	gt_xxaqv_pn_cont_tab(ln_line_count).primary_flag                      := i.primary_flag;            
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute_category                := i.attribute_category;      
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute1                        := i.attribute1;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute2                        := i.attribute2;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute3                        := i.attribute3;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute4                        := i.attribute4;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute5                        := i.attribute5;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute6                        := i.attribute6;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute7                        := i.attribute7;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute8                        := i.attribute8;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute9                        := i.attribute9;              
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute10                       := i.attribute10;             
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute11                       := i.attribute11;             
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute12                       := i.attribute12;             
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute13                       := i.attribute13;                               
	gt_xxaqv_pn_cont_tab(ln_line_count).attribute14                       := i.attribute14;       
    gt_xxaqv_pn_cont_tab(ln_line_count).attribute15                       := i.attribute15;       
    gt_xxaqv_pn_cont_tab(ln_line_count).job_title_meaning                 := i.job_title_meaning; 
    gt_xxaqv_pn_cont_tab(ln_line_count).ou_name                           := i.ou_name;           
    gt_xxaqv_pn_cont_tab(ln_line_count).company_site_name                 := i.company_site_name; 
    gt_xxaqv_pn_cont_tab(ln_line_count).company_name                      := i.company_name;
    gt_xxaqv_pn_cont_tab(ln_line_count).company_number                    := i.company_number;
    --gt_xxaqv_pn_cont_tab(ln_line_count).concatenated_address              := i.concatenated_address;
	gt_xxaqv_pn_cont_tab(ln_line_count).address_line1                     := i.address_line1;
	gt_xxaqv_pn_cont_tab(ln_line_count).address_line2                     := i.address_line2;
	gt_xxaqv_pn_cont_tab(ln_line_count).address_line3                     := i.address_line3;
	gt_xxaqv_pn_cont_tab(ln_line_count).address_line4                     := i.address_line4;
	gt_xxaqv_pn_cont_tab(ln_line_count).county                            := i.county;
	gt_xxaqv_pn_cont_tab(ln_line_count).city                              := i.city;         
	gt_xxaqv_pn_cont_tab(ln_line_count).state                             := i.state;       
	gt_xxaqv_pn_cont_tab(ln_line_count).province                          := i.province;     
	gt_xxaqv_pn_cont_tab(ln_line_count).zip_code                          := i.zip_code;     
	gt_xxaqv_pn_cont_tab(ln_line_count).country                           := i.country;      
	gt_xxaqv_pn_cont_tab(ln_line_count).lease_role_type                   := i.lease_role_type;	
    gt_xxaqv_pn_cont_tab(ln_line_count).process_flag                      := gv_load_success;
	gt_xxaqv_pn_cont_tab(ln_line_count).last_update_date                  := SYSDATE;    
	gt_xxaqv_pn_cont_tab(ln_line_count).last_updated_by                   := gn_user_id;    
	gt_xxaqv_pn_cont_tab(ln_line_count).creation_date                     := SYSDATE;    
	gt_xxaqv_pn_cont_tab(ln_line_count).created_by                        := gn_user_id;    
	gt_xxaqv_pn_cont_tab(ln_line_count).last_update_login 	              := gn_login_id;
	ln_line_count                                                         := ln_line_count + 1;
END LOOP;
     BEGIN
          FORALL i IN gt_xxaqv_pn_cont_tab.first..gt_xxaqv_pn_cont_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_CONT_C011_STG VALUES gt_xxaqv_pn_cont_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_CONT_C011_STG:Contacts Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Contacts : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Contacts dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Contacts Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Contacts Unexpected Error:' 
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
   END extract_cont_data;

--/****************************************************************************************************************
-- * Procedure  : extract_phone_data                                                                               *
-- * Purpose    : This Procedure is used to load phone data into staging Table                                 *
-- ****************************************************************************************************************/

   PROCEDURE extract_phone_data ( x_retcode        OUT   NUMBER
                               , x_err_msg        OUT   VARCHAR2 )
    IS

      CURSOR lcu_phone
      IS 
	  SELECT  phone.status          status
       , phone.phone_type           phone_type
       , phone.area_code            area_code
       , phone.phone_number         phone_number
       , phone.extension            extension
       , phone.primary_flag         primary_flag
       , phone.attribute_category   attribute_category
       , phone.attribute1           attribute1
       , phone.attribute2           attribute2
       , phone.attribute3           attribute3
       , phone.attribute4           attribute4
       , phone.attribute5           attribute5
       , phone.attribute6           attribute6
       , phone.attribute7           attribute7
       , phone.attribute8           attribute8
       , phone.attribute9           attribute9
       , phone.attribute10          attribute10
       , phone.attribute11          attribute11
       , phone.attribute12          attribute12
       , phone.attribute13          attribute13
       , phone.attribute14          attribute14
       , phone.attribute15          attribute15
       , plook.meaning              phone_type_is
       , hra.name                   ou_name
       , pcs.name                   company_site_name
	   --, pcs.concatenated_address   concatenated_address
       , regexp_replace(
		 pcomp.name,'[^[!-~]]*'
          ,' '
              )                     company_name
	   , pcomp.company_number       company_number
       , pc.first_name              first_name
       , pc.last_name               last_name
       , pc.email_address           email_address  
       , pc.job_title               job_title	
       , pc.primary_flag            cprimary_flag
       , paa.address_line1          address_line1
       , paa.address_line2          address_line2
       , paa.address_line3          address_line3
       , paa.address_line4          address_line4
       , paa.county                 county       
       , paa.city                   city         
       , paa.state                  state        
       , paa.province               province     
       , paa.zip_code               zip_code     
       , paa.country                country      
  FROM fnd_lookups@xxaqv_conv_cmn_dblink            plook
     , pn_phones@xxaqv_conv_cmn_dblink              phone
     , pn_contacts@xxaqv_conv_cmn_dblink            pc
     , pn_company_sites_all@xxaqv_conv_cmn_dblink   pcs
     , pn_companies@xxaqv_conv_cmn_dblink           pcomp
     , hr_operating_units@xxaqv_conv_cmn_dblink     hra
	 , pn_addresses_all@xxaqv_conv_cmn_dblink       paa
 WHERE plook.lookup_type   = 'PN_PHONE_TYPE'
   AND plook.lookup_code   = phone.phone_type
   AND phone.contact_id    = pc.contact_id
   AND pc.company_site_id  = pcs.company_site_id
   AND pcs.address_id      = paa.address_id
   AND pcs.company_id      = pcomp.company_id
   AND phone.org_id        = hra.organization_id
   AND phone.status        = 'A'
   AND phone.phone_id between nvl(gv_legacy_id_from,phone.phone_id)and nvl(gv_legacy_id_to,phone.phone_id);			 

   -- LOCAL VARIABLES
      ln_line_count    BINARY_INTEGER := 1;
      ln_error_count   BINARY_INTEGER := 0;
      ex_dml_errors    EXCEPTION;
      ln_cmt_count     NUMBER   :=0;
      PRAGMA exception_init ( ex_dml_errors, -24381 );

  --INSERTING INTO STAGING TABLE
   BEGIN


      FND_GLOBAL.SET_NLS_CONTEXT('AMERICAN');
	  MO_GLOBAL.SET_POLICY_CONTEXT@xxaqv_conv_cmn_dblink('S','103');
      print_debug('EXTRACT_DATA: START Load data into staging table and mark them LS');
      print_debug('EXTRACT_DATA: phone_id from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: phone_id to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;   
      --

    gt_xxaqv_pn_phone_tab.delete;

    FOR i IN lcu_phone
    LOOP

	
	gt_xxaqv_pn_phone_tab(ln_line_count).status                            := i.status;               
	gt_xxaqv_pn_phone_tab(ln_line_count).phone_type                        := i.phone_type;        
	gt_xxaqv_pn_phone_tab(ln_line_count).area_code                         := i.area_code;         
	gt_xxaqv_pn_phone_tab(ln_line_count).phone_number                      := i.phone_number;      
	gt_xxaqv_pn_phone_tab(ln_line_count).extension                         := i.extension;         
	gt_xxaqv_pn_phone_tab(ln_line_count).primary_flag                      := i.primary_flag;      
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute_category                := i.attribute_category;      
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute1                        := i.attribute1;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute2                        := i.attribute2;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute3                        := i.attribute3;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute4                        := i.attribute4;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute5                        := i.attribute5;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute6                        := i.attribute6;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute7                        := i.attribute7;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute8                        := i.attribute8;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute9                        := i.attribute9;        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute10                       := i.attribute10;       
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute11                       := i.attribute11;       
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute12                       := i.attribute12;       
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute13                       := i.attribute13;       
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute14                       := i.attribute14;                        
	gt_xxaqv_pn_phone_tab(ln_line_count).attribute15                       := i.attribute15;       
    gt_xxaqv_pn_phone_tab(ln_line_count).phone_type_is                     := i.phone_type_is;     
    gt_xxaqv_pn_phone_tab(ln_line_count).ou_name                           := i.ou_name;           
    gt_xxaqv_pn_phone_tab(ln_line_count).company_site_name                 := i.company_site_name; 
    gt_xxaqv_pn_phone_tab(ln_line_count).company_name                      := i.company_name;
    gt_xxaqv_pn_phone_tab(ln_line_count).company_number                    := i.company_number;	
    gt_xxaqv_pn_phone_tab(ln_line_count).first_name                        := i.first_name;        
    gt_xxaqv_pn_phone_tab(ln_line_count).last_name                         := i.last_name;         
	gt_xxaqv_pn_phone_tab(ln_line_count).job_title                         := i.job_title;
	gt_xxaqv_pn_phone_tab(ln_line_count).cprimary_flag                     := i.cprimary_flag;
	gt_xxaqv_pn_phone_tab(ln_line_count).address_line1                     := i.address_line1;
	gt_xxaqv_pn_phone_tab(ln_line_count).address_line2                     := i.address_line2;
	gt_xxaqv_pn_phone_tab(ln_line_count).address_line3                     := i.address_line3;
	gt_xxaqv_pn_phone_tab(ln_line_count).address_line4                     := i.address_line4;
	gt_xxaqv_pn_phone_tab(ln_line_count).county                            := i.county;       
	gt_xxaqv_pn_phone_tab(ln_line_count).city                              := i.city;         
	gt_xxaqv_pn_phone_tab(ln_line_count).state                             := i.state;        
	gt_xxaqv_pn_phone_tab(ln_line_count).province                          := i.province;     
	gt_xxaqv_pn_phone_tab(ln_line_count).zip_code                          := i.zip_code;     
	gt_xxaqv_pn_phone_tab(ln_line_count).country                           := i.country;      
	
    --gt_xxaqv_pn_phone_tab(ln_line_count).email_address                     := i.email_address;
	
    IF gv_db_name <> 'PRERP01' and i.email_address IS NOT NULL THEN
                  gt_xxaqv_pn_phone_tab(ln_line_count).email_address := 'XX' || i.email_address;
               ELSE
                  gt_xxaqv_pn_phone_tab(ln_line_count).email_address := i.email_address;
               END IF	;
    gt_xxaqv_pn_phone_tab(ln_line_count).process_flag                      := gv_load_success;
	gt_xxaqv_pn_phone_tab(ln_line_count).last_update_date                  := SYSDATE;    
	gt_xxaqv_pn_phone_tab(ln_line_count).last_updated_by                   := gn_user_id;    
	gt_xxaqv_pn_phone_tab(ln_line_count).creation_date                     := SYSDATE;    
	gt_xxaqv_pn_phone_tab(ln_line_count).created_by                        := gn_user_id;    
	gt_xxaqv_pn_phone_tab(ln_line_count).last_update_login 	               := gn_login_id;
	ln_line_count                                                          := ln_line_count + 1;
END LOOP;
     BEGIN
          FORALL i IN gt_xxaqv_pn_phone_tab.first..gt_xxaqv_pn_phone_tab.last SAVE EXCEPTIONS
            INSERT INTO XXAQV_PN_PHONE_C011_STG VALUES gt_xxaqv_pn_phone_tab ( i );

             print_debug(gv_module
                        ||':'
                        ||'EXTRACT_DATA: XXAQV_PN_PHONE_C011_STG:Phone Records loaded sucessfully: ' 
                        || SQL%rowcount);         
             COMMIT;

         EXCEPTION
            WHEN ex_dml_errors 
            THEN
               x_retcode        := 1;
               ln_error_count   := SQL%bulk_exceptions.count;
              print_debug(gv_module
                         ||':'
                         ||'EXTRACT_DATA:  Number of failures for Phone : ' 
                         || ln_error_count,gv_log_type); 
               FOR i IN 1..ln_error_count 
               LOOP 
               print_debug(gv_module
                           ||':'
                           ||'EXTRACT_DATA: Phone dml Error: ' 
                           || i 
                           || 'Array Index: '
                           || SQL%bulk_exceptions(i).error_index 
                           || 'Message: ' 
                           || sqlerrm(-SQL%bulk_exceptions(i).error_code),gv_log_type);
               END LOOP;

            WHEN OTHERS 
            THEN
               print_debug(gv_module||':'
                          ||'EXTRACT_DATA:Phone Unexpected Error: ' 
                          || sqlerrm, gv_log_type );
               x_retcode   := 1;
               x_err_msg   := gv_module
                              ||':'||'EXTRACT_DATA:Phone Unexpected Error:' 
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
   END extract_phone_data;


--/****************************************************************************************************************
-- * Procedure : validate_comp_records                                                                             *
-- * Purpose   : This Procedure validate the Company records in the staging table.                               *
-- ****************************************************************************************************************/	

   PROCEDURE validate_comp_records ( x_retcode        OUT   NUMBER
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


     CURSOR lcu_comp 
	 IS
	 SELECT comp.*,rowid
      FROM XXAQV_PN_COMP_C011_STG comp
	  WHERE comp.process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

 TYPE lcu_comp_type IS
         TABLE OF lcu_comp%rowtype INDEX BY BINARY_INTEGER;
      lcu_comp_tab       lcu_comp_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: company_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: company_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --

      OPEN lcu_comp;
      LOOP
         lcu_comp_tab.DELETE;
         FETCH lcu_comp BULK COLLECT INTO lcu_comp_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_comp_tab.count = 0;
         --
         FOR i IN lcu_comp_tab.first..lcu_comp_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;
          --validate org_id
         ln_org_id  := xxaqv_conv_cmn_utility_pkg.validate_oper_unit(lcu_comp_tab(i).ou_name);
         IF ln_org_id = 0 THEN
            lcu_comp_tab(i).process_flag         := gv_validate_error;
            gv_process_flag                      := 'E';        
            gv_err_msg        := gv_err_msg|| ';' || 'Invalid Operating Unit';           
         ELSE
            lcu_comp_tab(i).x_org_id := ln_org_id;
         END IF;		   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_comp_tab(i).error_message := gv_err_msg;
			    	lcu_comp_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_comp_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_comp_tab.first..lcu_comp_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_COMP_C011_STG
               SET x_org_id              = lcu_comp_tab(i).x_org_id
                 , process_flag          = lcu_comp_tab(i).process_flag
				 , error_message         = lcu_comp_tab(i).error_message
            WHERE
                1 = 1
                AND ROWID = lcu_comp_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_COMP_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
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

      CLOSE lcu_comp;
      print_debug('VALIDATE_RECORDS: END Validate Company table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_comp_records;		 


--/****************************************************************************************************************
-- * Procedure : validate_comp_sites_records                                                                             *
-- * Purpose   : This Procedure validate the Company sites records in the staging table.                               *
-- ****************************************************************************************************************/	

   PROCEDURE validate_comp_sites_records ( x_retcode        OUT   NUMBER
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


     CURSOR lcu_comp_sites 
	 IS
	 SELECT comp.*,rowid
      FROM XXAQV_PN_COMP_SITES_C011_STG comp
	  WHERE comp.process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

 TYPE lcu_comp_sites_type IS
         TABLE OF lcu_comp_sites%rowtype INDEX BY BINARY_INTEGER;
      lcu_comp_sites_tab       lcu_comp_sites_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: company_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: company_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --

      OPEN lcu_comp_sites;
      LOOP
         lcu_comp_sites_tab.DELETE;
         FETCH lcu_comp_sites BULK COLLECT INTO lcu_comp_sites_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_comp_sites_tab.count = 0;
         --
         FOR i IN lcu_comp_sites_tab.first..lcu_comp_sites_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;
 
            --validate company_id
           
               validate_company_id( p_company_name       => lcu_comp_sites_tab(i).company_name
			                      , p_company_number     => lcu_comp_sites_tab(i).company_number
                                  , x_company_id         => lcu_comp_sites_tab(i).x_company_id
                                  , x_retcode            => gn_retcode
                                  , x_err_msg            => lv_error_message
               );                    

               IF gn_retcode <> 0 THEN
                  lcu_comp_sites_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;			
          


          --validate org_id
         ln_org_id  := xxaqv_conv_cmn_utility_pkg.validate_oper_unit(lcu_comp_sites_tab(i).ou_name);
         IF ln_org_id = 0 THEN
            lcu_comp_sites_tab(i).process_flag         := gv_validate_error;
            gv_process_flag                      := 'E';        
            gv_err_msg        := gv_err_msg|| ';' || 'Invalid Operating Unit';           
         ELSE
            lcu_comp_sites_tab(i).x_org_id := ln_org_id;
         END IF;		   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_comp_sites_tab(i).error_message := gv_err_msg;
			    	lcu_comp_sites_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_comp_sites_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_comp_sites_tab.first..lcu_comp_sites_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_COMP_SITES_C011_STG
               SET x_org_id              = lcu_comp_sites_tab(i).x_org_id
			     , x_company_id          = lcu_comp_sites_tab(i).x_company_id
                 , process_flag          = lcu_comp_sites_tab(i).process_flag
				 , error_message         = lcu_comp_sites_tab(i).error_message
            WHERE
                1 = 1
                AND ROWID = lcu_comp_sites_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_COMP_SITES_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
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

      CLOSE lcu_comp_sites;
      print_debug('VALIDATE_RECORDS: END Validate company_sites table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_comp_sites_records;		 



--/****************************************************************************************************************
-- * Procedure : validate_cont_records                                                                             *
-- * Purpose   : This Procedure validate the contact records in the staging table.                               *
-- ****************************************************************************************************************/	

   PROCEDURE validate_cont_records ( x_retcode        OUT   NUMBER
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


     CURSOR lcu_cont 
	 IS
	 SELECT comp.*,rowid
      FROM XXAQV_PN_CONT_C011_STG comp
	  WHERE comp.process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

 TYPE lcu_cont_type IS
         TABLE OF lcu_cont%rowtype INDEX BY BINARY_INTEGER;
      lcu_cont_tab       lcu_cont_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: contact_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: contact_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --
	 

      OPEN lcu_cont;
      LOOP
         lcu_cont_tab.DELETE;
         FETCH lcu_cont BULK COLLECT INTO lcu_cont_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_cont_tab.count = 0;
         --
         FOR i IN lcu_cont_tab.first..lcu_cont_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;
 
            --validate company_site_id
           
          validate_company_site_id( p_company_name         => lcu_cont_tab(i).company_name
                                  , p_company_site_name    => lcu_cont_tab(i).company_site_name
								  , p_company_number       => lcu_cont_tab(i).company_number								 
								  , p_address_line1        => lcu_cont_tab(i).address_line1 
								  , p_address_line2        => lcu_cont_tab(i).address_line2 
								  , p_address_line3        => lcu_cont_tab(i).address_line3 
								  , p_address_line4        => lcu_cont_tab(i).address_line4 
								  , p_county               => lcu_cont_tab(i).county        
								  , p_city                 => lcu_cont_tab(i).city          
								  , p_state                => lcu_cont_tab(i).state         
								  , p_province             => lcu_cont_tab(i).province      
								  , p_zip_code             => lcu_cont_tab(i).zip_code      
								  , p_country              => lcu_cont_tab(i).country       
                                  , p_lease_role_type      => lcu_cont_tab(i).lease_role_type
								  , x_company_site_id      => lcu_cont_tab(i).x_company_site_id
                                  , x_retcode              => gn_retcode
                                  , x_err_msg              => lv_error_message
               );                    

               IF gn_retcode <> 0 THEN
                  lcu_cont_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;			
          


          --validate org_id
         ln_org_id  := xxaqv_conv_cmn_utility_pkg.validate_oper_unit(lcu_cont_tab(i).ou_name);
         IF ln_org_id = 0 THEN
            lcu_cont_tab(i).process_flag         := gv_validate_error;
            gv_process_flag                      := 'E';        
            gv_err_msg        := gv_err_msg|| ';' || 'Invalid Operating Unit';           
         ELSE
            lcu_cont_tab(i).x_org_id := ln_org_id;
         END IF;		   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_cont_tab(i).error_message := gv_err_msg;
			    	lcu_cont_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_cont_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_cont_tab.first..lcu_cont_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_CONT_C011_STG
               SET x_org_id              = lcu_cont_tab(i).x_org_id
			     , x_company_site_id     = lcu_cont_tab(i).x_company_site_id
                 , process_flag          = lcu_cont_tab(i).process_flag
				 , error_message         = lcu_cont_tab(i).error_message
            WHERE
                1 = 1
                AND ROWID = lcu_cont_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_CONT_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
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

      CLOSE lcu_cont;
	  
	  BEGIN
	  
	    --Erroring out duplicate records--
	  
	  UPDATE XXAQV_PN_CONT_C011_STG
	     SET process_flag  = gv_validate_error
		 , error_message = 'Duplicate Record'
	   WHERE rowid = (SELECT MIN(rowid)
                      FROM XXAQV_PN_CONT_C011_STG
                      GROUP BY last_name, first_name, email_address,x_company_site_id having count(*)>1);
					  
					  COMMIT;
                 EXCEPTION
					   WHEN OTHERS THEN
               print_debug( 'VALIDATE_RECORDS: Update duplicate records: ' || sqlerrm ,gv_log_type);
               x_retcode   := 1;
               x_err_msg   := 'VALIDATE_RECORDS: Update duplicate records:' || to_char(sqlcode) || '-' || sqlerrm;
	  END;
      print_debug('VALIDATE_RECORDS: END Validate contact table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_cont_records;		 


--/****************************************************************************************************************
-- * Procedure : validate_phone_records                                                                             *
-- * Purpose   : This Procedure validate the phone records in the staging table.                               *
-- ****************************************************************************************************************/	

   PROCEDURE validate_phone_records ( x_retcode        OUT   NUMBER
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


     CURSOR lcu_phone 
	 IS
	 SELECT comp.*,rowid
      FROM XXAQV_PN_PHONE_C011_STG comp
	  WHERE comp.process_flag in  (gv_load_success,gv_validate_error,gv_import_error);

 TYPE lcu_phone_type IS
         TABLE OF lcu_phone%rowtype INDEX BY BINARY_INTEGER;
      lcu_phone_tab       lcu_phone_type;
      ln_line_count      BINARY_INTEGER := 0;
      ln_error_count     BINARY_INTEGER := 0;
      ex_dml_errors EXCEPTION;
      PRAGMA exception_init ( ex_dml_errors,-24381 );
   --
   BEGIN
      print_debug('VALIDATE_RECORDS: START Validate staging table records and mark them as VS/VE');
      print_debug('EXTRACT_DATA: phone_ID from: ' || gv_legacy_id_from);
      print_debug('EXTRACT_DATA: phone_ID to: ' || gv_legacy_id_to);
      --
      x_retcode   := 0;
      x_err_msg   := NULL;
      --

      OPEN lcu_phone;
      LOOP
         lcu_phone_tab.DELETE;
         FETCH lcu_phone BULK COLLECT INTO lcu_phone_tab LIMIT gn_commit_cnt;
         EXIT WHEN lcu_phone_tab.count = 0;
         --
         FOR i IN lcu_phone_tab.first..lcu_phone_tab.last LOOP
            gn_retcode        := 0;
            gv_err_msg        := NULL;
            gv_process_flag   := 'S';            
			lv_error_message  := NULL;
 
            --validate contact_id
           
               validate_contact_id( p_company_number       => lcu_phone_tab(i).company_number
                                  , p_company_site_name    => lcu_phone_tab(i).company_site_name
								  , p_first_name           => lcu_phone_tab(i).first_name
								  , p_last_name            => lcu_phone_tab(i).last_name
								  , p_primary_flag         => lcu_phone_tab(i).cprimary_flag
								  , p_email_address        => lcu_phone_tab(i).email_address
								  , p_address_line1        => lcu_phone_tab(i).address_line1
								  , p_address_line2        => lcu_phone_tab(i).address_line2
								  , p_address_line3        => lcu_phone_tab(i).address_line3
								  , p_address_line4        => lcu_phone_tab(i).address_line4
								  , p_county               => lcu_phone_tab(i).county       
								  , p_city                 => lcu_phone_tab(i).city         
								  , p_state                => lcu_phone_tab(i).state        
								  , p_province             => lcu_phone_tab(i).province     
								  , p_zip_code             => lcu_phone_tab(i).zip_code     
								  , p_country      		   => lcu_phone_tab(i).country      					  
								  , p_job_title            => lcu_phone_tab(i).job_title
                                  , x_contact_id           => lcu_phone_tab(i).x_contact_id
                                  , x_retcode              => gn_retcode
                                  , x_err_msg              => lv_error_message
               );                    

               IF gn_retcode <> 0 THEN
                  lcu_phone_tab(i).process_flag := gv_validate_error;
                  gv_process_flag              := 'E';                 
				  gv_err_msg                   := gv_err_msg || ';'|| lv_error_message;
               END IF;			
          


          --validate org_id
         ln_org_id  := xxaqv_conv_cmn_utility_pkg.validate_oper_unit(lcu_phone_tab(i).ou_name);
         IF ln_org_id = 0 THEN
            lcu_phone_tab(i).process_flag         := gv_validate_error;
            gv_process_flag                      := 'E';        
            gv_err_msg        := gv_err_msg|| ';' || 'Invalid Operating Unit';           
         ELSE
            lcu_phone_tab(i).x_org_id := ln_org_id;
         END IF;		   

                IF gv_process_flag = 'E' 
				THEN
                    lcu_phone_tab(i).error_message := gv_err_msg;
			    	lcu_phone_tab(i).process_flag  := gv_validate_error;
                ELSE  
                   lcu_phone_tab(i).process_flag := gv_validate_success;
                END IF;

			    print_debug('END: '||gv_err_msg||' '||gv_process_flag);			
         END LOOP; -- table type loop

         --UPDATING THE VALIDATED RECORDS

         BEGIN
            FORALL i IN lcu_phone_tab.first..lcu_phone_tab.last SAVE EXCEPTIONS
            UPDATE XXAQV_PN_PHONE_C011_STG
               SET x_org_id              = lcu_phone_tab(i).x_org_id
			     , x_contact_id          = lcu_phone_tab(i).x_contact_id
                 , process_flag          = lcu_phone_tab(i).process_flag
				 , error_message         = lcu_phone_tab(i).error_message
            WHERE
                1 = 1
                AND ROWID = lcu_phone_tab(i).rowid;

            print_debug('VALIDATE_RECORDS: XXAQV_PN_PHONE_C011_STG: Records loaded sucessfully: ' || SQL%rowcount);
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

      CLOSE lcu_phone;
      print_debug('VALIDATE_RECORDS: END Validate phone table records and mark them as VS/VE');
   EXCEPTION
      WHEN OTHERS THEN
         print_debug( 'VALIDATE_RECORDS: Unexpected error:' || sqlerrm ,gv_log_type);
         x_retcode   := 1;
         x_err_msg   := 'VALIDATE_RECORDS: Unexpected error:' || to_char(sqlcode) || '-' || sqlerrm;

END validate_phone_records;		 


--/****************************************************************************************************************
-- * Procedure  : IMPORT_COMP_DATA                                                                                *
-- * Purpose    : This Procedure is used to import the data for the companies                                    *
-- ****************************************************************************************************************/  

   PROCEDURE import_comp_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2)
   IS
   ln_row_id         VARCHAR2(500) :=null;
   ln_company_id	 NUMBER        := null;
   ln_company_number VARCHAR2(500) :=null;
   -- This Cursor is used to retrieve information from Staging Table --
      CURSOR lcu_select IS
	  
	  SELECT pcomp.*
	       , pcomp.rowid
		FROM XXAQV_PN_COMP_C011_STG pcomp
		WHERE pcomp.process_flag = 'Validate Success';
		
  BEGIN


      FOR lcu_r_cur_select IN lcu_select 
      LOOP 
        ln_row_id         :=null;
		ln_company_id	  := null;
		ln_company_number := null;
       pnt_comp_pkg.insert_row  
      (
          x_rowid                   => ln_row_id
        , x_company_id              => ln_company_id		  
        , x_company_number          => lcu_r_cur_select.company_number		  
        , x_last_update_date        => lcu_r_cur_select.last_update_date      		  
        , x_last_updated_by         => lcu_r_cur_select.last_updated_by       		  
        , x_creation_date           => lcu_r_cur_select.creation_date         		  
        , x_created_by              => lcu_r_cur_select.created_by            		  
        , x_last_update_login       => lcu_r_cur_select.last_update_login     		  
        , x_name                    => lcu_r_cur_select.name                  		  
        , x_enabled_flag            => lcu_r_cur_select.enabled_flag          		  
        , x_parent_company_id       => lcu_r_cur_select.parent_company_id     		  
        , x_attribute_category      => lcu_r_cur_select.attribute_category    		  
        , x_attribute1              => lcu_r_cur_select.attribute1            		  
        , x_attribute2              => lcu_r_cur_select.attribute2            		  
        , x_attribute3              => lcu_r_cur_select.attribute3            		  
        , x_attribute4              => lcu_r_cur_select.attribute4            		  
        , x_attribute5              => lcu_r_cur_select.attribute5            		  
        , x_attribute6              => lcu_r_cur_select.attribute6            		  
        , x_attribute7              => lcu_r_cur_select.attribute7            		  
        , x_attribute8              => lcu_r_cur_select.attribute8            		  
        , x_attribute9              => lcu_r_cur_select.attribute9            		  
        , x_attribute10             => lcu_r_cur_select.attribute10           		  
        , x_attribute11             => lcu_r_cur_select.attribute11           		  
        , x_attribute12             => lcu_r_cur_select.attribute12           		  
        , x_attribute13             => lcu_r_cur_select.attribute13           		  
        , x_attribute14             => lcu_r_cur_select.attribute14           		  
        , x_attribute15             => lcu_r_cur_select.attribute15           		  
        , x_org_id                  => lcu_r_cur_select.x_org_id       );         		  

         UPDATE XXAQV_PN_COMP_C011_STG 
            SET r12_company_id    = ln_company_id
			  , process_flag      = gv_import_success 
		  WHERE trim(upper(name)) = trim(upper(lcu_r_cur_select.name))
            AND company_number    = lcu_r_cur_select.company_number; 
      COMMIT;


      END LOOP;
      --COMMIT;
      
	  UPDATE XXAQV_PN_COMP_C011_STG 
	     SET process_flag = gv_import_error 
	   WHERE r12_company_id is null 
	     AND process_flag = gv_validate_success; 
	  COMMIT;
     EXCEPTION

      WHEN OTHERS 
      THEN
         print_debug( 'IMPORT: Exception processing Company Data:' || sqlerrm, gv_log_type );
         x_retcode   := 1;
         x_err_msg   := 'IMPORT: Unexpected error: ' || sqlerrm;
   END import_comp_data;	  
 

--/****************************************************************************************************************
-- * Procedure  : IMPORT_COMP_SITES_DATA                                                                                *
-- * Purpose    : This Procedure is used to import the data for the companies                                    *
-- ****************************************************************************************************************/  

   PROCEDURE import_comp_sites_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2)
   IS
   ln_row_id             VARCHAR2(500) := null;
   ln_company_site_id	 NUMBER        := null;
   ln_address_id         NUMBER        := null;
   -- This Cursor is used to retrieve information from Staging Table --
      CURSOR lcu_select IS
	  
	  SELECT pcomp.*
	       , pcomp.rowid
		FROM XXAQV_PN_COMP_SITES_C011_STG pcomp 
		WHERE pcomp.process_flag = 'Validate Success';
		
  BEGIN


      FOR lcu_r_cur_select IN lcu_select 
      LOOP 
        ln_row_id         :=null;
		ln_company_site_id	  := null;
		ln_address_id := null;
       pnt_comp_site_pkg.insert_row 
      (  
          x_rowid                        =>   ln_row_id
	    , x_company_site_id              =>   ln_company_site_id
	    , x_last_update_date             =>   lcu_r_cur_select.last_update_date           
	    , x_last_updated_by              =>   lcu_r_cur_select.last_updated_by            
	    , x_creation_date                =>   lcu_r_cur_select.creation_date              
	    , x_created_by                   =>   lcu_r_cur_select.created_by                 
	    , x_last_update_login            =>   lcu_r_cur_select.last_update_login          
	    , x_name                         =>   lcu_r_cur_select.name                       
	    , x_company_id                   =>   lcu_r_cur_select.x_company_id                 
	    , x_enabled_flag                 =>   lcu_r_cur_select.enabled_flag               
	    , x_company_site_code            =>   lcu_r_cur_select.company_site_code          
	    , x_address_id                   =>   ln_address_id                 
	    , x_lease_role_type              =>   lcu_r_cur_select.lease_role_type            
	    , x_attribute_category           =>   lcu_r_cur_select.attribute_category         
	    , x_attribute1                   =>   lcu_r_cur_select.attribute1                 
	    , x_attribute2                   =>   lcu_r_cur_select.attribute2                 
	    , x_attribute3                   =>   lcu_r_cur_select.attribute3                 
	    , x_attribute4                   =>   lcu_r_cur_select.attribute4                 
	    , x_attribute5                   =>   lcu_r_cur_select.attribute5                 
	    , x_attribute6                   =>   lcu_r_cur_select.attribute6                 
	    , x_attribute7                   =>   lcu_r_cur_select.attribute7                 
	    , x_attribute8                   =>   lcu_r_cur_select.attribute8                 
	    , x_attribute9                   =>   lcu_r_cur_select.attribute9                 
	    , x_attribute10                  =>   lcu_r_cur_select.attribute10                
	    , x_attribute11                  =>   lcu_r_cur_select.attribute11                
	    , x_attribute12                  =>   lcu_r_cur_select.attribute12                
	    , x_attribute13                  =>   lcu_r_cur_select.attribute13                
	    , x_attribute14                  =>   lcu_r_cur_select.attribute14                
	    , x_attribute15                  =>   lcu_r_cur_select.attribute15                
	    , x_address_line1                =>   lcu_r_cur_select.address_line1              
	    , x_address_line2                =>   lcu_r_cur_select.address_line2              
	    , x_address_line3                =>   lcu_r_cur_select.address_line3              
	    , x_address_line4                =>   lcu_r_cur_select.address_line4              
	    , x_county                       =>   lcu_r_cur_select.county                     
	    , x_city                         =>   lcu_r_cur_select.city                       
	    , x_state                        =>   lcu_r_cur_select.state                      
	    , x_province                     =>   lcu_r_cur_select.province                   
	    , x_zip_code                     =>   lcu_r_cur_select.zip_code                   
	    , x_country                      =>   lcu_r_cur_select.country                    
	    , x_territory_id                 =>   lcu_r_cur_select.territory_id               
	    , x_addr_last_update_date        =>   lcu_r_cur_select.last_update_date      
	    , x_addr_last_updated_by         =>   lcu_r_cur_select.last_updated_by       
	    , x_addr_creation_date           =>   lcu_r_cur_select.creation_date         
	    , x_addr_created_by              =>   lcu_r_cur_select.created_by            
	    , x_addr_last_update_login       =>   lcu_r_cur_select.last_update_login     
	    , x_addr_attribute_category      =>   lcu_r_cur_select.addr_attribute_category    
	    , x_addr_attribute1              =>   lcu_r_cur_select.addr_attribute1            
	    , x_addr_attribute2              =>   lcu_r_cur_select.addr_attribute2            
	    , x_addr_attribute3              =>   lcu_r_cur_select.addr_attribute3            
	    , x_addr_attribute4              =>   lcu_r_cur_select.addr_attribute4            
	    , x_addr_attribute5              =>   lcu_r_cur_select.addr_attribute5            
	    , x_addr_attribute6              =>   lcu_r_cur_select.addr_attribute6            
	    , x_addr_attribute7              =>   lcu_r_cur_select.addr_attribute7            
	    , x_addr_attribute8              =>   lcu_r_cur_select.addr_attribute8            
	    , x_addr_attribute9              =>   lcu_r_cur_select.addr_attribute9            
	    , x_addr_attribute10             =>   lcu_r_cur_select.addr_attribute10           
	    , x_addr_attribute11             =>   lcu_r_cur_select.addr_attribute11           
	    , x_addr_attribute12             =>   lcu_r_cur_select.addr_attribute12           
	    , x_addr_attribute13             =>   lcu_r_cur_select.addr_attribute13           
	    , x_addr_attribute14             =>   lcu_r_cur_select.addr_attribute14           
	    , x_addr_attribute15             =>   lcu_r_cur_select.addr_attribute15           
	    , x_org_id                       =>   lcu_r_cur_select.x_org_id                     
		 
		 );         		  

         UPDATE XXAQV_PN_COMP_SITES_C011_STG 
            SET r12_company_site_id     = ln_company_site_id
			  , r12_address_id          = ln_address_id
			  , process_flag            = gv_import_success 
		  WHERE trim(upper(name))       = trim(upper(lcu_r_cur_select.name)) 
		  AND trim(upper(company_name)) = trim(upper(lcu_r_cur_select.company_name))
		  AND company_number            = lcu_r_cur_select.company_number; 
      COMMIT;


      END LOOP;
      --COMMIT;
      
	  UPDATE XXAQV_PN_COMP_SITES_C011_STG 
	     SET process_flag = gv_import_error 
	   WHERE r12_company_site_id is null
	     AND process_flag = gv_validate_success	   ; 
	  COMMIT;
     EXCEPTION

      WHEN OTHERS 
      THEN
         print_debug( 'IMPORT: Exception processing Company Sites Data:' || sqlerrm, gv_log_type );
         x_retcode   := 1;
         x_err_msg   := 'IMPORT: Unexpected error: ' || sqlerrm;
   END import_comp_sites_data;	  
 
 

--/****************************************************************************************************************
-- * Procedure  : IMPORT_CONT_DATA                                                                                *
-- * Purpose    : This Procedure is used to import the data for the Contacts                                    *
-- ****************************************************************************************************************/  

   PROCEDURE import_cont_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2)
   IS
   ln_row_id             VARCHAR2(500) := null;
   ln_contact_id	     NUMBER        := null;
   
   -- This Cursor is used to retrieve information from Staging Table --
      CURSOR lcu_select IS
	  
	  SELECT pcomp.*
	       , pcomp.rowid
		FROM XXAQV_PN_CONT_C011_STG pcomp 
		WHERE pcomp.process_flag = 'Validate Success';
		
  BEGIN


      FOR lcu_r_cur_select IN lcu_select 
      LOOP 
        ln_row_id         :=null;
		ln_contact_id	  := null;
		
       pnt_cont_pkg.insert_row
      (  
          x_rowid                  => ln_row_id
		, x_contact_id             => ln_contact_id
		, x_company_site_id        => lcu_r_cur_select.x_company_site_id      
		, x_last_name              => lcu_r_cur_select.last_name            
		, x_created_by             => lcu_r_cur_select.created_by           
		, x_creation_date          => lcu_r_cur_select.creation_date        
		, x_last_updated_by        => lcu_r_cur_select.last_updated_by      
		, x_last_update_date       => lcu_r_cur_select.last_update_date     
		, x_last_update_login      => lcu_r_cur_select.last_update_login    
		, x_status                 => lcu_r_cur_select.status               
		, x_first_Name             => lcu_r_cur_select.first_Name           
		, x_job_title              => lcu_r_cur_select.job_title            
		, x_mail_stop              => lcu_r_cur_select.mail_stop            
		, x_email_address          => lcu_r_cur_select.email_address        
		, x_primary_flag           => lcu_r_cur_select.primary_flag         
		, x_company_or_location    => NULL  
		, x_attribute_category     => lcu_r_cur_select.attribute_category   
		, x_attribute1             => lcu_r_cur_select.attribute1           
		, x_attribute2             => lcu_r_cur_select.attribute2           
		, x_attribute3             => lcu_r_cur_select.attribute3           
		, x_attribute4             => lcu_r_cur_select.attribute4           
		, x_attribute5             => lcu_r_cur_select.attribute5           
		, x_attribute6             => lcu_r_cur_select.attribute6           
		, x_attribute7             => lcu_r_cur_select.attribute7           
		, x_attribute8             => lcu_r_cur_select.attribute8           
		, x_attribute9             => lcu_r_cur_select.attribute9           
		, x_attribute10            => lcu_r_cur_select.attribute10          
		, x_attribute11            => lcu_r_cur_select.attribute11          
		, x_attribute12            => lcu_r_cur_select.attribute12          
		, x_attribute13            => lcu_r_cur_select.attribute13          
		, x_attribute14            => lcu_r_cur_select.attribute14          
		, x_attribute15            => lcu_r_cur_select.attribute15          
		, x_org_id                 => lcu_r_cur_select.x_org_id               

		 );         		  

         UPDATE XXAQV_PN_CONT_C011_STG 
            SET r12_contact_id                  = ln_contact_id
			  , process_flag                    = gv_import_success 
		  WHERE trim(upper(company_site_name))  = trim(upper(lcu_r_cur_select.company_site_name)) 
		  AND trim(upper(company_name))         = trim(upper(lcu_r_cur_select.company_name))
		  AND nvl(last_name,'XX')               = nvl(lcu_r_cur_select.last_name,'XX')
		  AND nvl(first_name,'XX')              = nvl(lcu_r_cur_select.first_name,'XX')
		  AND nvl(email_address,'XX')           = nvl(lcu_r_cur_select.email_address,'XX')
		  AND lease_role_type                   = lcu_r_cur_select.lease_role_type
		  AND company_number                    = lcu_r_cur_select.company_number;
		 --
      COMMIT;


      END LOOP;
      --COMMIT;
      
	  UPDATE XXAQV_PN_CONT_C011_STG 
	     SET process_flag = gv_import_error 
	   WHERE r12_contact_id is null 
	     AND process_flag = gv_validate_success; 
	  COMMIT;
     EXCEPTION

      WHEN OTHERS 
      THEN
         print_debug( 'IMPORT: Exception processing Contacts Data:' || sqlerrm, gv_log_type );
         x_retcode   := 1;
         x_err_msg   := 'IMPORT: Unexpected error: ' || sqlerrm;
   END import_cont_data;

--/****************************************************************************************************************
-- * Procedure  : IMPORT_PHONE_DATA                                                                                *
-- * Purpose    : This Procedure is used to import the data for the Contacts                                    *
-- ****************************************************************************************************************/  

   PROCEDURE import_phone_data ( x_retcode        OUT   NUMBER
                              , x_err_msg        OUT   VARCHAR2)
   IS
   ln_row_id             VARCHAR2(500) := null;
   ln_phone_id	         NUMBER        := null;
   
   -- This Cursor is used to retrieve information from Staging Table --
      CURSOR lcu_select IS
	  
	  SELECT pcomp.*
	       , pcomp.rowid
		FROM XXAQV_PN_PHONE_C011_STG pcomp 
		WHERE pcomp.process_flag = 'Validate Success';
		
  BEGIN


      FOR lcu_r_cur_select IN lcu_select 
      LOOP 
        ln_row_id         :=null;
		ln_phone_id	  := null;
		
       pnt_phone_pkg.insert_row
      (  
	      X_Rowid              => ln_row_id   
	    , X_Phone_Id           => ln_phone_id   
	    , X_Last_Update_Date   => lcu_r_cur_select.last_update_date     
	    , X_Last_Updated_By    => lcu_r_cur_select.last_updated_by      
	    , X_Creation_Date      => lcu_r_cur_select.creation_date        
	    , X_Created_By         => lcu_r_cur_select.created_by           
	    , X_Phone_Number       => lcu_r_cur_select.phone_number         
	    , X_Status             => lcu_r_cur_select.status               
	    , X_Phone_Type         => lcu_r_cur_select.phone_type           
	    , X_Last_Update_Login  => lcu_r_cur_select.last_update_login    
	    , X_Contact_Id         => lcu_r_cur_select.x_contact_id           
	    , X_Area_Code          => lcu_r_cur_select.area_code            
	    , X_Extension          => lcu_r_cur_select.extension            
	    , X_Primary_Flag       => lcu_r_cur_select.primary_flag         
	    , X_Attribute_Category => lcu_r_cur_select.attribute_category   
	    , X_Attribute1         => lcu_r_cur_select.attribute1           
	    , X_Attribute2         => lcu_r_cur_select.attribute2           
	    , X_Attribute3         => lcu_r_cur_select.attribute3           
	    , X_Attribute4         => lcu_r_cur_select.attribute4           
	    , X_Attribute5         => lcu_r_cur_select.attribute5           
	    , X_Attribute6         => lcu_r_cur_select.attribute6           
	    , X_Attribute7         => lcu_r_cur_select.attribute7           
	    , X_Attribute8         => lcu_r_cur_select.attribute8           
	    , X_Attribute9         => lcu_r_cur_select.attribute9           
	    , X_Attribute10        => lcu_r_cur_select.attribute10          
	    , X_Attribute11        => lcu_r_cur_select.attribute11          
	    , X_Attribute12        => lcu_r_cur_select.attribute12          
	    , X_Attribute13        => lcu_r_cur_select.attribute13          
	    , X_Attribute14        => lcu_r_cur_select.attribute14          
	    , X_Attribute15        => lcu_r_cur_select.attribute15          
	    , X_Org_id             => lcu_r_cur_select.x_org_id               
	
		 );         		  

         UPDATE XXAQV_PN_PHONE_C011_STG 
            SET r12_phone_id              = ln_phone_id
			  , process_flag              = gv_import_success 
		  WHERE upper(company_site_name)  = upper(lcu_r_cur_select.company_site_name)
		  AND company_number              = lcu_r_cur_select.company_number
		  AND upper(last_name)            = upper(lcu_r_cur_select.last_name)
		  AND nvl(first_name,'XX')        = nvl(lcu_r_cur_select.first_name,'XX')
		  AND nvl(email_address,'XX')     = nvl(lcu_r_cur_select.email_address,'XX')
		  AND nvl(phone_number,'XX')      = nvl(lcu_r_cur_select.phone_number,'XX')
		  AND nvl(job_title,'XX')         = nvl(lcu_r_cur_select.job_title,'XX'); 
      COMMIT;


      END LOOP;
      --COMMIT;
      
	  UPDATE XXAQV_PN_PHONE_C011_STG 
	     SET process_flag = gv_import_error 
	   WHERE r12_phone_id is null 
	     AND process_flag = gv_validate_success; 
	  COMMIT;
     EXCEPTION

      WHEN OTHERS 
      THEN
         print_debug( 'IMPORT: Exception processing Phone Data:' || sqlerrm, gv_log_type );
         x_retcode   := 1;
         x_err_msg   := 'IMPORT: Unexpected error: ' || sqlerrm;
   END import_phone_data;
   
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
 
           -- for email masking

      SELECT name
        INTO gv_db_name
        FROM v$database;
 
 
    IF gv_module='COMPANY'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_COMP_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_comp_data( x_retcode       => ln_retcode
                                     , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_comp_records( x_retcode       => ln_retcode
                                  , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'IMPORT' 
        THEN
             import_comp_data ( x_retcode       => ln_retcode
                              , x_err_msg       => lv_err_msg);

             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


    END IF;

	IF gv_module='COMPANY_SITES'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_COMP_SITES_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_comp_sites_data( x_retcode       => ln_retcode
                                    , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_comp_sites_records( x_retcode       => ln_retcode
                                 , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'IMPORT' 
        THEN
             import_comp_sites_data ( x_retcode       => ln_retcode
                           , x_err_msg       => lv_err_msg);

             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


    END IF;
	
  IF gv_module='COMPANY_CONTACTS'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_CONT_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_cont_data( x_retcode       => ln_retcode
                                     , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_cont_records( x_retcode       => ln_retcode
                                  , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'IMPORT' 
        THEN
             import_cont_data ( x_retcode       => ln_retcode
                              , x_err_msg       => lv_err_msg);

             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


    END IF;

  IF gv_module='COMPANY_PHONE'
	THEN
	 print_debug( gv_module );
        IF gv_conv_mode = 'EXTRACT' 
        THEN
            lv_err_msg := 'Deleting Error Records from Staging table for extract mode';
             print_debug( lv_err_msg, gv_log_type );
             --
            DELETE FROM XXAQV_PN_CONT_C011_STG 
                  WHERE process_flag NOT IN ('Load Success','Import Success');
             -- 
                    extract_phone_data( x_retcode       => ln_retcode
                                     , x_err_msg       => lv_err_msg);
                IF ln_retcode <> 0 THEN
                    RAISE ex_errors;
                END IF;           
        END IF;

        IF gv_conv_mode = 'MAP' 
        THEN
             validate_phone_records( x_retcode       => ln_retcode
                                  , x_err_msg       => lv_err_msg );
             IF ln_retcode <> 0 
             THEN
                RAISE ex_errors;
             END IF;
        END IF;


        IF gv_conv_mode = 'IMPORT' 
        THEN
             import_phone_data ( x_retcode       => ln_retcode
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
      print_debug( 'MAIN: END Import Process'
                 , gv_log_type   );
   EXCEPTION
      WHEN ex_errors 
      THEN
         print_debug( 'MAIN: Exception processing lease contact data ex_errors:' || lv_err_msg
                    , gv_log_type );
         errbuff   := 'MAIN: Exception processing lease contact data Data:' || lv_err_msg;
         retcode   := 2;
      WHEN OTHERS 
      THEN
         print_debug( 'MAIN: Exception processing lease contact data Data:' || lv_err_msg
                    , gv_log_type );
         errbuff   := 'MAIN: Unexpected Exception processing lease contact data Data:' || to_char(sqlcode) || '-' || sqlerrm;
         retcode   := 2;
   END main;
   --
END xxaqv_pn_lease_cont_pkg;
/
SHOW ERRORS
/