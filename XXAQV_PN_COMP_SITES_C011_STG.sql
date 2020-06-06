--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Table                                                                                   *
-- * Application Module : XXAQV                                                                                   *
-- * Table Name         : XXAQV_PN_COMP_SITES_C011_STG                                                            *
-- * Script Name        : XXAQV_PN_COMP_SITES_C011_STG.sql                                                        *
-- * Purpose            : Create a Table to load  Company site Records.                                           *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 0.1         CTS               28/05/2020     Initial Version                                                 *
-- ***************************************************************************************************************/
CREATE TABLE xxaqv.xxaqv_pn_comp_sites_c011_stg ( r12_company_site_id              NUMBER         
                                                , last_update_date                 DATE           
                                                , last_updated_by                  NUMBER         
                                                , creation_date                    DATE           
                                                , created_by                       NUMBER         
                                                , last_update_login                NUMBER         
                                                , name                             VARCHAR2(40)   
                                                , x_company_id                     NUMBER         
                                                , enabled_flag                     VARCHAR2(1)    
                                                , company_site_code                VARCHAR2(15)   
                                                , lease_role_type                  VARCHAR2(30)   
                                                , r12_address_id                   NUMBER         
                                                , attribute_category               VARCHAR2(30)   
                                                , attribute1                       VARCHAR2(150)  
                                                , attribute2                       VARCHAR2(150)  
                                                , attribute3                       VARCHAR2(150)  
                                                , attribute4                       VARCHAR2(150)  
                                                , attribute5                       VARCHAR2(150)  
                                                , attribute6                       VARCHAR2(150)  
                                                , attribute7                       VARCHAR2(150)  
                                                , attribute8                       VARCHAR2(150)  
                                                , attribute9                       VARCHAR2(150)  
                                                , attribute10                      VARCHAR2(150)  
                                                , attribute11                      VARCHAR2(150)  
                                                , attribute12                      VARCHAR2(150)  
                                                , attribute13                      VARCHAR2(150)  
                                                , attribute14                      VARCHAR2(150)  
                                                , attribute15                      VARCHAR2(150)  
                                                , address_line1                    VARCHAR2(240)  
                                                , address_line2                    VARCHAR2(240)  
                                                , address_line3                    VARCHAR2(240)  
                                                , address_line4                    VARCHAR2(240)  
                                                , county                           VARCHAR2(60)   
                                                , city                             VARCHAR2(60)   
                                                , state                            VARCHAR2(60)   
                                                , province                         VARCHAR2(60)   
                                                , zip_code                         VARCHAR2(60)   
                                                , country                          VARCHAR2(60)   
                                                , territory_id                     NUMBER         
                                                , addr_attribute_category          VARCHAR2(30)   
                                                , addr_attribute1                  VARCHAR2(150)  
                                                , addr_attribute2                  VARCHAR2(150)  
                                                , addr_attribute3                  VARCHAR2(150)  
                                                , addr_attribute4                  VARCHAR2(150)  
                                                , addr_attribute5                  VARCHAR2(150)  
                                                , addr_attribute6                  VARCHAR2(150)  
                                                , addr_attribute7                  VARCHAR2(150)  
                                                , addr_attribute8                  VARCHAR2(150)  
                                                , addr_attribute9                  VARCHAR2(150)  
                                                , addr_attribute10                 VARCHAR2(150)  
                                                , addr_attribute11                 VARCHAR2(150)  
                                                , addr_attribute12                 VARCHAR2(150)  
                                                , addr_attribute13                 VARCHAR2(150)  
                                                , addr_attribute14                 VARCHAR2(150)  
                                                , addr_attribute15                 VARCHAR2(150)  
                                                , address_style                    VARCHAR2(30)   
                                                , territory_short_name             VARCHAR2(80)   
                                                , lease_role                       VARCHAR2(80)   
                                                , concatenated_address             VARCHAR2(4000) 
                                                , x_org_id                         NUMBER
                                                , ou_name                          VARCHAR2(40)
                                                , company_name                     VARCHAR2(150)
                                                , company_number                   VARCHAR2(30)  
										        , error_message                    VARCHAR2(4000)
                                                , process_flag                     VARCHAR2(20)      );


EXEC ad_zd_table.upgrade('XXAQV','XXAQV_PN_COMP_SITES_C011_STG');
/

SHOW ERRORS
/
