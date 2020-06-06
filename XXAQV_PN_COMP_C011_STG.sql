--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Table                                                                                   *
-- * Application Module : XXAQV                                                                                   *
-- * Table Name         : XXAQV_PN_COMP_C011_STG                                                                  *
-- * Script Name        : XXAQV_PN_COMP_C011_STG.sql                                                              *
-- * Purpose            : Create a Table to load Company Records.                                                 *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 0.1         CTS               28/05/2020     Initial Version                                                 *
-- ***************************************************************************************************************/
CREATE TABLE xxaqv.xxaqv_pn_comp_c011_stg ( r12_company_id               NUMBER        
                                          , company_number               VARCHAR2(30)  
                                          , last_update_date             DATE          
                                          , last_updated_by              NUMBER        
                                          , creation_date                DATE          
                                          , created_by                   NUMBER        
                                          , last_update_login            NUMBER        
                                          , name                         VARCHAR2(80)  
                                          , enabled_flag                 VARCHAR2(1)   
                                          , parent_company_id            NUMBER        
                                          , attribute_category           VARCHAR2(30)  
                                          , attribute1                   VARCHAR2(150) 
                                          , attribute2                   VARCHAR2(150) 
                                          , attribute3                   VARCHAR2(150) 
                                          , attribute4                   VARCHAR2(150) 
                                          , attribute5                   VARCHAR2(150) 
                                          , attribute6                   VARCHAR2(150) 
                                          , attribute7                   VARCHAR2(150) 
                                          , attribute8                   VARCHAR2(150) 
                                          , attribute9                   VARCHAR2(150) 
                                          , attribute10                  VARCHAR2(150) 
                                          , attribute11                  VARCHAR2(150) 
                                          , attribute12                  VARCHAR2(150) 
                                          , attribute13                  VARCHAR2(150) 
                                          , attribute14                  VARCHAR2(150) 
                                          , attribute15                  VARCHAR2(150) 
                                          , parent_company_name          VARCHAR2(80)  
                                          , x_org_id                     NUMBER
                                          , ou_name                      VARCHAR2(150)
                                          , company_name                 VARCHAR2(150)
                                          , error_message               VARCHAR2(4000)
                                          , process_flag                VARCHAR2(20) );

EXEC ad_zd_table.upgrade('XXAQV','XXAQV_PN_COMP_C011_STG');
/

SHOW ERRORS
/
