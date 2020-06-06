--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Table                                                                                   *
-- * Application Module : XXAQV                                                                                   *
-- * Table Name         : XXAQV_PN_LOC_C011_STG                                                                        *
-- * Script Name        : XXAQV_PN_LOC_C011_STG.sql                                                                    *
-- * Purpose            : Create a Table to load Location Records.                                                *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation DATE    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 1.0         CTS               20/05/2020     Initial Version                                                 *
-- ***************************************************************************************************************/
CREATE TABLE xxaqv.xxaqv_pn_loc_c011_stg ( batch_name                           VARCHAR2(80)
                                         , entry_type                           VARCHAR2(51)
                                         , r12_location_id                      NUMBER
                                         , location_type_lookup_code            VARCHAR2(80)
                                         , location_code                        VARCHAR2(140)
                                         , lease_or_owned                       VARCHAR2(80)
                                         , floor                                VARCHAR2(70)
                                         , office                               VARCHAR2(70)
                                         , address_line1                        VARCHAR2(290)
                                         , address_line2                        VARCHAR2(290)
                                         , address_line3                        VARCHAR2(290)
                                         , address_line4                        VARCHAR2(290)
                                         , county                               VARCHAR2(90)
                                         , city                                 VARCHAR2(90)
                                         , state                                VARCHAR2(90)
                                         , province                             VARCHAR2(90)
                                         , zip_code                             VARCHAR2(90)
                                         , country                              VARCHAR2(90)
                                         , address_style                        VARCHAR2(80)
                                         , max_capacity                         NUMBER
                                         , optimum_capacity                     NUMBER
                                         , rentable_area                        NUMBER
                                         , usable_area                          NUMBER
                                         , allocate_cost_center_code            VARCHAR2(80)
                                         , uom_code	                           VARCHAR2(53)
                                         , parent_location_id                   NUMBER
                                         , last_update_date                     DATE
                                         , last_update_login                    NUMBER
                                         , created_by                           NUMBER
                                         , creation_date                        DATE
                                         , last_updated_by                      NUMBER
                                         , attribute_category                   VARCHAR2(80)
                                         , attribute1                           VARCHAR2(200)
                                         , attribute2                           VARCHAR2(200)
                                         , attribute3                           VARCHAR2(200)
                                         , attribute4                           VARCHAR2(200)
                                         , attribute5                           VARCHAR2(200)
                                         , attribute6                           VARCHAR2(200)
                                         , attribute7                           VARCHAR2(200)
                                         , attribute8                           VARCHAR2(200)
                                         , attribute9                           VARCHAR2(200)
                                         , attribute10                          VARCHAR2(200)
                                         , attribute11                          VARCHAR2(200)
                                         , attribute12                          VARCHAR2(200)
                                         , attribute13                          VARCHAR2(200)
                                         , attribute14                          VARCHAR2(200)
                                         , attribute15                          VARCHAR2(200)
                                         , addr_attribute_category              VARCHAR2(80)
                                         , addr_attribute1                      VARCHAR2(200)
                                         , addr_attribute2                      VARCHAR2(200)
                                         , addr_attribute3                      VARCHAR2(200)
                                         , addr_attribute4                      VARCHAR2(200)
                                         , addr_attribute5                      VARCHAR2(200)
                                         , addr_attribute6                      VARCHAR2(200)
                                         , addr_attribute7                      VARCHAR2(200)
                                         , addr_attribute8                      VARCHAR2(200)
                                         , addr_attribute9                      VARCHAR2(200)
                                         , addr_attribute10                     VARCHAR2(200)
                                         , addr_attribute11                     VARCHAR2(200)
                                         , addr_attribute12                     VARCHAR2(200)
                                         , addr_attribute13                     VARCHAR2(200)
                                         , addr_attribute14                     VARCHAR2(200)
                                         , addr_attribute15                     VARCHAR2(200)
                                         , transferred_to_cad                   VARCHAR2(51)
                                         , transferred_to_pn                    VARCHAR2(51)
                                         , error_message                        VARCHAR2(290)
                                         , source                               VARCHAR2(180)
                                         , request_id                           NUMBER
                                         , program_application_id               NUMBER
                                         , program_id                           NUMBER
                                         , program_update_date                  DATE
                                         , space_type_lookup_code               VARCHAR2(80)
                                         , gross_area                           NUMBER
                                         , assignable_area                      NUMBER
                                         , class                                VARCHAR2(80)
                                         , status_type                          VARCHAR2(80)
                                         , suite                                VARCHAR2(80)
                                         , common_area                          NUMBER
                                         , common_area_flag                     VARCHAR2(80)
                                         , function_type_lookup_code            VARCHAR2(80)
                                         , location_alias                       VARCHAR2(80)
                                         , x_property_id                        NUMBER
                                         , standard_type_lookup_code            VARCHAR2(80)
                                         , active_start_date                    DATE
                                         , active_end_date                      DATE
                                         , change_mode                          VARCHAR2(80)
                                         , change_date                          DATE
                                         , new_active_start_date                DATE
                                         , new_active_end_date                  DATE
                                         , site_id                              NUMBER
                                         , building                             VARCHAR2(100)
                                         , property_name                        VARCHAR2(150)
                                         , property_code                        VARCHAR2(150)
                                         , process_flag                         VARCHAR2(40)
);

EXEC ad_zd_table.upgrade('XXAQV','XXAQV_PN_LOC_C011_STG');
/

SHOW ERRORS
/
