--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Table                                                                                   *
-- * Application Module : XXAQV                                                                                   *
-- * Table Name         : XXAQV_PN_PROP_C011_STG                                                                  *
-- * Script Name        : XXAQV_PN_PROP_C011_STG.sql                                                              *
-- * Purpose            : Create a Table to load  Property Records.                                               *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 0.1         CTS               19/05/2020     Initial Version                                                 *
-- ***************************************************************************************************************/
CREATE TABLE xxaqv.xxaqv_pn_prop_c011_stg ( r12_property_id          NUMBER
                                          , property_id              NUMBER
                                          , last_update_date         DATE
                                          , last_updated_by          NUMBER
                                          , creation_date            DATE
                                          , created_by               NUMBER
                                          , last_update_login        NUMBER
                                          , property_name            VARCHAR2(300)
                                          , property_code            VARCHAR2(150)
                                          , x_location_park_id       NUMBER           
                                          , zone                     VARCHAR2(300)
                                          , district                 VARCHAR2(300)
                                          , country                  VARCHAR2(100)
                                          , description              VARCHAR2(340)
                                          , portfolio                VARCHAR2(300)
                                          , tenure                   VARCHAR2(300)
                                          , class                    VARCHAR2(300)
                                          , property_status          VARCHAR2(300)
                                          , condition                VARCHAR2(300)
                                          , active_property          VARCHAR2(1)   
                                          , attribute_category       VARCHAR2(100)
                                          , attribute1               VARCHAR2(150)
                                          , attribute2               VARCHAR2(150)
                                          , attribute3               VARCHAR2(150)
                                          , attribute4               VARCHAR2(150)
                                          , attribute5               VARCHAR2(150)
                                          , attribute6               VARCHAR2(150)
                                          , attribute7               VARCHAR2(150)
                                          , attribute8               VARCHAR2(150)
                                          , attribute9               VARCHAR2(150)
                                          , attribute10              VARCHAR2(150)
                                          , attribute11              VARCHAR2(150)
                                          , attribute12              VARCHAR2(150)
                                          , attribute13              VARCHAR2(150)
                                          , attribute14              VARCHAR2(150)
                                          , attribute15              VARCHAR2(150)
                                          , x_org_id                 NUMBER  
                                          , error_message            VARCHAR2(2000)
                                          , process_flag             VARCHAR(40)
                                          , ou_name                  VARCHAR2(40)
                                          , location_park_name       VARCHAR2(100) );


EXEC ad_zd_table.upgrade('XXAQV','XXAQV_PN_PROP_C011_STG');
/

SHOW ERRORS
/
