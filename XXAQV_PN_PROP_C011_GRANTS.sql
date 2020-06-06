--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Grant                                                                                   *
-- * Application Module : APPS                                                                                    *
-- * Packagage Name     : XXAQV_PN_PROP_GRANTS                                                                    *
-- * Script Name        : XXAQV_PN_PROP_GRANTS.sql                                                                *
-- * Purpose            : Script for Grants on schema                                                             *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 0.1         CTS               26/05/2020     Initial Version                                                 *
-- ****************************************************************************************************************

GRANT ALL ON xxaqv.xxaqv_pn_prop_c011_stg TO xxdm;

GRANT ALL ON xxaqv.xxaqv_pn_loc_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_prop_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_loc_c011_stg TO xxdm;

GRANT ALL ON apps.pn_locations_itf TO xxdm;
/