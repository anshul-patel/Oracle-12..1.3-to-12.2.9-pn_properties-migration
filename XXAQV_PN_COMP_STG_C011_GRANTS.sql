--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Grant                                                                                   *
-- * Application Module : APPS                                                                                    *
-- * Packagage Name     : XXAQV_PN_COMP_STG_GRANTS                                                                *
-- * Script Name        : XXAQV_PN_COMP_STG_GRANTS.sql                                                            *
-- * Purpose            : Script for Grants on schema                                                             *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 0.1         CTS               26/05/2020     Initial Version                                                 *
-- ****************************************************************************************************************

GRANT ALL ON xxaqv.xxaqv_pn_comp_c011_stg TO xxdm;

GRANT ALL ON xxaqv.xxaqv_pn_comp_sites_c011_stg TO xxdm;

GRANT ALL ON xxaqv.xxaqv_pn_cont_c011_stg TO xxdm;

GRANT ALL ON xxaqv.xxaqv_pn_phone_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_comp_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_comp_sites_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_cont_c011_stg TO xxdm;

GRANT ALL ON apps.xxaqv_pn_phone_c011_stg TO xxdm;


/