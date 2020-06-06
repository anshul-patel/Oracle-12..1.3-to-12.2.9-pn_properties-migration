CREATE OR REPLACE PACKAGE xxaqv_pn_lease_cont_pkg AS
--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Package Specification                                                                   *
-- * Application Module : Arqiva Custom Application (xxaqv)                                                       *
-- * Packagage Name     : XXAQV_PN_LEASE_CONT_PKG                                                                 *
-- * Script Name        : XXAQV_PN_LEASE_CONT_PKG.pks                                                             *
-- * Purpose            : Used for migrating lease contacts from 12.1.3 to 12.2.9                                 *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 1.0         CTS                29/05/2020     Initial Version                                                *
-- ****************************************************************************************************************


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
   );
END xxaqv_pn_lease_cont_pkg;
/
SHOW ERRORS
/