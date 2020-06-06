CREATE OR REPLACE PACKAGE xxaqv_pn_prop_pkg AS
--/*------------------------------------------------- Arqiva -----------------------------------------------------*
-- ****************************************************************************************************************
-- * Type               : Package Specification                                                                   *
-- * Application Module : Arqiva Custom Application (xxaqv)                                                       *
-- * Packagage Name     : XXAQV_PN_PROP_PKG                                                                       *
-- * Script Name        : XXAQV_PN_PROP_PKG.pks                                                                   *
-- * Purpose            : Package for All property related Data.                                                  *
-- * Company            : Cognizant Technology Solutions.                                                         *
-- *                                                                                                              *
-- * Change History                                                                                               *
-- * Version     Created By        Creation Date    Comments                                                      *
-- *--------------------------------------------------------------------------------------------------------------*
-- * 1.0         CTS                19/05/2020     Initial Version                                                *
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
END xxaqv_pn_prop_pkg;
/
SHOW ERRORS
/