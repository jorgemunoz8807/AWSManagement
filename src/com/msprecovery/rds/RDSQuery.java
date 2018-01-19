/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.rds;

/**
 *
 * @author jmunoz
 */
public class RDSQuery {

    public static String GET_GLOBAL_UNIQUE_MBRS_BY_HMO(String hmo, int startLimit, int batchSize) {
        String query = "SELECT `msp_patient_id`, `msp_mrd_id`, `msp_member_id`, `msp_client_id`, `msp_client`, `msp_hmo`, `msp_MEMB_ID`, `msp_MEMB_NAME`, `msp_MEMB_MNAME`, `msp_MEMB_LNAME`, `msp_MEMB_FULL_NAME`, `msp_MEMB_NAME_UPDATED`, `msp_MEMB_MNAME_UPDATED`, `msp_MEMB_LNAME_UPDATED`, `msp_MEMB_AGE`, `msp_MEMB_GENDER`, `msp_MEMB_RELATION`, `msp_MEMB_EMPLOYEE_GROUP`, `msp_MEMB_PLANID`, `msp_MEMB_EFFMONTH`, `msp_MEMB_HIC`, `msp_MEMB_SS`, `msp_MEMB_DOB`, `msp_MEMB_PHONE`, `msp_MEMB_ADDRESS`, `msp_MEMB_COUNTY`, `msp_MEMB_STATE`, `msp_MEMB_CITY`, `msp_MEMB_ZIP`, `msp_crd_id`, `msp_origin_file`, `msp_crd_batch`, `msp_min_dos`, `msp_max_dos`, `msp_bill_amount`, `msp_paid_amount`, `msp_commercial_high`, `msp_total_claims`, `msp_created`, `msp_updated`, `msp_all_mdl_id`, `msp_crash_report_fuzzy`, `msp_mdl_fuzzy`, `msp_eligibility`, `msp_abd`"
                + " FROM `MSP_MASTER_MBRS`.`msp_global_unique_mbrs` "
                + "WHERE msp_hmo = '" + hmo + "'"
                + " LIMIT " + startLimit + "," + batchSize + "";

        return query;
    }

    public static String GET_GLOBAL_CLAIMS_FUNNELS_BY_HMO(String hmo, int startLimit, int batchSize) {
        String query = "SELECT CONCAT_WS('-',msp_mrd_id,msp_funnel, SUBSTRING_INDEX(msp_client, '-', -1)) as msp_global_funnel_id, "
                + " `msp_frd_id`, `msp_mrd_id`, `msp_funnel`, `msp_member_id`, `msp_client`"
                + " FROM `MSP_MASTER_FUNNELS`.`msp_funnel_" + hmo.toLowerCase() + "` "
                + " LIMIT " + startLimit + "," + batchSize + "";
//        System.out.println(query);
//        System.exit(1);
        return query;
    }

}
