select *
from (
    select treaty_name,
        legal_entity,
        reinsurance_assumed_ceded_flag,
        reinsurance_account_flag,
        reinsurance_basis,
        type_of_business_reinsured,
        reinsurance_group_individual_flag,
        source_system_reinsurance_counterparty_code,
        geac_centers,
        cessionidentifier,
        row_number() over (
            partition by treaty_name,
                legal_entity,
                geac_centers
            order by rcd_crtd_dt desc
        ) as rn
    from time_sch.lkp_reinsurance_attributes
)
where rn = 1
