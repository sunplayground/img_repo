-- DDAY
with 
email_list as ( --ok
    select distinct 
        email, 
        category
    from thbdbi_partner.th_staff
    where ingestion_timestamp in (select max(ingestion_timestamp) from thbdbi_partner.th_staff)
        and email = '$useremail'
        -- and email = ruby.rattanar@shopee.com
)
,cmi_input as --ok
(
        select  
            distinct date(grass_date) grass_date
            ,try_cast(deals_start_date as date) deals_start_date
            ,try_cast(deals_end_date  as date) deals_end_date
            ,replace(exclude_date, ',', '|') exclude_date_regex
            ,try_cast(is_dday  as date) is_dday_date
            ,try_cast(is_midmonth  as date) is_midmonth_date
            ,day(try_cast(deals_end_date  as date) - try_cast(deals_start_date  as date) + interval '1' day) num_day
        from    
            thbdbi_partner.cmi_campaign_deals_and_cfs
        where   
            ingestion_timestamp = (select max(ingestion_timestamp) from thbdbi_partner.cmi_campaign_deals_and_cfs)
            and try_cast(grass_date  as date) = ${param_campaign_date}
)
, s_type as ( --ok
    select distinct
        shopid shop_id
        , case 
            when seller_tier_lv1 in ('CB') then seller_tier_lv2
            when seller_tier_lv2 in ('SKAM', 'ST-MS') then seller_tier_lv2 
            else seller_tier_lv2
        end seller_type
        , seller_tier_lv2
    from thbdbi_partner.seller_type_v2
    where exec_ts in (select max(exec_ts) col from thbdbi_partner.seller_type_v2)
)

, ep_promotion_id as (
    select try_cast(trim(promotionid) as bigint) as promotion_id 
    from thbdbi_sellermgmt.shopee_th_bi_team__new_buyer_exclusive_price_promotionid
    where ingestion_timestamp in (
        select max(ingestion_timestamp) as madate 
        from thbdbi_sellermgmt.shopee_th_bi_team__new_buyer_exclusive_price_promotionid
    )
)
, order_mart as ( --ok
    select 
        t1.shop_id
        , coalesce(t2.seller_type, 'LT') seller_type        
        , t1.item_id 
        , t1.model_id 
        , t1.item_price_pp
        , t1.item_rebate_by_shopee_amt
        , t1.item_amount 
        , if(t1.flash_sale_type_id in (0,1,3,4),1,0) as is_flash
        , if(t3.shopid is not null, 1, 0) is_sbd
        , if(date(from_unixtime(create_timestamp - 3600)) = (select distinct is_dday_date from cmi_input), 1, 0) is_dday -- check
        , if(date(from_unixtime(create_timestamp - 3600)) = (select distinct is_midmonth_date from cmi_input), 1, 0) is_midmonth_dday  -- check
        , t1.create_timestamp
        , t1.order_fraction
        , t1.gmv_usd 
        , t1.gmv 
        , t1.item_offer_id
        , t1.is_wholesale
    from mp_order.dwd_order_item_all_ent_df__th_s0_live t1
    left join s_type t2 
        on t1.shop_id = t2.shop_id
    left join (
            select distinct 
                try_cast(try_cast(trim(shopid) as double) as bigint) shopid
                ,try_cast(sbd_date as date) grass_date
            from thbdbi_partner.sbd_list
            where ingestion_timestamp = (select max(ingestion_timestamp) from thbdbi_partner.sbd_list)
            ) t3 
        on date(split(t1.create_datetime,' ')[1]) = t3.grass_date 
        and t1.shop_id =t3.shopid
    where 
        date(from_unixtime(t1.create_timestamp - 3600)) between (select distinct deals_start_date from cmi_input) and (select distinct deals_end_date from cmi_input)  -- check
        -- and if((select exclude_date from cmi_input) is null, true, date(from_unixtime(t1.create_timestamp - 3600)) not in (select exclude_date from cmi_input))   -- check
        and if((select exclude_date_regex from cmi_input) = '', true,not regexp_like(cast(date(from_unixtime(t1.create_timestamp - 3600)) as varchar), (select exclude_date_regex from cmi_input)))
        
        and if(false=cast(${param_filter_itemid} as boolean), true,t1.item_id in ${param_itemid_list})
        and if(false=cast(${param_filter_shopid} as boolean), true,t1.shop_id in ${param_shopid_list})
        -- and split(t1.kpi_categories[1][1], ':')[2] in ${param_main_category}
        and if(false=cast(${param_filter_seller_type} as boolean), true, coalesce(t2.seller_type, 'LT') in ${param_seller_type_input})
        and t1.bundle_deal_id is null
        and t1.is_add_on_sub_item = 0
        -- exclude exclusive price 
        and t1.item_promotion_id not in (select distinct promotion_id from ep_promotion_id)
)
, model_mart as ( --ok
    select 
        t1.shop_id 
        , t2.seller_type
        , t1.item_id 
        , t1.model_id 
        , t1.model_price price ---final_price
        , t1.model_price_before_discount price_before_discount ---price_bf_disc
        , t1.model_shopee_rebate_amt shopee_rebate_amt ---rebate_price
        , t1.model_price + t1.model_shopee_rebate_amt as seller_offer_price
        , if(date(from_unixtime(t1.model_create_timestamp - 3600)) > (select distinct deals_end_date from cmi_input),'Yes','No') is_new_sku  -- check
        , t1.model_create_timestamp create_timestamp
        , case when t1.item_status = 0 then 'delete'
               when t1.item_status = 1 then 'normal'
               when t1.item_status = 2 then 'reviewing'
               when t1.item_status = 3 then 'banned'
               when t1.item_status in (4,5) then 'invalid'
               when t1.item_status = 6 then 'offensive hide'
               when t1.item_status = 7 then 'auditing'
        end as item_status_kw
        , t1.item_status
        , t1.model_status 
        , t1.item_name 
        , t1.model_name 
        , t1.model_stock 
        , t1.item_stock   
        , t1.shop_name username
        , if(cardinality(t1.kpi_categories[1]) is null, null, split(t1.kpi_categories[1][1], ':')[2]) main_category
        , if(cardinality(t1.kpi_categories[1]) < 2, null, split(t1.kpi_categories[1][2], ':')[2]) sub_category
        , if(cardinality(t1.fe_display_categories[1]) is null, null, split(t1.fe_display_categories[1][1], ':')[2]) fe_main_category
        , if(cardinality(t1.fe_display_categories[1]) < 2, null, split(t1.fe_display_categories[1][2], ':')[2]) fe_sub_category
        , if(cardinality(t1.fe_display_categories[1]) < 3 or cardinality(split(t1.fe_display_categories[1][3], ':')) < 2,null,split(t1.fe_display_categories[1][3], ':')[2]) fe_l3_category
    from mp_item.dim_model__th_s0_live t1
    left join s_type t2 on t1.shop_id = t2.shop_id
    -- inner join ep_info t3 on t1.model_id = t3.model_id
    where grass_date = (current_date - interval '2' day)
        and if(false=cast(${param_filter_itemid} as boolean), true,t1.item_id in ${param_itemid_list})
        and if(false=cast(${param_filter_shopid} as boolean), true,t1.shop_id in ${param_shopid_list})
        and if(false=cast(${param_filter_seller_type} as boolean), true, coalesce(t2.seller_type, 'LT') in ${param_seller_type_input})  
        and if(${param_is_exclude_no_sale_sku} = 'exclude', t1.model_id in (select distinct model_id from order_mart),true)
)
, all as ( --ok
    select
        shop_id shopid,
        seller_type,
        item_id itemid,
        model_id modelid,
        min(distinct if(is_sbd=0, item_price_pp+(item_rebate_by_shopee_amt/item_amount))) as lowest_offer_incl_cfs,
        min(distinct if(is_flash=0 and is_sbd=0, item_price_pp+(item_rebate_by_shopee_amt/item_amount))) as lowest_offer_excl_cfs,
        avg(distinct if(is_sbd=0, item_price_pp+(item_rebate_by_shopee_amt/item_amount))) as avg_offer_incl_cfs,
        avg(distinct if(is_flash=0 and is_sbd=0, item_price_pp+(item_rebate_by_shopee_amt/item_amount))) as avg_offer_excl_cfs
    from order_mart
    where item_offer_id = 0 and is_wholesale = 0
    group by 1,2,3,4
)
, performance as ( --ok
    select
        item_id as itemids,
        model_id as modelids,
        sum(if(is_dday = 1 and is_flash = 1 and is_sbd = 0,order_fraction)) as dday_cfs_order,
        sum(if(is_dday = 1 and is_flash = 0 and is_sbd = 0,order_fraction)) as dday_non_cfs_order,
        sum(if(is_dday = 1 and is_flash = 1 and is_sbd = 0,gmv_usd)) as dday_cfs_gmv_usd,
        sum(if(is_dday = 1 and is_flash = 0 and is_sbd = 0,gmv_usd)) as dday_non_cfs_gmv_usd,
        sum(if(is_dday = 1 and is_sbd = 0,item_amount)) as dday_qty_sold,
        
        sum(if(is_midmonth_dday = 1 and is_flash = 1 and is_sbd = 0,order_fraction)) as midmonth_dday_cfs_order,
        sum(if(is_midmonth_dday = 1 and is_flash = 0 and is_sbd = 0,order_fraction)) as midmonth_dday_non_cfs_order,
        sum(if(is_midmonth_dday = 1 and is_flash = 1 and is_sbd = 0,gmv_usd)) as midmonth_dday_cfs_gmv_usd,
        sum(if(is_midmonth_dday = 1 and is_flash = 0 and is_sbd = 0,gmv_usd)) as midmonth_dday_non_cfs_gmv_usd,
        sum(if(is_midmonth_dday = 1 and is_sbd = 0,item_amount)) as midmonth_dday_qty_sold,
        
        sum(if(is_sbd = 1,order_fraction)) as sbd_order,
        sum(if(is_sbd = 1,gmv_usd)) as sbd_gmv_usd,
        sum(if(is_sbd = 1,item_amount)) as sbd_qty_sold,
        sum(if(is_sbd = 0 and is_dday = 0 and is_flash = 1,order_fraction)) as bau_cfs_order,
        sum(if(is_sbd = 0 and is_dday = 0 and is_flash = 0,order_fraction)) as bau_non_cfs_order,
        sum(if(is_sbd = 0 and is_dday = 0 and is_flash = 1,gmv_usd)) as bau_cfs_gmv_usd,
        sum(if(is_sbd = 0 and is_dday = 0 and is_flash = 0,gmv_usd)) as bau_non_cfs_gmv_usd,
        sum(if(is_sbd = 0 and is_dday = 0 and is_flash = 0,item_amount)) as bau_qty_sold
    from order_mart
    group by 1,2
)

, final_with_order as ( --ok
    select
        all.shopid,
        all.seller_type,
        all.itemid,
        all.modelid,
        all.avg_offer_incl_cfs,
        all.avg_offer_excl_cfs,
        all.lowest_offer_incl_cfs,
        all.lowest_offer_excl_cfs,
        performance.dday_cfs_order as dday_cfs_order,
        performance.dday_non_cfs_order as dday_non_cfs_order,
        performance.dday_cfs_gmv_usd as dday_cfs_gmv_usd,
        performance.dday_non_cfs_gmv_usd as dday_non_cfs_gmv_usd,
        performance.dday_qty_sold as dday_qty_sold,
        
        performance.midmonth_dday_cfs_order as midmonth_dday_cfs_order,
        performance.midmonth_dday_non_cfs_order as midmonth_dday_non_cfs_order,
        performance.midmonth_dday_cfs_gmv_usd as midmonth_dday_cfs_gmv_usd,
        performance.midmonth_dday_non_cfs_gmv_usd as midmonth_dday_non_cfs_gmv_usd,
        performance.midmonth_dday_qty_sold as midmonth_dday_qty_sold,
        
        performance.sbd_order as sbd_order,
        performance.sbd_gmv_usd as sbd_gmv_usd,
        performance.sbd_qty_sold as sbd_qty_sold,
        performance.bau_cfs_order as bau_cfs_order,
        performance.bau_non_cfs_order as bau_non_cfs_order,
        performance.bau_cfs_gmv_usd as bau_cfs_gmv_usd,
        performance.bau_non_cfs_gmv_usd as bau_non_cfs_gmv_usd,
        performance.bau_qty_sold as bau_qty_sold
    from all
    left join performance on all.itemid = performance.itemids and all.modelid = performance.modelids
)

, final_no_order as ( --non sellable models
    select
        shop_id,
        seller_type,
        item_id,
        model_id,
        0 as avg_offer_incl_cfs,
        0 as avg_offer_excl_cfs,
        0 as lowest_offer_incl_cfs,
        0 as lowest_offer_excl_cfs, 
        0 as dday_cfs_order,
        0 as dday_non_cfs_order,
        0 as dday_cfs_gmv_usd,
        0 as dday_non_cfs_gmv_usd,
        0 as dday_qty_sold,
        
        0 as midmonth_dday_cfs_order,
        0 as midmonth_dday_non_cfs_order,
        0 as midmonth_dday_cfs_gmv_usd,
        0 as midmonth_dday_non_cfs_gmv_usd,
        0 as midmonth_dday_qty_sold,
        
        0 as sbd_order,
        0 as sbd_gmv_usd,
        0 as sbd_qty_sold,
        0 as bau_cfs_order,
        0 as bau_non_cfs_order,
        0 as bau_cfs_gmv_usd,
        0 as bau_non_cfs_gmv_usd,
        0 as bau_qty_sold
    from model_mart
    where concat(cast(item_id as varchar),cast(model_id as varchar)) not in (select distinct concat(cast(itemid as varchar),cast(modelid as varchar)) from final_with_order)
    and 
    if(false=cast(${param_filter_itemid} as boolean),true,item_id in ${param_itemid_list})
        and if(false=cast(${param_filter_shopid} as boolean),true,shop_id in ${param_shopid_list})
        
    and model_status = 1
    and if(${param_is_exclude_no_sale_sku} = 'exclude', model_id is null,true)
)

, final as 
( --ok
    select  
        *
    from 
        final_with_order
    union
    select 
        *
    from 
        final_no_order
)

, ms as (
    select distinct concat(cast(cast(trim(shopid) as bigint) as varchar), main_category) as to_filter
    from thbdbi_partner.managed_seller_pic
    where ingestion_timestamp in (select max(ingestion_timestamp) from thbdbi_partner.managed_seller_pic)
)

, seller_offer_item as (
    select
        item_id itemid,
        sum(order_fraction) as order_item_level,
        sum(gmv_usd) as gmv_usd_item_level,
        sum(order_fraction)/(select distinct num_day from cmi_input) as ado_item,  -- check
        sum(gmv)/(select distinct num_day from cmi_input) as adg_thb_item,  -- check
        sum(gmv_usd)/(select distinct num_day from cmi_input) as adg_usd_item  -- check
    from order_mart
    group by 1
)
    
, order_mart_no_excl as ( 
    select 
         t1.item_id as itemid
        , t1.model_id as modelid
        , sum(t1.order_fraction) total_order
        , sum(t1.gmv_usd) total_gmv_usd 
        , sum(t1.gmv) total_gmv_thb 
    from order_mart t1
    group by 1,2
)

, shop_filter as (
   select  distinct temp.shopid
        from    (
            select  shopid
                    ,sequence(try_cast(start_date as date), try_cast(end_date as date)) as date_array
            from    thbdbi_partner.dw_negative_reinforcement t1
            left join (select shopid shop_id, max(end_date) max_end_date from thbdbi_partner.dw_negative_reinforcement group by shopid) t2
                on t1.shopid = t2.shop_id and t1.end_date = t2.max_end_date
            where   try_cast(start_date as date) <> try_cast(end_date as date) and max_end_date is not null
        ) temp 
        cross join unnest (date_array) as t(date_exemp)
        where   date_exemp = current_date
    )

, shop_all as (
    select distinct shopid
    from thbdbi_partner.dw_negative_reinforcement
    where try_cast(start_date as date) = try_cast(end_date as date)
        and try_cast(start_date as date) = if(hour(current_time + interval '30' minute) between 0 and 11, current_date - interval '2' day, current_date - interval '1' day)
)

, final_negative as (
    select *
    from shop_filter
    union 
    select *
    from shop_all    
)

,semi_final as (
    select
        model_mart.username,
        model_mart.item_name,
        model_mart.model_name,
        final.shopid,
        final.seller_type,
        model_mart.main_category,
        model_mart.sub_category,
        final.itemid,
        final.modelid,
        coalesce(final.avg_offer_incl_cfs, 0.0) avg_offer_incl_cfs,
        coalesce(final.avg_offer_excl_cfs, 0.0) avg_offer_excl_cfs,
        coalesce(final.lowest_offer_incl_cfs, 0.0) lowest_offer_incl_cfs,
        coalesce(final.lowest_offer_excl_cfs, 0.0) lowest_offer_excl_cfs,
        coalesce(final.dday_cfs_order, 0) dday_cfs_order,
        coalesce(final.dday_non_cfs_order, 0) dday_non_cfs_order,
        coalesce(final.dday_cfs_gmv_usd, 0) dday_cfs_gmv_usd,
        coalesce(final.dday_non_cfs_gmv_usd, 0) dday_non_cfs_gmv_usd,
        coalesce(final.dday_qty_sold, 0) dday_qty_sold,
        coalesce(final.midmonth_dday_cfs_order, 0) midmonth_dday_cfs_order,
        coalesce(final.midmonth_dday_non_cfs_order, 0) midmonth_dday_non_cfs_order,
        coalesce(final.midmonth_dday_cfs_gmv_usd, 0) midmonth_dday_cfs_gmv_usd,
        coalesce(final.midmonth_dday_non_cfs_gmv_usd, 0) midmonth_dday_non_cfs_gmv_usd,
        coalesce(final.midmonth_dday_qty_sold, 0) midmonth_dday_qty_sold,
        coalesce(final.sbd_order, 0) sbd_order,
        coalesce(final.sbd_gmv_usd, 0) sbd_gmv_usd,
        coalesce(final.sbd_qty_sold, 0) sbd_qty_sold,
        coalesce(final.bau_cfs_order, 0) bau_cfs_order,
        coalesce(final.bau_non_cfs_order, 0) bau_non_cfs_order,
        coalesce(final.bau_cfs_gmv_usd, 0) bau_cfs_gmv_usd,
        coalesce(final.bau_non_cfs_gmv_usd, 0) bau_non_cfs_gmv_usd,
        coalesce(final.bau_qty_sold, 0) bau_qty_sold,
        model_mart.is_new_sku,
        model_mart.seller_offer_price as current_seller_offer_price,
        model_mart.item_stock,
        model_mart.model_stock,
        model_mart.item_status_kw as item_status,
        model_mart.model_status,
        if((concat(cast(final.shopid as varchar),model_mart.main_category) in (select to_filter from ms)) or (final.shopid in (select distinct shop_id from mp_user.dim_shop__th_s0_live where is_official_shop = 1 and status = 1 and grass_date = current_date - interval '1' day)),1,0) as is_os_or_ms,
        if(final.shopid in (select shopid from final_negative), 1, 0) as is_in_shop_negative,
        seller_offer_item.order_item_level,
        seller_offer_item.gmv_usd_item_level,
        seller_offer_item.ado_item,
        seller_offer_item.adg_thb_item,
        seller_offer_item.adg_usd_item,
        coalesce(order_mart_no_excl.total_order, 0) total_order,
        coalesce(order_mart_no_excl.total_gmv_usd, 0) total_gmv_usd,
        coalesce(order_mart_no_excl.total_gmv_thb, 0) total_gmv_thb, 
        model_mart.fe_main_category, 
        model_mart.fe_sub_category,
        model_mart.fe_l3_category,
        ${param_campaign_date} campaign_date
    from final final
    inner join model_mart on final.itemid = model_mart.item_id and final.modelid = model_mart.model_id
    left join seller_offer_item on final.itemid = seller_offer_item.itemid
    left join order_mart_no_excl on final.itemid = order_mart_no_excl.itemid and final.modelid = order_mart_no_excl.modelid
    left join email_list t1 on if(t1.category = 'Overall Platform', true, model_mart.main_category = t1.category)
    left join s_type on final.shopid = s_type.shop_id
    left join email_list t2 on case when s_type.seller_tier_lv2 like '%CB%' then 'CB' when s_type.seller_tier_lv2 like '%MT-%' then 'MT' else s_type.seller_tier_lv2 end = t2.category
    where coalesce(t1.category, t2.category) is not null
)

select * 
from semi_final 
where is_os_or_ms in ${param_is_os_or_ms_cat}
