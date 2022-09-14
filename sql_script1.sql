with ste as ( 
select /*+ parallel(8) */
b.pym_channel_id, b.FAN, b.DAN, b.EXTERNAL_ID, b.PAY_CATEGORY, b.PCN_STATUS, b.STATUS_DATE, b.CUSTOMER_ID,
s.SUBS_NO, s.SUB_STATUS, s.SUB_STATUS_DATE, s.CAL_PYM_CATEGORY,  
cd.EFFECTIVE_DATE, cd.EXPIRATION_DATE, cd.agreement_no,
case when sa.sdc = '12' then 'service 1' else null end as service,
nd.IIN_, nd.NAME_CUST_,
c.customer_type, ct.CUSTTP_DESC cate,
(
case when cd.agreement_no is not null 
     then row_number() over (partition by cd.agreement_no order by cd.expiration_date desc nulls first, cd.effective_date desc nulls last)
     end ) as rn
from 
owner1.pay_channel@db_link b,
owner1.charge_distr@db_link cd,
owner1.subs@db_link s,
owner1.agreement@db_link sa,
owner1.address@db_link nml,
owner1.name_data@db_link nd,
owner2.customer@db_link c,
owner2.customer_type@db_link ct
where 1=1
and b.pym_channel_id = cd.pcn(+)
and cd.agreement_no = s.subs_no(+)
and (s.subs_no = sa.agreement_no(+) and sa.sdc(+) = '12'
      and sa.expiration_date(+) is null
      and sa.sdc_status(+) = 'A'
      )
and (b.customer_id= nml.entity_id(+)
      and nml.type(+)= 'C'
      and nml.entity_type(+)= 'CUSTOMER'
      and nml.expiration_date(+) is null
      )
and nml.name_id= nd.name_id(+)
and b.customer_id = c.customer_id
and c.CUSTOMER_TYPE = ct.CUSTOMER_TYPE
and cd.sdc(+) in ('13','34','45','56')
and cd.level(+) = 'S'
)

select 
 *
 from
ste
where (rn  = 1 or rn is null)