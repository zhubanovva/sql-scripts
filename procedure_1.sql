create or replace procedure P_BI_896_4V2
(
    file_to_load_username varchar2,
    E_START_DATE  DATE,
    E_END_DATE  DATE,
    Log_ID in out number,
    Rows_Count out number,
    Err_Code out number,
    Err_Msg out varchar2
    )


is
    XSTR  VARCHAR2(32000);
    i number;
    

begin
    Err_Code := 0;
    Err_Msg := null;
    Rows_Count := 0;
    i := rp_clob_delim_txt('file_to_load_username');
commit;


XSTR := 'MSISDN;USERNAME;BONUS;BRAND;DATE;';
         REPGATHER_4V2(LOG_ID, XSTR);
         IF  ISWCH2NEW THEN
    PKGARC_XLSX.ADD_TITLE(XSTR);
    PKGARC_XLSX.SET_RWSMAX();
    END IF;

   
if i = 0 then 
  raise_application_error(-20000, 'File file_to_load_username is empty!');
  end if;

if E_START_DATE is null
  then raise_application_error(-20001, 'Enter start_date');
  end if;

if E_END_DATE is null
  then raise_application_error(-20001, 'Enter end_date');
  end if;
  

------------------first brand--------------------------------------------------
if i != 0 then 
  for res in (
  select /*+parallel(ti,16) full(ti) parallel(pp,16) full(pp) parallel(a,16) full(a) parallel(n,16) full(n)*/
    ti.subs||';'||
    a.creator_id||';'||
    n.name_text||';'||
    'K'||';'||
    to_char(pp.start_date,'dd.mm.yyyy hh24:mi:ss') abcd
  
  from 
      owner1.Item@db_link ti,
      owner1.PRICE_PLAN@db_link pp,
      owner1.action@db_link a,
      owner2.NAME@db_link n,
      xtemp x
           
  where 1=1
    and ti.MAIN_IND = 1
    and ti.main_item_id = pp.main_item_id
    and (ti.order_action_id = a.order_unit_id(+)
         and a.order_unit_id(+) is not null
        )
    and pp.order_action_id = a.order_unit_id(+)
    and pp.item_def_id = n.cid(+)
    and pp.item_def_ver = n.pcversion_id(+)
    and ti.start_date between to_date(RP_BI_896_4V2.E_START_DATE,'dd.mm.yyyy') and to_date(RP_BI_896_4V2.E_END_DATE,'dd.mm.yyyy')+1-1/86400
    and pp.start_date between to_date(RP_BI_896_4V2.E_START_DATE,'dd.mm.yyyy') and to_date(RP_BI_896_4V2.E_END_DATE,'dd.mm.yyyy')+1-1/86400
    and a.ctdb_cre_datetime between to_date(RP_BI_896_4V2.E_START_DATE,'dd.mm.yyyy') and to_date(RP_BI_896_4V2.E_END_DATE,'dd.mm.yyyy')+1-1/86400
    and upper(a.creator_id) = upper(x.s1)
    and lower(n.name_text) in (
                          'service 1',
                          'service 2',
                          'service 3',
                          'service 4',
                          'service 5'
                          )
        
  group by ti.subs, a.creator_id, n.name_text,pp.start_date
   
  )
  loop
  REPGATHER_4V2(LOG_ID, RES.ABCD);
  ROWS_COUNT := COALESCE(ROWS_COUNT,0)+1;
  end loop;
  commit;
    

------------------second brand----------------------------------------------
for rec in (
  select /*+ parallel(p,16) full(p) parallel(s,16) full(s) parallel(m,16) full(m)*/
    m.subs_id||';'||
    s.username||';'||
    p.value||';'||
    m.brand||';'||
    to_char(s.eventtime,'dd.mm.yyyy hh24:mi:ss') abcd
      
  from  
        owner3.event_logs@db_link2 s,
        owner3.eventparams@db_link2 p,
        owner3.subs@db_link2 m,
        xtemp x
    
  where s.event_logs_id = p.event_logs_id
    and s.owner_id = m.subs_id
    and p.eventtime between to_date(RP_BI_896_4V2.E_START_DATE,'dd.mm.yyyy') and to_date(RP_BI_896_4V2.E_END_DATE,'dd.mm.yyyy')+1-1/86400
    and s.eventtime between to_date(RP_BI_896_4V2.E_START_DATE,'dd.mm.yyyy') and to_date(RP_BI_896_4V2.E_END_DATE,'dd.mm.yyyy')+1-1/86400
    and upper(s.username) = upper(x.s1)
    nd lower(p.value) like '%service%'
         
  order by s.eventtime
         
  )

loop          
REPGATHER_4V2(LOG_ID, REC.ABCD);
ROWS_COUNT := COALESCE(ROWS_COUNT,0)+1;
end loop;
commit;
REPFLUSH_4V2();
   

commit;
end if;


exception
 when others then
    Err_Msg := Substr(SqlErrM, 1, 300);
    Err_Code := SqlCode;
   Rollback;
end P_BI_896_4V2;
