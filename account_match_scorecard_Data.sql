-- Databricks notebook source
-- MAGIC %python
-- MAGIC FY24_CALC = spark.read.format("delta").load("/mnt/Fy24_2")
-- MAGIC FY24_CALC.createOrReplaceTempView("CALC_Fy24")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Importing cdes
-- MAGIC EDL_mountpoint_DAM = "/mnt/mountEDL_Insights_Garima"
-- MAGIC Corpus_Path_DAM = "/standardized/masterandreference/contactmanagement/"
-- MAGIC abfss_url_DAM = "abfss://cseo@cseoedlprodadlsg2wus201.dfs.core.windows.net"
-- MAGIC
-- MAGIC
-- MAGIC df = spark.read.load(EDL_mountpoint_DAM+'/mappings_dbo_vw_edlp_domaintoaccountmappings/v1/standard')
-- MAGIC df.createOrReplaceTempView("domainmappings")

-- COMMAND ----------

---Creating the ABM list for FY24 
create or replace table ABM_Targeted_FY24
with 
 CDES as 
 (
select A.MSSalesId as TPID,B.Country,Domain,
REGEXP_extract(trim('-' FROM trim(',' FROM trim('.' FROM replace(replace(replace(replace(replace(replace(replace(nullif(nullif(LOWER(B.Domain),''),'#N/A'),'|',''),' ',''),'https://',''),'http://',''),'www2.',''),'www.',''),'www','')))), '(.+?)(/|$)') Domain_cleaned,
B.CRMAccountId,AccountName from 
         MSX as A inner join domainmappings as B
         on upper(trim(A.AccountNumber)) = upper(trim(B.CRMAccountId))
         where MSSalesID is not null 
         and lower(trim(status)) = 'active'


 ),
 CDES_domains as 
 (
select A.*,B.Suppressed from CDES A left join mappings_dbo_vw_edlp_domainsuppressions B
on lower(trim(A.domain)) = lower(trim(B.Domain)) and lower(trim(A.country)) = lower(trim(B.country)) 
),

Output as
(
select MSSalesAccountID TPID,
MSSalesAccountName AccountName,
A.CRMAccountID CRMID,
CRMAccountName,
GPID,HQ_DS,
SegmentGroup,
Segment,
SubSegment,
A.Country CountryName,
Vertical,B.Domain,
REGEXP_extract(trim('-' FROM trim(',' FROM trim('.' FROM replace(replace(replace(replace(replace(replace(replace(nullif(nullif(LOWER(B.Domain),''),'#N/A'),'|',''),' ',''),'https://',''),'http://',''),'www2.',''),'www.',''),'www','')))), '(.+?)(/|$)') Domain_cleaned,
NULLIF(UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(A.MSSalesAccountName)), ' ',''),'.',''),',',''),';',''),"'",""),'CORPORATION',''),'CORP',''),'TECHNOLOGIES',''),'TECHNOLOGY',''),'TECH',''),'INC',''),'THE',''),'GROUP',''),'PVTLTD',''),'PRIVATELIMITED',''),'LTD',''),'LIMITED',''),'&CO',''),'&COMPANY',''),'-',''))),'') AccountName_cleaned
from CALC_Fy24 A left join 


(select * from CDES_domains where suppressed = false OR suppressed IS NULL ) B
on
A.mssalesaccountID = B.TPID and 
lower(trim(A.country)) = lower(trim(B.country))
where 
COALESCE(HQ_DS,'')<>'DS' and
MSSalesAccountID >=1 and MSSalesAccountID is not null  and upper(trim(MSSalesAccountID)) not in ('UNKNOWN',0,'') and
lower(trim(A.Country)) not in ('russia','belarus','ukraine') and 
lower(trim(subsegment)) not in ('major - education','sm&c education - smb','sm&c education - corporate') and 
lower(trim(vertical)) not in ('higher education','primary & secondary edu/k-12') and
lower(trim(parentingLevel)) != 'child' and 
lower(trim(isManagedAccount)) = 'true'
)

select distinct * from output

--)

-- COMMAND ----------

-- DBTITLE 1, 
select SalesAlignmentName, count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from account_match_report
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
GROUP BY 1

-- COMMAND ----------

select count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from account_match_report
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
-- GROUP BY 1

-- COMMAND ----------

select count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Account_match_report_2_V2
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')

-- COMMAND ----------

SELECT * FROM Account_match_report_Backup

-- COMMAND ----------

SELECT MAX(Activity_Date) FROM account_match_report

-- COMMAND ----------

select  count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Account_match_report_2_V2
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
-- GROUP BY 1 
 
-- actual
-- Completness
-- Accuracy
-- 1
-- 132794
-- 131036
-- 126130

-- COMMAND ----------

select  count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Account_match_report_2
where Activity_Date > to_date('2023-07-01') 

-- COMMAND ----------

select Segment_group, count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Account_match_report_2
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
GROUP BY 1

-- COMMAND ----------


With Priority_list as
( Select * ,
case when B.TPID is not null /*and lower(trim(B.TPID)) not in ('unknown','',0)*/ then 1 else 0 end as ABM_Domain_leadCountry_Flag,
case when c.TPID is not null /*and lower(trim(B.TPID)) not in ('unknown','',0)*/ then 1 else 0 end as ABM_lscompany_leadCountry_Flag

from Account_match_report_2_V2 A
left join ABM_Targeted_FY24 B
on
REGEXP_extract(trim('-' FROM trim(',' FROM trim('.' FROM replace(replace(replace(replace(replace(replace(replace(nullif(nullif(LOWER(A.emailDomain),''),'#N/A'),'|',''),' ',''),'https://',''),'http://',''),'www2.',''),'www.',''),'www','')))), '(.+?)(/|$)') = B.Domain_cleaned and
lower(trim(A.marketingCountry)) = lower(trim(B.CountryName))
left join ABM_Targeted_FY24 as C
on
NULLIF(UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(A.TPAccountName)), ' ',''),'.',''),',',''),';',''),"'",""),'CORPORATION',''),'CORP',''),'TECHNOLOGIES',''),'TECHNOLOGY',''),'TECH',''),'INC',''),'THE',''),'GROUP',''),'PVTLTD',''),'PRIVATELIMITED',''),'LTD',''),'LIMITED',''),'&CO',''),'&COMPANY',''),'-',''))),'')= c.AccountName_cleaned and
lower(trim(A.marketingCountry)) = lower(trim(c.CountryName))
)


,Base_1 as (
Select *, 
case
  when ABM_Domain_leadCountry_Flag = 1 or ABM_lscompany_leadCountry_Flag=1 then 'ABM_Organic'
  else 'Non_ABM' end as Attribute
from Priority_list

)



select Attribute, count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Base_1
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
GROUP BY 1

-- COMMAND ----------

select Segment_group, count(distinct(MarketingLeadID)) actual, count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from Account_match_report_2_V2
where Activity_Date between to_date('2023-07-01') and to_date('2023-07-31')
GROUP BY 1

-- COMMAND ----------

Select *, from marketingleadp0

-- COMMAND ----------

-- DBTITLE 1,Domain Country lead matching numbers 
-- MAGIC %sql
-- MAGIC with ABM as 
-- MAGIC (Select * from ABM_Targeted_FY24)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ,marketo as(
-- MAGIC select Upper(trim(EmailDomain)) as EmailDomain,Upper(trim(MarketingCountry))Country,AccountTPID,MarketingLeadId,LSCompany 
-- MAGIC from marketingleadp0
-- MAGIC where (to_date(createdAt) between to_date('2023-06-01') AND to_date('2023-06-30'))
-- MAGIC AND Deleted_Flag = 'No'
-- MAGIC )
-- MAGIC
-- MAGIC ,Marketo_ABMleads  AS 
-- MAGIC (
-- MAGIC Select A.*, 
-- MAGIC case 
-- MAGIC when B.TPID is not null then "ABM"
-- MAGIC when C.Domain_cleaned is not null then "ABM" 
-- MAGIC when D.AccountName_cleaned is not null then "ABM" else "NON ABM" end as ABM_flag,
-- MAGIC
-- MAGIC case when AccountTPID > 0 and AccountTPID is not null and upper(trim(AccountTPID)) not in ("NULL","UNKNOWN","") then 'Has TPID'  else 'No TPID' end as TPID_Flag
-- MAGIC
-- MAGIC from marketo A
-- MAGIC left join ABM B  -- TPID 
-- MAGIC on lower(TRIM(A.AccountTPID))=lower(TRIM(B.TPID))
-- MAGIC
-- MAGIC -- union 
-- MAGIC -- Select A.*, case when C.Domain_cleaned is not null then 1 else 0 end as ABM_flag 
-- MAGIC -- from marketo A
-- MAGIC left join ABM C --(Domain,country)
-- MAGIC on 
-- MAGIC REGEXP_extract(trim('-' FROM trim(',' FROM trim('.' FROM replace(replace(replace(replace(replace(replace(replace(nullif(nullif(LOWER(A.emailDomain),''),'#N/A'),'|',''),' ',''),'https://',''),'http://',''),'www2.',''),'www.',''),'www','')))), '(.+?)(/|$)') = C.Domain_cleaned and
-- MAGIC lower(trim(A.Country)) = lower(trim(C.CountryName))
-- MAGIC
-- MAGIC -- union
-- MAGIC -- Select A.*, case when D.AccountName_cleaned is not null then 1 else 0 end as ABM_flag 
-- MAGIC -- from marketo A
-- MAGIC left join ABM D -- Company country 
-- MAGIC on 
-- MAGIC NULLIF(UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(A.LSCompany)), ' ',''),'.',''),',',''),';',''),"'",""),'CORPORATION',''),'CORP',''),'TECHNOLOGIES',''),'TECHNOLOGY',''),'TECH',''),'INC',''),'THE',''),'GROUP',''),'PVTLTD',''),'PRIVATELIMITED',''),'LTD',''),'LIMITED',''),'&CO',''),'&COMPANY',''),'-',''))),'')= D.AccountName_cleaned and
-- MAGIC lower(trim(A.Country)) = lower(trim(D.CountryName))
-- MAGIC )
-- MAGIC
-- MAGIC ,Leads as(
-- MAGIC select EmailDomain, Country, TPID_Flag, ABM_flag,
-- MAGIC count(Distinct MarketingLeadId) as  Lead_Count
-- MAGIC from Marketo_ABMleads
-- MAGIC group by 1,2,3,4)
-- MAGIC
-- MAGIC Select case when Lead_count <2 then '0-1'
-- MAGIC             else '>=2' end as Lead_Bucket, TPID_Flag,ABM_flag,count(distinct concat(EmailDomain,Country) )Domain_Country_Count
-- MAGIC from Leads
-- MAGIC group by 1,2,3
-- MAGIC Order by 1,2,3
-- MAGIC
-- MAGIC
-- MAGIC -- select * from Marketo_ABMleads/

-- COMMAND ----------

Select *, Ls from marketingleadp0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #overall numbers

-- COMMAND ----------

select 
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from account_match_report
--where Acc_type = 'TAL'
where Activity_Date between to_date('2023-06-01') and to_date('2023-06-30')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Segment - Subsegment level breakdown

-- COMMAND ----------

select Segment_group,
--Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from account_match_report
where 
--Acc_type = 'TAL'
Activity_Date between to_date('2023-06-01') and to_date('2023-06-30')
group by 1

order by 1

-- COMMAND ----------

select Activity_Date,Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from account_match_report
where Activity_Date between to_date('2023-03-01') and to_date('2023-03-21')
group by Subsegment,Activity_Date

order by Subsegment

-- COMMAND ----------

-- select * from Account_match_report
-- where Activity_Date 

-- COMMAND ----------

select * from jt_jr_mapping

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('com.crealytics.spark.excel').option('header','true').option("dataAddress","'Combined'!A1").load('dbfs:/FileStore/shared_uploads/v-mosislam@microsoft.com/FY24Q1_Paid_Media_TAL_Iteration_1_2_5_12_23.xlsx')
-- MAGIC df.createOrReplaceTempView("fy24_Q1")

-- COMMAND ----------

select * from fy24_q1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC FY24_CALC = spark.read.format("delta").load("/mnt/Fy24_2")
-- MAGIC FY24_CALC.createOrReplaceTempView("CALC_Fy24")

-- COMMAND ----------

select distinct TPID,Segment,
SubSegment from CALC_Fy24

-- COMMAND ----------

select marketingsubsegment,count(distinct marketingleadID) from marketingleadp0
group by 1 order by 2 desc

-- COMMAND ----------

select DSA.SalesAlignmentName,count(distinct A.marketingleadID) from
webdemandgenactivity_new_prod A left join marketingleadp0 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on B.AccountTPID = C.TPID
left join fy24_q1 PML on
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.marketingCountry)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
group by 1 order by 2 desc 

-- COMMAND ----------

with 
marketingleadp0_1 as (
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0),
A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID
from
webdemandgenactivity_new_prod A left join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on UPPER(TRIM(B.AccountTPID)) = UPPER(TRIM(C.TPID))
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.MarketingCountry_1)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select 
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A 

-- COMMAND ----------

with 
marketingleadp0_1 as (
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0),
A as 
(select distinct MarketingLeadId
-- ,B.AccountTPID,DSA.SalesAlignmentName,
-- CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
-- when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
-- when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
-- else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
-- case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
-- case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID
from marketingleadp0_1 B
-- on A.MarketingLeadId = B.MarketingLeadId
-- LEFT JOIN SalesAlignment DSA
-- ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
-- left join CALC_Fy24 C
-- on UPPER(TRIM(B.AccountTPID)) = UPPER(TRIM(C.TPID))
-- left join fy24_q1 PML on 
-- B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.MarketingCountry_1)) = lower(trim(PML.Subsidiary))
-- where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
-- To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') 
WHERE DATE(CreatedAt) BETWEEN To_Date('2023-07-01') and To_Date('2023-07-31')
and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select 
count(distinct(MarketingLeadID)) actual
-- ,
-- count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
-- count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A 

-- COMMAND ----------

with 
marketingleadp0_1 as (
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0),
A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID
from
webdemandgenactivity_new_prod A left join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on UPPER(TRIM(B.AccountTPID)) = UPPER(TRIM(C.TPID))
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.MarketingCountry_1)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
-- To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') 
To_Date(B.CreatedAt) between To_Date('2023-07-01') and To_Date('2023-07-31') 
and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select 
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A 

-- COMMAND ----------

with 
marketingleadp0_1 as (
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0),
A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID
from
webdemandgenactivity_new_prod A INNER join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on UPPER(TRIM(B.AccountTPID)) = UPPER(TRIM(C.TPID))
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.MarketingCountry_1)) = lower(trim(PML.Subsidiary))
where  
DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select 
count(distinct(MarketingLeadID)) actual
-- ,
-- count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
-- count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A 

-- COMMAND ----------

with marketingleadp0_1 as 
(
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0)

,A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID

from
webdemandgenactivity_new_prod A left join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on B.AccountTPID = C.TPID
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.marketingCountry_1)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select Segment_group,
--Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A

group by 1

order by 1

-- COMMAND ----------

with marketingleadp0_1 as 
(
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0)
,A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID

from
webdemandgenactivity_new_prod A left join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on UPPER(TRIM(B.AccountTPID)) = UPPER(TRIM(C.TPID))
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.marketingCountry_1)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select Segment_group,
--Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A
group by 1
order by 1

-- COMMAND ----------

with A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID



from
webdemandgenactivity_new_prod A left join marketingleadp0 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on B.AccountTPID = C.TPID
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.marketingCountry)) = lower(trim(PML.Subsidiary))
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select Segment_group,
Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A

group by 1,2

order by 1

-- COMMAND ----------

select distinct Area from marketo

-- COMMAND ----------

select max(CreatedAt) from jt_jr_base_table

-- COMMAND ----------



-- COMMAND ----------

with marketingleadp0_1 as 
(
Select *, 
Case when Lower(Trim(Marketingcountry)) like "%hong kong%" then "hong kong"
when Lower(Trim(Marketingcountry)) like "%india" then "india sc"
when Lower(Trim(Marketingcountry)) like "türkiye" then "turkey"
when (Lower(Trim(Marketingcountry)) like "%trinidad%" and Lower(Trim(Marketingcountry))  like "%tobago%") then "trinidad & tobago"
else Lower(Trim(Marketingcountry)) end as MarketingCountry_1
from marketingleadp0)

,A as 
(select distinct A.MarketingLeadId,B.AccountTPID,DSA.SalesAlignmentName,
CASE WHEN (UPPER(TRIM(C.Subsegment)) like '%SM&C%' and UPPER(TRIM(C.Subsegment)) like '%SMB%') then 'SMB' 
when (UPPER(TRIM(c.subsegment)) like '%SM&C%' and UPPER(TRIM(c.Subsegment)) like '%CORPORATE%') then 'SMC Corporate'
when (UPPER(TRIM(C.Subsegment)) like '%STRATEGIC%' or UPPER(TRIM(C.Subsegment)) like '%MAJOR%') then 'Enterprise'
else 'Other Segments' end as Segment_group,C.segment,C.Subsegment,
case when B.AccountTPID is not null and lower(trim(B.AccountTPID)) not in ('unknown',0,'') then 1 else 0 end as quantity_Flag,
case when PML.TPID is not null and lower(trim(PML.TPID)) not in ('unknown',0,'') then 1 else 0 end as Quality_flag,B.accountTPID,emailDomain,marketingcountry,PML.subsidiary,PML.domain,PML.TPID

from
webdemandgenactivity_new_prod A left join marketingleadp0_1 B
on A.MarketingLeadId = B.MarketingLeadId
LEFT JOIN SalesAlignment DSA
ON  UPPER(TRIM(A.DIM_SalesAlignmentId)) =  UPPER(TRIM(DSA.DIM_SalesAlignmentId))
left join CALC_Fy24 C
on B.AccountTPID = C.TPID
left join fy24_q1 PML on 
B.AccountTPID = PML.TPID and lower(trim(B.emailDomain)) = lower(trim(PML.Domain)) and lower(trim(B.marketingCountry_1)) = lower(trim(PML.Subsidiary))
left join ABM_Targeted_FY24 B
on
REGEXP_extract(trim('-' FROM trim(',' FROM trim('.' FROM replace(replace(replace(replace(replace(replace(replace(nullif(nullif(LOWER(A.emailDomain),''),'#N/A'),'|',''),' ',''),'https://',''),'http://',''),'www2.',''),'www.',''),'www','')))), '(.+?)(/|$)') = B.Domain_cleaned and
lower(trim(A.marketingCountry)) = lower(trim(B.CountryName))
left join ABM_Targeted_FY24 as C
on
NULLIF(UPPER(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(A.lsCompany)), ' ',''),'.',''),',',''),';',''),"'",""),'CORPORATION',''),'CORP',''),'TECHNOLOGIES',''),'TECHNOLOGY',''),'TECH',''),'INC',''),'THE',''),'GROUP',''),'PVTLTD',''),'PRIVATELIMITED',''),'LTD',''),'LIMITED',''),'&CO',''),'&COMPANY',''),'-',''))),'')= c.AccountName_cleaned and
lower(trim(A.marketingCountry)) = lower(trim(c.CountryName)) 
where  DSA.SalesAlignmentName in ('GDC-MWD-TAL-SMC+ENT','GDC-MWD-TAL-ENT','GDC-MWD-TAL-SMC','GDC-MWD-TAL-SMB','Product-M-TAL-SMC+ENT') and 
To_Date(A.MLPA_ActivityDateTime) between To_Date('2023-07-01') and To_Date('2023-07-31') and B.Deleted_Flag = 'No'
and 
upper(trim(originalleadsource)) Rlike ('CONTENT SYNDICATION|FACEBOOK|LINKEDIN')
)
select Segment_group,
--Subsegment,
count(distinct(MarketingLeadID)) actual,
count(distinct( case when Quantity_Flag = 1 then marketingLeadID end)) Completness,
count(distinct( case when Quality_Flag = 1 then marketingLeadID end)) Accuracy
from A

group by 1

order by 1
