USE ifrs9_fsdb
GO

IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = N'p_ecl_stage_status_tod_ghb' AND schema_id = SCHEMA_ID('dbo'))
  BEGIN
    PRINT N'Creating procedure [dbo].[p_ecl_stage_status_tod_ghb] ...'
    EXEC (N'CREATE PROCEDURE [dbo].[p_ecl_stage_status_tod_ghb] AS RETURN(0)')
    WAITFOR DELAY N'00:00:00.003'
  END
PRINT N'Altering procedure [dbo].[p_ecl_stage_status_tod_ghb]...'
GO
ALTER PROCEDURE [dbo].[p_ecl_stage_status_tod_ghb] 
AS

declare @min_factor_date datetime = '20170430' -- oldest date for filtering dpd_eom only
declare @segmentation_date datetime = (select cob_date from t_entity ) -- to get initial segmentation data
declare @reporting_date datetime  = eomonth(@segmentation_date,-1)
declare @x_nontdr int = (select config_value from t_sys_configures where config_id = 7002) -- configuration of duration becoming status 'cure' (dpd 0 @x_nontdr month) currently at 12 months
declare @sql nvarchar(4000)

truncate table t_stage_status_ghb

-- prepare segmentation : only reporting_date at take-on date
IF OBJECT_ID('tempdb..#t_ecl_segmentation_ghb', 'U') IS NOT NULL 
  DROP TABLE #t_ecl_segmentation_ghb; --1672422

select *
	into #t_ecl_segmentation_ghb -- 1555590
	from t_ecl_segmentation_ghb
where reporting_date = @segmentation_date
and value_date <= @reporting_date
and segment_pool_id not in (2,3,4,6,7)

update #t_ecl_segmentation_ghb
	set reporting_date = @reporting_date
		,restructure_start_date = (case when restructure_start_date > @reporting_date then null else restructure_start_date end)

---------------------------------------------
----------- prepare dpd ---------------------
---------------------------------------------
-- cut dpd only for 23 points consideration --> 1 point be
IF OBJECT_ID('tempdb..#t_dpd_eom_ghb_original', 'U') IS NOT NULL 
  DROP TABLE #t_dpd_eom_ghb_original; 

select deal_id, factor_date, factor_value
	into #t_dpd_eom_ghb_original
	from t_dpd_eom_ghb
	where factor_date >= @min_factor_date and factor_date <= @reporting_date -- 35353038

-- fill NULL values in between with max(earliest available, latest available)
IF OBJECT_ID('tempdb..#t_dpd_eom_ghb', 'U') IS NOT NULL 
  DROP TABLE #t_dpd_eom_ghb; 

;with dpd_maxmindate as(
  	select dpd.deal_id, (case when @min_factor_date > seg.value_date then @min_factor_date else seg.value_date end) as min_factor_date
	from #t_dpd_eom_ghb_original dpd
	left join #t_ecl_segmentation_ghb seg
	on dpd.deal_id = seg.deal_tfi_id
	group by dpd.deal_id, seg.value_date --, seg.value_date
),running_date as(
	select distinct factor_date
	from #t_dpd_eom_ghb_original
),dpd_preparefactor as(
	select dpdmm.deal_id, run.factor_date
	from dpd_maxmindate dpdmm
	cross join running_date run
	where run.factor_date >= dpdmm.min_factor_date and run.factor_date <= @reporting_date
),dpd_fillwithna as(
	select dpdprep.deal_id, dpdprep.factor_date, dpd.factor_value
	from dpd_preparefactor dpdprep
	left join
	#t_dpd_eom_ghb_original dpd
	on dpdprep.deal_id = dpd.deal_id
	and dpdprep.factor_date = dpd.factor_date
),dpd_fillwithna_group as(
	 select
		dpd.deal_id
		,dpd.factor_date
		,dpd.factor_value
		,sum(case when dpd.factor_value is null then 0 else 1 end)
			over (partition by dpd.deal_id order by dpd.factor_date) as head_group
		,sum(case when dpd.factor_value is null then 0 else 1 end)
			over (partition by dpd.deal_id order by dpd.factor_date desc) as tail_group
	 from
	 dpd_fillwithna dpd
)
,dpd_filledvalue as(
	select dpd.deal_id
	,dpd.factor_date
	,dpd.factor_value
	,first_value(dpd.factor_value)
		over (partition by dpd.deal_id, dpd.head_group
		order by dpd.factor_date
		rows between unbounded preceding and current row) as head
	,first_value(dpd.factor_value)
		over (partition by dpd.deal_id, dpd.tail_group
		order by dpd.factor_date desc
		rows between unbounded preceding and current row) as tail
	from dpd_fillwithna_group dpd
)
select dpd.deal_id, dpd.factor_date, dpd.head,dpd.tail
	,iif(dpd.factor_value is null, iif(head is null, tail, iif(head >= isnull(tail,0),head,tail)), dpd.factor_value)  as factor_value
into #t_dpd_eom_ghb
from dpd_filledvalue dpd

--truncate table t_dpd_eom_ghb

insert into t_dpd_eom_ghb
(deal_id, entity, factor_type, factor_date, factor_value,last_modified,modified_by)
select dpd.deal_id
,'GHB' entity
,'DPD' factor_type
,dpd.factor_date
,dpd.factor_value
,getdate() as last_modified
,'p_ecl_stage_status_tod_ghb(impute)' modified_by
from #t_dpd_eom_ghb dpd
left join t_dpd_eom_ghb dpd_old
on 
dpd.deal_id = dpd_old.deal_id
and dpd.factor_date = dpd_old.factor_date
where dpd_old.deal_id is null

IF OBJECT_ID('tempdb..#dpd_x_nontdr', 'U') IS NOT NULL
  DROP TABLE #dpd_x_nontdr;

create table #dpd_x_nontdr (
	deal_id nvarchar(max),
	factor_date date,
	row_no int,
	max_x_nontdrm int
)

set @sql = 'select dpd.deal_id,dpd.factor_date,'
+ ' row_number() over (partition by dpd.deal_id order by dpd.factor_date) as row_no,'
+ 'max(dpd.factor_value) over(partition by dpd.deal_id order by dpd.factor_date rows between ' 
+ convert(varchar,@x_nontdr-1) 
+ ' preceding and current row) as max_x_nontdrm from #t_dpd_eom_ghb dpd '
+ ' join #t_ecl_segmentation_ghb seg on dpd.deal_id = seg.deal_tfi_id and seg.reporting_date = ''' 
+ convert(varchar,@reporting_date) 
+ ''' left join (select deal_id,max(factor_date) max_npl_date from t_dpd_eom_ghb where factor_value >90 and factor_date <= ''' 
+ convert(varchar,@reporting_date) 
+ ''' and factor_date >= ''' 
+ convert(varchar,@min_factor_date) 
+ ''' group by deal_id ) max_npl on max_npl.deal_id = dpd.deal_id  OPTION (RECOMPILE)'

insert into #dpd_x_nontdr
exec sp_executesql @sql

--declare @reporting_date date = '20190228'

IF OBJECT_ID('tempdb..#t_dpd_eom_stage_event_initial', 'U') IS NOT NULL
  DROP TABLE #t_dpd_eom_stage_event_initial;


select
	t.*
	,(case when t.max_dpd_12m = 0 and row_no >= 12 then 1 else 0 end) as dpd_12m_event -- not used, for the case of nontdr use dpd_x_nontdrm_event
	,(case when t.max_dpd_9m <= 90 and t.factor_value = 0 and row_no > 8 then 1 else 0 end) as dpd_9m_event -- when this event occurs -> we mark m_stg2 to count from here
	,(case when t.sum_dpd_3m = 0 and row_no >= 3 then 1 else 0 end) as dpd_3m_event
	,(case when t.factor_value >30 and t.factor_value <= 90 then 1 else 0 end) as dpd3090_event
	,(case when t.max_x_nontdrm = 0 and row_no >= @x_nontdr then 1 else 0 end) as dpd_x_nontdrm_event
	into #t_dpd_eom_stage_event_initial
from(
select 
	dpd.deal_id
	,max_npl.max_npl_date as npl_date
	,seg.restructure_start_date as restructure_start_date
	,dpd.factor_date
	,dpd.factor_value
	,row_number() over (partition by dpd.deal_id order by dpd.factor_date) as row_no
	,SUM(dpd.factor_value) OVER ( partition by dpd.deal_id order by dpd.factor_date 
												ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum_dpd_3m
	,MAX(dpd.factor_value) OVER ( partition by dpd.deal_id order by dpd.factor_date
												ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as max_dpd_9m
	,MAX(dpd.factor_value) OVER ( partition by dpd.deal_id order by dpd.factor_date
												ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as max_dpd_12m
	,nontdr.max_x_nontdrm
from
	#t_dpd_eom_ghb dpd
left join
	#t_ecl_segmentation_ghb seg
	on dpd.deal_id = seg.deal_tfi_id
	and seg.reporting_date = @reporting_date
left join
	(select deal_id,max(factor_date) max_npl_date
		from t_dpd_eom_ghb
		where factor_value >90
		and factor_date <= @reporting_date and factor_date >= @min_factor_date
		group by deal_id
	) max_npl
	on max_npl.deal_id = dpd.deal_id
left join
	#dpd_x_nontdr nontdr
	on dpd.deal_id = nontdr.deal_id
	and dpd.factor_date = nontdr.factor_date 
	) t

IF OBJECT_ID('tempdb..#has_npl', 'U') IS NOT NULL 
  DROP TABLE #has_npl; 
  
---- npl_date is not null only and restructure occurs after nonrestructure period
;with has_npl as(
	select seg.deal_tfi_id deal_id, seg.restructure_start_date as restructure_date, dpd.npl_date
	from
	#t_ecl_segmentation_ghb seg
	join #t_dpd_eom_stage_event_initial dpd
	on seg.deal_tfi_id = dpd.deal_id
	where dpd.npl_date is not null and dpd.factor_date >= @min_factor_date
	and seg.restructure_start_date is not null
	and seg.reporting_date = @reporting_date
	group by seg.deal_tfi_id, dpd.npl_date, seg.restructure_start_date
), check_tdr_zerobetween as(
	select has_npl.deal_id, has_npl.restructure_date, has_npl.npl_date, max(dpd.factor_date) zerobetween
	from
	has_npl
	join #t_dpd_eom_stage_event_initial dpd
	on has_npl.deal_id = dpd.deal_id
	where has_npl.npl_date < has_npl.restructure_date
	and datediff(month, has_npl.npl_date, dpd.factor_date) >= @x_nontdr
	and dpd.factor_date < has_npl.restructure_date
	and dpd.dpd_x_nontdrm_event = 1
	group by has_npl.deal_id, has_npl.restructure_date, has_npl.npl_date
)
	select has_npl.deal_id, has_npl.restructure_date
	,chk.zerobetween zerobetween_date
	,(case when chk.zerobetween is null and has_npl.npl_date < has_npl.restructure_date then eomonth(has_npl.restructure_date,-1)
		when chk.zerobetween is not null and has_npl.npl_date < has_npl.restructure_date then null
		else has_npl.npl_date end) npl_date
	into #has_npl
	from has_npl
	left join check_tdr_zerobetween chk
	on has_npl.deal_id = chk.deal_id

IF OBJECT_ID('tempdb..#t_dpd_eom_stage_event2', 'U') IS NOT NULL 
  DROP TABLE #t_dpd_eom_stage_event2; 

select *
into #t_dpd_eom_stage_event2
from #t_dpd_eom_stage_event_initial

update evt
	set evt.npl_date = has_npl.npl_date
	from #t_dpd_eom_stage_event2 evt
	join #has_npl has_npl
	on has_npl.deal_id = evt.deal_id
	where (has_npl.npl_date <> evt.npl_date) or (has_npl.npl_date is null and has_npl.deal_id is not null)

IF OBJECT_ID('tempdb..#restructure_transfered_from_npl_to_non_npl', 'U') IS NOT NULL 
  DROP TABLE #restructure_transfered_from_npl_to_non_npl; 

select *
	into #restructure_transfered_from_npl_to_non_npl
	from #has_npl has_npl -- 16002
	where (has_npl.npl_date is null and has_npl.deal_id is not null)


IF OBJECT_ID('tempdb..#t_dpd_eom_stage_event_final', 'U') IS NOT NULL 
  DROP TABLE #t_dpd_eom_stage_event_final; 

select
	t.deal_id
	,t.npl_date
	,t.restructure_start_date
	,t.factor_date
	,t.factor_value
	,(case when t.max_dpd_12m =0 and row_no >= 12 then 1 else 0 end) as dpd_12m_event
	,(case when t.max_dpd_9m <= 90 and t.factor_value = 0 and row_no > 8 then 1 else 0 end) as dpd_9m_event -- when this event occurs -> we mark m_stg2 to count from here
	,(case when t.sum_dpd_3m = 0 and row_no >2 then 1 else 0 end) as dpd_3m_event
	,(case when t.factor_value >30 and t.factor_value <= 90 then 1 else 0 end) as dpd3090_event
	,(case when t.max_x_nontdrm = 0 and row_no >= @x_nontdr then 1 else 0 end) as dpd_x_nontdrm_event
	into #t_dpd_eom_stage_event_final
from #t_dpd_eom_stage_event2 t

IF OBJECT_ID('tempdb..#t_stage_npl_tdr', 'U') IS NOT NULL 
  DROP TABLE #t_stage_npl_tdr; 

;with zero_3m as(
	select has_npl.deal_id, has_npl.npl_date,has_npl.restructure_date, min(dpd.factor_date) [0_3m_start_date], max(dpd.factor_date) [0_3m_end_date]
	from #has_npl has_npl
	left join #t_dpd_eom_stage_event_final dpd
	on has_npl.deal_id = dpd.deal_id
	and dpd.dpd_3m_event = 1
	and datediff(month,dpd.npl_date,dpd.factor_date) >= 3 -- npl_date|--- 3 months ---->|000
	where has_npl.npl_date is not null
	group by has_npl.deal_id, has_npl.npl_date, has_npl.restructure_date
),zero_9m as(
	select zero_3m.deal_id, zero_3m.npl_date, zero_3m.restructure_date, zero_3m.[0_3m_start_date], zero_3m.[0_3m_end_date], min(factor_date) [9m_after_0_3m]
	from
	zero_3m
	left join #t_dpd_eom_stage_event_final dpd
	on zero_3m.deal_id = dpd.deal_id
	and dpd.dpd_9m_event = 1
	and datediff(month, zero_3m.[0_3m_start_date], dpd.factor_date) >= 9 -- 000|<-xxxxxxxx more than or equal to 8 months with dpd less than 90---->|0 
	group by zero_3m.deal_id, zero_3m.npl_date, zero_3m.restructure_date, zero_3m.[0_3m_start_date], zero_3m.[0_3m_end_date]
),two_after_zero_9m as(
	select zero_9m.deal_id, zero_9m.npl_date, zero_9m.restructure_date, zero_9m.[0_3m_start_date], zero_9m.[0_3m_end_date], zero_9m.[9m_after_0_3m], min(dpd.factor_date) [fst_2_after_zero_9m], max(dpd.factor_date) [last_2_after_zero_9m]
	from
	zero_9m
	left join #t_dpd_eom_stage_event_final dpd
	on zero_9m.deal_id = dpd.deal_id
	and dpd.dpd3090_event = 1
	and dpd.factor_date > zero_9m.[9m_after_0_3m] -- fst_2_after_zero_9m, last_2_after_zero_9m > 0_3m_start_date
	group by zero_9m.deal_id, zero_9m.npl_date, zero_9m.restructure_date, zero_9m.[0_3m_start_date], zero_9m.[0_3m_end_date], zero_9m.[9m_after_0_3m]
),zero_before_last_2 as(
	-- this is to check m_stg2 only
	select two_z9.deal_id, two_z9.npl_date, two_z9.restructure_date, two_z9.[0_3m_start_date], two_z9.[0_3m_end_date], two_z9.[9m_after_0_3m], two_z9.[fst_2_after_zero_9m], two_z9.[last_2_after_zero_9m], max(dpd.factor_date) [0_before_last_2]
	from
	two_after_zero_9m two_z9
	left join #t_dpd_eom_stage_event_final dpd
	on two_z9.deal_id = dpd.deal_id
	and dpd.dpd_3m_event = 1
	and dpd.factor_date > two_z9.[fst_2_after_zero_9m] and dpd.factor_date < two_z9.[last_2_after_zero_9m] -- use 2 > 000 < 2 is sufficient
	group by two_z9.deal_id, two_z9.npl_date, two_z9.restructure_date, two_z9.[0_3m_start_date], two_z9.[0_3m_end_date], two_z9.[9m_after_0_3m],two_z9.[fst_2_after_zero_9m], two_z9.[last_2_after_zero_9m]
),two_after_zero_before_last_2 as(
	select zero_bf.deal_id, zero_bf.npl_date, zero_bf.restructure_date, zero_bf.[0_3m_start_date], zero_bf.[0_3m_end_date], zero_bf.[9m_after_0_3m], zero_bf.[fst_2_after_zero_9m], zero_bf.[last_2_after_zero_9m], zero_bf.[0_before_last_2], min(dpd.factor_date) [2_af_zero_bf_last2]
	from
	zero_before_last_2 zero_bf
	left join #t_dpd_eom_stage_event_final dpd
	on zero_bf.deal_id = dpd.deal_id
	and dpd.dpd3090_event = 1
	and dpd.factor_date > zero_bf.[0_before_last_2] and dpd.factor_date < zero_bf.[last_2_after_zero_9m] -- 000 >|..2..|< 2 m_stg2 
	group by zero_bf.deal_id, zero_bf.npl_date, zero_bf.restructure_date, zero_bf.[0_3m_start_date], zero_bf.[0_3m_end_date], zero_bf.[9m_after_0_3m], zero_bf.[fst_2_after_zero_9m], zero_bf.[last_2_after_zero_9m], zero_bf.[0_before_last_2]
)
select 
two_after_zero_before_last_2.*
,(case when [0_3m_start_date] is null then 1 -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c1' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then 2 -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then 3 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then 4 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then 5 -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then 6
	else null end) as condition_no -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
,(case when [0_3m_start_date] is null then 'c3' -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c1' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then 'c2' -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then 'c1' -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then 'c1' -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then 'c2'-- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then 'c2'
	else null end) as prev_status -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
,(case when [0_3m_start_date] is null then 3 -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c1' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then 2 -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then 1 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then 1 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then 2 -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then 2 -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
	else null end) as calculated_stage 
,(case when [0_3m_start_date] is null then 'Y' -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c1' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then 'Y' -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then 'N' -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then 'N' -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then 'N' -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then 'N' -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
		else null end) as stg3_flag
,(case when [0_3m_start_date] is null then 'c3-c2-c1' -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c3' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then 'c3-c2-c1' -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then NULL -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then NULL -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then 'c2-c1' -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then 'c2-c1' -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
		else null end) as path_tdr
,(case when [0_3m_start_date] is null then 0 -- stage 3 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c3' m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is null) then datediff(month,[0_3m_start_date],@reporting_date) -- stage 2 stg3_flag = 'Y' path_tdr = 'c3-c2-c1' prev_status = 'c2' m_stg2 = datediff(month,[0_3m_start_date],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is null) then 0 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] > [last_2_after_zero_9m]) then 0 -- stage 1 stg3_flag = 'N' path_tdr = NULL m_stg2 = 0
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [fst_2_after_zero_9m]) then datediff(month,[fst_2_after_zero_9m],@reporting_date) -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[fst_2_after_zero_9m],@reporting_date)
	when ([0_3m_start_date] is not null and [9m_after_0_3m] is not null) -- cure from 3 to 1
		and ([fst_2_after_zero_9m] is not null) -- turn worse to stage 2 after cure
		and ([0_3m_end_date] < [last_2_after_zero_9m]) 
		and ([0_before_last_2] is not null) then datediff(month,[2_af_zero_bf_last2],@reporting_date) -- stage 2 stg3_flag = 'N' path_tdr = 'c2-c1' m_stg2 = datediff(month,[2_af_zero_bf_last2],@reporting_date)
		else null end) as m_stg2
,null as cure_flag
into #t_stage_npl_tdr
from two_after_zero_before_last_2

-----------------------------------------------------------------------------------------------------------------------------
----------------------------------------- tdr no npl case -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#consecutive_stg2', 'U') IS NOT NULL 
  DROP TABLE #consecutive_stg2_before_tdr;

;with consecutive_stg2_bftdr as(
	select deal_id
		,factor_date
		,dpd3090_event
		, (select count(*) 
   FROM #t_dpd_eom_stage_event_final dpdg
   WHERE dpdg.dpd3090_event <> dpdgr.dpd3090_event 
   AND dpdg.deal_id = dpdgr.deal_id
   AND dpdg.factor_date <= dpdgr.factor_date) as run_group
	from #t_dpd_eom_stage_event_final dpdgr
), consecutive_stg2_bftdr_2 as(
	SELECT deal_id, 
		min(factor_date) as StartDate, 
		max(factor_date) as EndDate, 
		count(*) as streak
	FROM consecutive_stg2_bftdr
	WHERE dpd3090_event = 1
	GROUP BY deal_id, dpd3090_event, run_group
)
select *
into #consecutive_stg2_before_tdr
from consecutive_stg2_bftdr_2


IF OBJECT_ID('tempdb..#not_npl_tdr', 'U') IS NOT NULL 
  DROP TABLE #not_npl_tdr;

---- npl_date is not null only and restructure only
;with tdr_no_npl as(
	select seg.deal_tfi_id deal_id
	from
	#t_ecl_segmentation_ghb seg
	left join #t_dpd_eom_stage_event_final dpd
	on seg.deal_tfi_id = dpd.deal_id
	where dpd.npl_date is null
	and seg.reporting_date = @reporting_date
	and seg.restructure_start_date is not null
	union
	select deal_id
	from #restructure_transfered_from_npl_to_non_npl
), tdr_no_npl2 as(
	select tdr_no_npl.deal_id, seg.restructure_start_date
	from
	tdr_no_npl
	left join #t_ecl_segmentation_ghb seg
	on seg.deal_tfi_id = tdr_no_npl.deal_id
), stage_before_restructure as(
	select tdr.deal_id
	,tdr.restructure_start_date
	,(case when tdr.restructure_start_date < @min_factor_date then null 
			when dpd.factor_date > 30 then 2
		else 1 end) as stage_before_restructure
	,iif((case when tdr.restructure_start_date < @min_factor_date then null 
			when dpd.factor_date > 30 then 2
		else 1 end) = 2,consec.StartDate,null) as last2_before_restructure_date
	from
	tdr_no_npl2 tdr
	left join #t_dpd_eom_stage_event_final dpd
	on tdr.deal_id = dpd.deal_id
	and dpd.factor_date = eomonth(tdr.restructure_start_date,-1)
	left join #consecutive_stg2_before_tdr consec
	on tdr.deal_id = consec.deal_id
	and eomonth(tdr.restructure_start_date,-1) between consec.StartDate and consec.EndDate
), two as(
	select tdr.deal_id, tdr.restructure_start_date, tdr.last2_before_restructure_date, (case when tdr.last2_before_restructure_date is not null then tdr.last2_before_restructure_date else min(dpd.factor_date) end) [fst_2], max(dpd.factor_date) [last_2]
	from
	stage_before_restructure tdr
	left join #t_dpd_eom_stage_event_final dpd
	on tdr.deal_id = dpd.deal_id
	and dpd.dpd3090_event = 1
	and dpd.factor_date >= tdr.restructure_start_date
	group by tdr.deal_id, tdr.restructure_start_date, tdr.stage_before_restructure, tdr.last2_before_restructure_date
),zero as(
	select two.deal_id, two.restructure_start_date, two.[fst_2], two.[last_2], min(dpd.factor_date) [fst_0_3m], max(dpd.factor_date) [last_0_3m]
	from
	two
	left join #t_dpd_eom_stage_event_final dpd
	on two.deal_id = dpd.deal_id
	and dpd.dpd_3m_event = 1
	and dpd.factor_date >= two.restructure_start_date
	group by two.deal_id, two.restructure_start_date, two.[fst_2], two.[last_2]
),zero_before_last_2 as(
	select zero.deal_id, zero.restructure_start_date, zero.[fst_2], zero.[last_2], zero.[fst_0_3m], zero.[last_0_3m], max(dpd.factor_date) [0_bf_last_2]
	from
	zero
	left join #t_dpd_eom_stage_event_final dpd
	on zero.deal_id = dpd.deal_id
	and dpd.dpd_3m_event = 1
	and dpd.factor_date < zero.last_2
	and dpd.factor_date >= zero.restructure_start_date
	group by zero.deal_id, zero.restructure_start_date, zero.[fst_2], zero.[last_2], zero.[fst_0_3m], zero.[last_0_3m]
),first2_after_zero_before_last_2 as (
	select zero_before_last_2.deal_id, zero_before_last_2.restructure_start_date, zero_before_last_2.[fst_2], zero_before_last_2.[last_2], zero_before_last_2.[fst_0_3m], zero_before_last_2.[last_0_3m], zero_before_last_2.[0_bf_last_2], min(dpd.factor_date) [fst2_af_0_bf_last_2]
	from
	zero_before_last_2
	left join #t_dpd_eom_stage_event_final dpd
	on zero_before_last_2.deal_id = dpd.deal_id
	and dpd.dpd3090_event = 1
	and dpd.factor_date > zero_before_last_2.[0_bf_last_2]
	and dpd.factor_date < [last_2]
	and dpd.factor_date >= zero_before_last_2.restructure_start_date
	group by zero_before_last_2.deal_id, zero_before_last_2.restructure_start_date, zero_before_last_2.[fst_2], zero_before_last_2.[last_2], zero_before_last_2.[fst_0_3m], zero_before_last_2.[last_0_3m], zero_before_last_2.[0_bf_last_2]
)  
select	*
	,(case when [fst_2] is null then 7
		when [fst_2] is not null and [last_0_3m] is null then 8
		when [fst_2] is not null and [last_0_3m] > [last_2] then 9
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then 10
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then 11
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then 12
		else null end) as condition_no
	,(case when [fst_2] is null then 'c1'
		when [fst_2] is not null and [last_0_3m] is null then 'c2'
		when [fst_2] is not null and [last_0_3m] > [last_2] then 'c1'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then 'c2'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then 'c2'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then 'c2'
		else 'c1' end) as prev_status
	,(case when [fst_2] is null then 1
		when [fst_2] is not null and [last_0_3m] is null then 2
		when [fst_2] is not null and [last_0_3m] > [last_2] then 1
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then 2
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then 2
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then 2		
		else 1 end) as calculated_stage
	,(case when [fst_2] is null then 'N'
		when [fst_2] is not null and [last_0_3m] is null then 'N'
		when [fst_2] is not null and [last_0_3m] > [last_2] then 'N'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then 'N'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then 'N'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then 'N'
		else 'N' end) as stg3_flag
	,(case when [fst_2] is null then NULL
		when [fst_2] is not null and [last_0_3m] is null then 'c2-c1'
		when [fst_2] is not null and [last_0_3m] > [last_2] then NULL
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then 'c2-c1'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then 'c2-c1'
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then 'c2-c1'
		else 'c2-c1' end) as path_tdr
	,(case when [fst_2] is null then 0
		when [fst_2] is not null and [last_0_3m] is null then datediff(month,[fst_2],@reporting_date)
		when [fst_2] is not null and [last_0_3m] > [last_2] then 0
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is null then datediff(month,[last_2],@reporting_date)
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is null then datediff(month,[last_2],@reporting_date)
		when [fst_2] is not null and [last_0_3m] < [last_2] and [0_bf_last_2] is not null and [fst2_af_0_bf_last_2] is not null then datediff(month,[fst2_af_0_bf_last_2],@reporting_date)
		else 0 end) as m_stg2
	,null as cure_flag
into #not_npl_tdr
from
first2_after_zero_before_last_2

update #not_npl_tdr
set cure_flag = 1
where deal_id IN (select deal_id from #restructure_transfered_from_npl_to_non_npl) -- cure before restructuring

-----------------------------------------------------------------------------------------------------------------------------
----------------------------------------- non-tdr case ----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#consecutive_stg2', 'U') IS NOT NULL 
  DROP TABLE #consecutive_stg2;

;with consecutive_stg2 as(
	select deal_id
		,factor_date
		,dpd3090_event
		, (select count(*) 
   FROM #t_dpd_eom_stage_event_final dpdg
   WHERE dpdg.dpd3090_event <> dpdgr.dpd3090_event 
   AND dpdg.deal_id = dpdgr.deal_id
   AND dpdg.factor_date <= dpdgr.factor_date) as run_group
	from #t_dpd_eom_stage_event_final dpdgr
), consecutive_stg2_2 as(
	SELECT deal_id, 
		min(factor_date) as StartDate, 
		max(factor_date) as EndDate, 
		count(*) as streak
	FROM consecutive_stg2
	WHERE dpd3090_event = 1
	GROUP BY deal_id, dpd3090_event, run_group
)
select *
into #consecutive_stg2
from consecutive_stg2_2
where EndDate = @reporting_date


IF OBJECT_ID('tempdb..#x_tdrm_after_npl', 'U') IS NOT NULL 
  DROP TABLE #x_tdrm_after_npl;

;with not_tdr as(
	select seg.deal_tfi_id deal_id,dpd.npl_date, dpd.factor_value dpd_t
	from
	#t_ecl_segmentation_ghb seg
	left join #t_dpd_eom_stage_event_final dpd
	on seg.deal_tfi_id = dpd.deal_id
	where seg.reporting_date = @reporting_date
	and seg.restructure_start_date is null
	and dpd.factor_date = @reporting_date
	group by seg.deal_tfi_id, dpd.npl_date, dpd.factor_value
), x_tdrm_after_npl as(
	select not_tdr.deal_id, not_tdr.npl_date, not_tdr.dpd_t, max(dpd.factor_date) last_cure_date
	from not_tdr
	left join #t_dpd_eom_stage_event_final dpd
	on not_tdr.deal_id = dpd.deal_id
	and dpd.dpd_x_nontdrm_event = 1
	and datediff(month, not_tdr.npl_date, dpd.factor_date) >= @x_nontdr
	group by not_tdr.deal_id, not_tdr.npl_date, not_tdr.dpd_t
)
select
	x_tdrm.*
	,(case when x_tdrm.npl_date is null and x_tdrm.dpd_t <= 30 then 12
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 13
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 90 then 14
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is null then 15
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t <= 30 then 16
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 17
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 90 then 18
		else null end) as condition_no
	,NULL as prev_status
	,(case when npl_date is null and x_tdrm.dpd_t <= 30 then 1
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 2
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 90 then 3
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is null then 3
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t <= 30 then 1
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 2
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 90 then 3
		else 1 end) as calculated_stage
	,(case when x_tdrm.npl_date is null and x_tdrm.dpd_t <= 30 then 'N'
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 'N'
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 90 then 'Y'
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is null then 'Y'
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t <= 30 then 'N'
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 30 and x_tdrm.dpd_t <= 90 then 'N'
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and x_tdrm.dpd_t > 90 then 'Y'
		else 'N' end) as stg3_flag
	,NULL as path_tdr
	,(case when x_tdrm.npl_date is null and x_tdrm.dpd_t <= 30 then 0
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 30 and dpd_t <= 90 then con_stg2.streak - 1
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 90 then 0
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is null then 0
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t <= 30 then 0
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t > 30 and dpd_t <= 90 then con_stg2.streak - 1
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t > 90 then 0
		else 0 end) as m_stg2
	,(case when x_tdrm.npl_date is null and x_tdrm.dpd_t <= 30 then null
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 30 and dpd_t <= 90 then null
		when x_tdrm.npl_date is null and x_tdrm.dpd_t > 90 then null
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is null then null
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t <= 30 then 1
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t > 30 and dpd_t <= 90 then 1
		when x_tdrm.npl_date is not null and x_tdrm.last_cure_date is not null and dpd_t > 90 then 1
		else null end) as cure_flag	
into #x_tdrm_after_npl
from x_tdrm_after_npl x_tdrm
left join #consecutive_stg2 con_stg2
on x_tdrm.deal_id = con_stg2.deal_id



----------------------------------------------------------------------
----------------------------- insert into table ----------------------
----------------------------------------------------------------------
truncate table t_stage_status_ghb

insert into t_stage_status_ghb
select deal_id, prev_status, stg3_flag, path_tdr, (case when calculated_stage = 2 then m_stg2 + 1 else 0 end) as m_stg2, 
	cure_flag,
	getdate() as last_modified,
	cast('p_ecl_stage_status_tod_ghb' as nvarchar(100)) as modified_by
from
#t_stage_npl_tdr -- npl (ever)restructure

insert into
t_stage_status_ghb
select deal_id, prev_status, stg3_flag, path_tdr, (case when calculated_stage = 2 then m_stg2 + 1 else 0 end) as m_stg2, 
	cure_flag,
	getdate() as last_modified, 
	cast('p_ecl_stage_status_tod_ghb' as nvarchar(100)) as modified_by
from
#not_npl_tdr -- non-npl (ever)restructure

insert into
t_stage_status_ghb
select deal_id, prev_status, stg3_flag, path_tdr,(case when calculated_stage = 2 then m_stg2 + 1 else 0 end) as m_stg2, 
	cure_flag,
	getdate() as last_modified, 
	cast('p_ecl_stage_status_tod_ghb' as nvarchar(100)) as modified_by
from
#x_tdrm_after_npl -- no restructure

GO
IF EXISTS (SELECT * FROM sys.procedures WHERE name = N'p_ecl_stage_status_tod_ghb' AND modify_date > create_date AND modify_date > DATEADD(s, -1, CURRENT_TIMESTAMP) AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    PRINT N'Procedure [dbo].[p_ecl_stage_status_tod_ghb] has been altered...'
END ELSE BEGIN
    PRINT N'Procedure [dbo].[p_ecl_stage_status_tod_ghb] has NOT been altered due to errors!'
END
GO



