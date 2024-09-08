
CREATE OR REPLACE FUNCTION postgres.final_build()
RETURNS void
LANGUAGE plpgsql
AS $function$
begin

/* CLEANUP LOGIC FOR TABLE postgres.raw_UID_ISO_FIPS_LookUp_Table
 * CREATE TABLE postgres.final_location_population
 * identified FIPS as id as it was identified to be unique
 * removing fields with no value (code3, Admin2)
 * added filter to remove FIPS = null
 * */
drop table if exists postgres.final_location_population;
create table postgres.final_location_population as 
(
	select
		lkp_tbl."FIPS" as id,
		lkp_tbl.iso2,
		lkp_tbl.iso3,
		lkp_tbl."Province_State",
		lkp_tbl."Country_Region",
		lkp_tbl."Lat",
		lkp_tbl."Long_",
		lkp_tbl."Combined_Key",
		lkp_tbl."Population"
	from
		postgres."raw_UID_ISO_FIPS_LookUp_Table" as lkp_tbl
	where 1=1
		and lkp_tbl."FIPS" is not null
)
;

/* CREATE DIMENSION TABLE final_dates
 * added field id as identifier for date
 * grouped by Date to make unique dimension
 * added filter to exclude rows without date
 * */
drop table if exists postgres.final_dates;
create table postgres.final_dates as 
(
	select
		TO_CHAR(date_trunc('month', dt."Date"::date)::date, 'yyyymm')::int as id,
		date_trunc('month', dt."Date"::date)::date as "Date"
	from
		postgres.raw_csse_covid_19_daily_reports_us as dt
	where 1=1
		and dt."Date" is not null
	group by 
		date_trunc('month', dt."Date"::date)
)
;

/* CREATE METRICS TABLE final_metrics
 * remove unecessary fields () due to now existing dimension tables (final_location_population, final_dates)
 * identified foreign keys: location_population_id and date_id
 * added deduplication logic partition by: location_population_id, date_id and order by: Last_Update
 * added filter to exclude rows without date
 * due to metric values are a running total, added deduplication logic partition by: location_population_id, DateMonth and order by: Date so that values is based on the end of each month
 * */
drop table if exists postgres.final_metrics;
create table postgres.final_metrics as 
(
	select 
		mt.location_population_id,
		mt.date_id,
		mt."Last_Update",
		mt."Confirmed",
		mt."Deaths",
		mt."Recovered",
		mt."Active",
		mt."Incident_Rate",
		mt."Total_Test_Results",
		mt."People_Hospitalized",
		mt."Case_Fatality_Ratio",
		mt."Testing_Rate",
		mt."Hospitalization_Rate",
		mt."People_Tested",
		mt."Mortality_Rate"
	from 
	(
		select 
			mt.location_population_id,
			mt.date_id,
			mt."DateMonth",
			mt."Date",
			mt."Last_Update",
			mt."Confirmed",
			mt."Deaths",
			mt."Recovered",
			mt."Active",
			mt."Incident_Rate",
			mt."Total_Test_Results",
			mt."People_Hospitalized",
			mt."Case_Fatality_Ratio",
			mt."Testing_Rate",
			mt."Hospitalization_Rate",
			mt."People_Tested",
			mt."Mortality_Rate",
			row_number() over(partition by mt.location_population_id, mt."DateMonth" order by mt."Date" desc) as row_num
		from 
		(	
			select
				mt."FIPS" as location_population_id,
				TO_CHAR(date_trunc('month', mt."Date"::date)::date, 'yyyymm')::int as date_id,
				date_trunc('month', mt."Date"::date)::date as "DateMonth",
				mt."Date"::date as "Date",
				mt."Last_Update",
				mt."Confirmed",
				mt."Deaths",
				mt."Recovered",
				mt."Active",
				mt."Incident_Rate",
				mt."Total_Test_Results",
				mt."People_Hospitalized",
				mt."Case_Fatality_Ratio",
				mt."Testing_Rate",
				mt."Hospitalization_Rate",
				mt."People_Tested",
				mt."Mortality_Rate",
				row_number() over(partition by mt."FIPS", TO_CHAR(mt."Date"::date, 'yyyymmdd')::int order by "Last_Update" desc) as row_num
			from
				postgres.raw_csse_covid_19_daily_reports_us as mt
			where 1=1
				and mt."Date" is not null
		) as mt
		where mt.row_num = 1
	) mt
	where 1=1
		and mt.row_num = 1
)
;

END;
$function$
;
