
final_build = """
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
"""

analysis_1 = """
-- looks for common values and number of times they appeared
with value_frequency as 
(
	select 
		fm."Deaths" as "Value",
		count(fm."Deaths") as "Frequency"
	from 
	(
		-- showing the star schema relation of final tables
		select
			fm.date_id,
			fd."Date",
			fm.location_population_id,
			flp.iso2,
			flp.iso3,
			flp."Province_State",
			flp."Country_Region",
			flp."Lat",
			flp."Long_",
			flp."Combined_Key",
			flp."Population",
			fm."Last_Update",
			fm."Confirmed",
			fm."Deaths",
			fm."Recovered",
			fm."Active",
			fm."Incident_Rate",
			fm."Total_Test_Results",
			fm."People_Hospitalized",
			fm."Case_Fatality_Ratio",
			fm."Testing_Rate",
			fm."Hospitalization_Rate",
			fm."People_Tested",
			fm."Mortality_Rate"
		from
			postgres.final_metrics fm
		join 
			postgres.final_dates fd
			on fm.date_id = fd.id 
		join 
			postgres.final_location_population flp 
			on fm.location_population_id = flp.id 
	) fm
	group by 
		fm."Deaths"
	order by 
		count(fm."Deaths") desc
),
-- due to having multiple confirmed value with same frequency
-- ranking based on frequency so that same frequency would be on the same ranking
frequency_rank as 
(
	select 
		cf."Frequency",
		rank() over(order by cf."Frequency" desc) as "Rank"
	from 
		value_frequency cf
	group by 
		cf."Frequency"
)

-- final output
select 
	fr."Rank",
	cf."Value",
	cf."Frequency"
from 
	value_frequency cf
join
	frequency_rank fr
	on cf."Frequency" = fr."Frequency"
where 1=1
	and fr."Rank" <= 5
"""

analysis_2 = """
-- aggregate metric value by month to identify change in value over time
with value_frequency as 
(
	select 
		fm."Date",
		round(avg(fm."Case_Fatality_Ratio"::decimal),2) as "Value"
	from 
	(
		-- showing the star schema relation of final tables
		select
			fm.date_id,
			fd."Date",
			fm.location_population_id,
			flp.iso2,
			flp.iso3,
			flp."Province_State",
			flp."Country_Region",
			flp."Lat",
			flp."Long_",
			flp."Combined_Key",
			flp."Population",
			fm."Last_Update",
			fm."Confirmed",
			fm."Deaths",
			fm."Recovered",
			fm."Active",
			fm."Incident_Rate",
			fm."Total_Test_Results",
			fm."People_Hospitalized",
			fm."Case_Fatality_Ratio",
			fm."Testing_Rate",
			fm."Hospitalization_Rate",
			fm."People_Tested",
			fm."Mortality_Rate"
		from
			postgres.final_metrics fm
		join 
			postgres.final_dates fd
			on fm.date_id = fd.id 
		join 
			postgres.final_location_population flp 
			on fm.location_population_id = flp.id 
	) fm
	group by 
		fm."Date"
	order by 
		fm."Date"
)

-- final output
select 
	vf."Date"::text as "Date",
	vf."Value"::float as "Value"
from value_frequency vf
"""

analysis_3a = """
-- aggregate metric value by month to identify change in value over time
with value_frequency as 
(
	select 
		fm."Date",
		sum(fm."Confirmed") as "Value A",
		avg(fm."Deaths") as "Value B"
	from 
	(
		-- showing the star schema relation of final tables
		select
			fm.date_id,
			fd."Date",
			fm.location_population_id,
			flp.iso2,
			flp.iso3,
			flp."Province_State",
			flp."Country_Region",
			flp."Lat",
			flp."Long_",
			flp."Combined_Key",
			flp."Population",
			fm."Last_Update",
			fm."Confirmed",
			fm."Deaths",
			fm."Recovered",
			fm."Active",
			fm."Incident_Rate",
			fm."Total_Test_Results",
			fm."People_Hospitalized",
			fm."Case_Fatality_Ratio",
			fm."Testing_Rate",
			fm."Hospitalization_Rate",
			fm."People_Tested",
			fm."Mortality_Rate"
		from
			postgres.final_metrics fm
		join 
			postgres.final_dates fd
			on fm.date_id = fd.id 
		join 
			postgres.final_location_population flp 
			on fm.location_population_id = flp.id 
	) fm
	group by 
		fm."Date"
	order by 
		fm."Date"
)

-- final output
select 
	vf."Date"::text as "Date",
	vf."Value A"::int as "Value A",
	vf."Value B"::int as "Value B"
from 
	value_frequency vf
;
"""

analysis_3b = """
-- aggregate metric value by month to identify change in value over time
with value_frequency as 
(
	select 
		fm."Date",
		sum(fm."Confirmed") as "Value A",
		avg(fm."Deaths") as "Value B"
	from 
	(
		-- showing the star schema relation of final tables
		select
			fm.date_id,
			fd."Date",
			fm.location_population_id,
			flp.iso2,
			flp.iso3,
			flp."Province_State",
			flp."Country_Region",
			flp."Lat",
			flp."Long_",
			flp."Combined_Key",
			flp."Population",
			fm."Last_Update",
			fm."Confirmed",
			fm."Deaths",
			fm."Recovered",
			fm."Active",
			fm."Incident_Rate",
			fm."Total_Test_Results",
			fm."People_Hospitalized",
			fm."Case_Fatality_Ratio",
			fm."Testing_Rate",
			fm."Hospitalization_Rate",
			fm."People_Tested",
			fm."Mortality_Rate"
		from
			postgres.final_metrics fm
		join 
			postgres.final_dates fd
			on fm.date_id = fd.id 
		join 
			postgres.final_location_population flp 
			on fm.location_population_id = flp.id 
	) fm
	group by 
		fm."Date"
	order by 
		fm."Date"
)

-- final output
select 
	round(corr(vf."Value A", vf."Value B")::decimal,2)::float as "Corr Coef"
from 
	value_frequency vf
;
"""