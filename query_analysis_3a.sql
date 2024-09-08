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