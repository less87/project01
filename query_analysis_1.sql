
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
