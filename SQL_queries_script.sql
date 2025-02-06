------------------------ DATABASE PREPARATION FOR ANALYSIS ------------------------

-- Creating a new table `president_elections.president_elect_summary` where we will store all the data for analyzing presidential elections
create table president_elections.president_elect_summary(
	state varchar(50)
	,cenzus_region varchar(50)
	,cenzus_subregion varchar(50)
	,county varchar(50)
	,candidate varchar(50)
	,party varchar(50)
	,candidate_votes numeric
	,all_county_votes_cast numeric
	,all_possible_county_votes numeric
	,percetage_of_county_votes_cast numeric
	,all_possible_state_votes numeric
	,if_candidate_won bool
);

--drop table president_elections.president_elect_summary

-- Filling the newly created table with data from the CTE `president_elect`, which combines data from imported presidential election tables
with
president_elect as
(select 
	pcc.state
	,case 
		when pcc.state in ('Connecticut','Maine','Massachusetts','New Hampshire','Rhode Island','Vermont',
							'New Jersey','New York','Pennsylvania') then 'Northeast'
		when pcc.state in ('Illinois','Indiana','Michigan','Ohio','Wisconsin',
							'Iowa','Kansas','Minnesota','Missouri','Nebraska','North Dakota','South Dakota') then 'Midwest'
		when pcc.state in ('Delaware','District of Columbia','Florida','Georgia','Maryland','North Carolina','South Carolina','Virginia','West Virginia',
							'Alabama','Kentucky','Mississippi','Tennessee',
							'Arkansas','Louisiana','Oklahoma','Texas') then 'South'	
		when pcc.state in ('Arizona','Colorado','Idaho','Montana','Nevada','New Mexico','Utah','Wyoming',
							'Alaska','California','Hawaii','Oregon','Washington') then 'West'	
		else 'ERROR!'
	end as cenzus_region
	,case 
		when pcc.state in ('Connecticut','Maine','Massachusetts','New Hampshire','Rhode Island','Vermont') then 'New England'
		when pcc.state in ('New Jersey','New York','Pennsylvania') then 'Mid-Atlantic'
		when pcc.state in ('Illinois','Indiana','Michigan','Ohio','Wisconsin') then 'East North Central'
		when pcc.state in ('Iowa','Kansas','Minnesota','Missouri','Nebraska','North Dakota','South Dakota') then 'West North Central'
		when pcc.state in ('Delaware','District of Columbia','Florida','Georgia','Maryland','North Carolina','South Carolina','Virginia','West Virginia') then 'South Atlantic'
		when pcc.state in ('Alabama','Kentucky','Mississippi','Tennessee') then 'East South Central'
		when pcc.state in ('Arkansas','Louisiana','Oklahoma','Texas') then 'West South Central'	
		when pcc.state in ('Arizona','Colorado','Idaho','Montana','Nevada','New Mexico','Utah','Wyoming') then 'Mountain'
		when pcc.state in ('Alaska','California','Hawaii','Oregon','Washington') then 'Pacific'	
		else 'ERROR!'
	end as cenzus_subregion
	,pcc.county
	,pcc.candidate
	,pcc.party 
	,pcc.total_votes as candidate_votes
	,pc.current_votes as all_possible_county_votes
	,pc.total_votes as all_county_votes_cast
	,pc.percent as percentage_of_votes_cast
	,ps.total_votes as all_possible_state_votes
	,pcc.won as if_candidate_won
from 
	president_elections.president_county_candidate pcc
	left join president_elections.president_county pc 
		on pcc.state = pc.state and pcc.county = pc.county
	left join president_elections.president_state ps
		on pcc.state = ps.state
order by 
	pcc.state,pcc.county,pcc.total_votes desc)
insert into 
	president_elections.president_elect_summary
select 
	*
from 
	president_elect;

-- Viewing the first 100 rows of the table after importing data
select 
	*
from 
	president_elections.president_elect_summary pes 
limit 100;

-- Checking if there are any errors in assigning states to regions and subregions
select 
	distinct 
	pes.cenzus_region
	,pes.cenzus_subregion
	,pes.state 
from
	president_elections.president_elect_summary pes 
where
	pes.cenzus_region = 'ERROR!' or pes.cenzus_subregion = 'ERROR!' 
order by 
	1;

-- Verifying that the number of rows in the new table equals the number of rows in the largest of its source tables
select
	count(pes.county) as pes_rows_number
	,count(pcc.county) as pcc_rows_number
from 
	president_elections.president_elect_summary pes,
	president_elections.president_county_candidate pcc;

-- Winners and vote counts for candidates of a given party in a specific state and county
select
	distinct
	pes.state
	,pes.party
	,pes.candidate
	,pes.if_candidate_won
	,sum(pes.candidate_votes) over(partition by pes.state, pes.party, pes.candidate) as number_of_votes_in_state
	,pes.all_possible_state_votes 
	,pes.cenzus_region
	,pes.cenzus_subregion
from 
	president_elections.president_elect_summary pes 
--where 
--	pes.state  = 'Illinois' 
--	and pes.candidate = 'Joe Biden'
order by
	1,2,3,6 desc;

/* In the United States, where an electoral system exists, it is essential to know how many electoral votes each state has.
 * Let's use the dataset `electoral_college`, which contains the number of electoral votes per state across all elections.
 * Of course, we are only interested in the year 2020.
*/
select 
	ec.*
	,sum(ec."Votes") over() as Total_votes
from 
	president_elections.electoral_college ec
where ec."Year" = '2020';

--drop table electoral_college 

-- Creating a new table `president_elections.state_winners` based on the CTE
create table president_elections.state_winners as
with state_win as 
(select
	src.state
	,src.party
	,src.candidate
	,src.candidate_votes
	,src.all_possible_state_votes
	,case 
		when src.candidate_votes = max(src.candidate_votes) over(partition by src.state) then true
		else false 
	end as if_candidate_won
from
	(select 
		pes.state
		,pes.party
		,pes.candidate
		,pes.all_possible_state_votes
		,sum(pes.candidate_votes) as candidate_votes
	from 
		president_elections.president_elect_summary pes
	group by
		pes.state
		,pes.party
		,pes.candidate
		,pes.all_possible_state_votes) as src)
,electors as
(select 
	ec."State" as state
	,ec."Votes" as electoral_votes
from
	president_elections.electoral_college ec
where 
	ec."Year" = '2020'
)
select 
	sw.*
	,e.electoral_votes
from 
	state_win sw
left join electors e
	on sw.state = e.state
order by 
	sw.state,
	sw.candidate_votes desc;

--drop table president_elections.state_winners

-- Let's see how our new table looks
select 
	*
from 
	president_elections.state_winners
limit 100;


------------------------ DATA ANALYSIS FOR RECOMMENDATIONS AND ANOMALY DETECTION ------------------------

-- Base dataset to be analyzed in Tableau
select
	distinct
	pes.state
	,pes.county 
	,pes.party
	,pes.candidate
	,pes.if_candidate_won as if_candidate_won_in_county
	,sum(pes.candidate_votes) over(partition by pes.state, pes.county, pes.party, pes.candidate) as number_of_votes_in_county
	,pes.all_county_votes_cast
	,pes.all_possible_county_votes 
	,sum(cast(pes.if_candidate_won as int)) over(partition by pes.state, pes.party, pes.candidate) as number_of_wins_in_state
	,sum(pes.candidate_votes) over(partition by pes.state, pes.party, pes.candidate) as number_of_votes_in_state
	,pes.all_possible_state_votes
	,sw.if_candidate_won as if_candidate_won_in_state
	,pes.cenzus_region
	,pes.cenzus_subregion
from 
	president_elections.president_elect_summary pes
left join president_elections.state_winners sw 
	on pes.state = sw.state 
	and pes.candidate = sw.candidate
--where
--	pes.party in ('DEM','REP')	
order by
	1,2,6 desc;


-- Assigning a state's political sympathy to a party based on the margin of the winning candidate
select
	sub_win.state
	,sub_win.party
	,sub_win.candidate
	,sub_win.percentage_of_all_votes as winner_percentage_of_all_votes
	,sub_loose.percentage_of_all_votes as opponent_percentage_of_all_votes
	,sub_win.percentage_of_all_votes - sub_loose.percentage_of_all_votes as margin
	,case
		when sub_win.party = 'REP' and sub_win.percentage_of_all_votes - sub_loose.percentage_of_all_votes > 5 then 'RED STATE'
		when sub_win.party = 'DEM' and sub_win.percentage_of_all_votes - sub_loose.percentage_of_all_votes > 5 then 'BLUE STATE'
		else 'SWING STATE'
	end as state_symphaty
	,sub_win.if_candidate_won
	,sub_win.electoral_votes
from
	(select -- IN THIS QUERY, WE INDICATE THE RESULTS OF THE WINNERS IN EACH STATE 
		sw.state
		,sw.party
		,sw.candidate
		,round(sw.candidate_votes/sw.all_possible_state_votes*100,2) as percentage_of_all_votes
		,if_candidate_won
		,electoral_votes
	from 
		president_elections.state_winners sw 
	where 
		sw.party in ('DEM','REP')
		and sw.if_candidate_won = true) as sub_win
left join 
	(select -- IN THIS QUERY, WE INDICATE THE RESULTS OF LOSING CANDIDATES
		sw.state
		,sw.party
		,sw.candidate
		,round(sw.candidate_votes/sw.all_possible_state_votes*100,2) as percentage_of_all_votes
		,if_candidate_won
		,electoral_votes
	from 
		president_elections.state_winners sw 
	where 
		sw.party in ('DEM','REP')
		and sw.if_candidate_won = false) as sub_loose
	on sub_win.state = sub_loose.state;
--where 
--	(case
--		when sub_win.party = 'REP' and sub_win.percentage_of_all_votes - sub_loose.percentage_of_all_votes > 5 then 'RED STATE'
--		when sub_win.party = 'DEM' and sub_win.percentage_of_all_votes - sub_loose.percentage_of_all_votes > 5 then 'BLUE STATE'
--		else 'SWING STATE'
--	end) = 'SWING STATE';


-- States where a candidate won the entire state but did not achieve the highest number of wins in counties
select 
	sub.state
	,sub.party
	,sub.candidate as winner
	,sub.number_of_state_votes
	,sub.number_of_state_wins
	,sub.if_max_number_of_state_wins
	,coalesce(sw.if_candidate_won,false) as if_candidate_won
from
	(select 
		ssub.state
		,ssub.party
		,ssub.candidate
		,ssub.number_of_state_votes
		,ssub.number_of_state_wins
		,case 
			when ssub.number_of_state_wins = max(ssub.number_of_state_wins) over(partition by ssub.state) then true
			else false
		end as if_max_number_of_state_wins
	from
	(select 
		distinct
		pes.state
		,pes.candidate
		,pes.party
		,sum(pes.candidate_votes) over(partition by pes.state, pes.party, pes.candidate) as number_of_state_votes
		,sum(cast(pes.if_candidate_won as int)) over(partition by pes.state, pes.party, pes.candidate) as number_of_state_wins
	from 
		president_elections.president_elect_summary pes
	order by 
		1,4 desc) as ssub
		) as sub
left join president_elections.state_winners sw
	on sub.state = sw.state 
	and sub.candidate = sw.candidate
	and sw.if_candidate_won is true
where 
	sw.if_candidate_won is true;


-- In the next step, we should identify the largest county by votes cast in each state
-- If a candidate won in that county but did not win the entire state, we have another anomaly worth investigating
select
	distinct
	rnk_cast.state
	,rnk_cast.county as biggest_county_by_votes_cast
	,rnk_cast.county_votes_cast -- for Arizona and Maricopa County there is a data error
	,rnk_cast.rank_by_votes_cast
	,rnk_poss.all_possible_county_votes
	,cw.candidate as county_winner
	,cw.candidate_votes as county_winner_votes
	,sw.candidate as state_winner
	,case 
		when cw.candidate != sw.candidate then true
		else false 
	end as winner_of_state_vs_winner_of_biggest_county
from
	(select 
		distinct
		pes.state
		,pes.county
		,pes.all_county_votes_cast as county_votes_cast
		,dense_rank() over(partition by pes.state order by pes.all_county_votes_cast desc) as rank_by_votes_cast
	from 
		president_elections.president_elect_summary pes
	where
		pes.all_county_votes_cast > 0) as rnk_cast
left join
	(select 
		distinct
		pes.state
		,pes.county
		,pes.all_possible_county_votes
	from 
		president_elections.president_elect_summary pes
	where
		pes.all_possible_county_votes > 0) as rnk_poss
	on rnk_cast.state = rnk_poss.state
	and rnk_cast.county = rnk_poss.county
left join 
	(select
		distinct
		pes.state
		,pes.county
		,pes.candidate
		,pes.party
		,sum(pes.candidate_votes) over(partition by pes.state, pes.county, pes.candidate) as candidate_votes
	from 
		president_elections.president_elect_summary pes
	) as cw
	on rnk_cast.state = cw.state
	and rnk_cast.county = cw.county
	and rnk_cast.rank_by_votes_cast = 1
left join
	(select 
		sw.state
		,sw.candidate
		,sw.party
		from 
		president_elections.state_winners sw
	where
		sw.if_candidate_won = true) as sw
	on rnk_cast.state = sw.state;