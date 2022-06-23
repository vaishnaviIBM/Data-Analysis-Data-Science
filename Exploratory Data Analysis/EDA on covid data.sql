--Querying the data from covid deaths table and ordering by 1st and 2nd columns
SELECT location, date, total_cases,new_cases,total_deaths,population
FROM Project..['COVID DEATHS']
order by 1,2

--Exploring total deaths vs total cases
SELECT location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as Deaths_Ratio
FROM Project..['COVID DEATHS']
order by 1,2

--Looking at the death ratio of UK country
SELECT location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as Deaths_Ratio
FROM Project..['COVID DEATHS']
where location like '%united kingdom%'
order by 1,2

--Percentage of population affected with Covid
SELECT location, date, total_cases,population, (total_cases/population)*100 as Percent_population_infection
FROM Project..['COVID DEATHS']
where location like '%states%'
order by 1,2

--Countries with Highest Infection rate
SELECT location, Max(total_cases) as Highest_Infectioncount,population, Max((total_cases/population))*100 as Percent_population_infection
from Project..['COVID DEATHS']
where continent is not null
group by population, location
order by Percent_population_infection desc

--Continents with highest death count vs population
SELECT continent, Max(cast(total_deaths as int)) as total_deathcount
from Project..['COVID DEATHS']
where continent is not null  
-- Some data is missing in continent column so, lets ignore the missing value rows and check for the rest of the data
group by continent
order by total_deathcount desc

--Looking at countries with highest death rate by grouping both continents & Country
SELECT continent,location, Max(cast(total_deaths as int)) as total_deathcount
from Project..['COVID DEATHS']
where continent is not null  
-- Some data is missing in continent column so, lets ignore the missing value rows and check for the rest of the data
group by continent,location
order by total_deathcount desc

--New cases & deaths on each day grouping by continent
select date, continent, sum(new_cases) as total_newcases,sum(cast(new_deaths as int)) as total_newdeaths
from Project..['COVID DEATHS']
where continent is not null
group by date,continent
order by total_newcases desc

--Percentage of new deaths on each day grouping by continent
select date, sum(new_cases) as total_newcases,sum(cast(new_deaths as int)) as total_newdeaths, (sum(cast(new_deaths as int))/sum(new_cases))*100 as Death_ratio
from Project..['COVID DEATHS']
where continent is not null
group by date
order by Death_ratio desc

--Percentage of deaths in the world
select sum(new_cases) as total_newcases,sum(cast(new_deaths as int)) as total_newdeaths, (sum(cast(new_deaths as int))/sum(new_cases))*100 as Death_ratio
from Project..['COVID DEATHS']
where continent is not null




--Exploring VACCINATION table 

--total count of vaccinations happened on each day
select total_vaccinations,location,date from Project..['COVID VACCINATIONS']
order by date desc


--Vaccinations in India
select location, Max(total_vaccinations) as HighestVaccination from Project..['COVID VACCINATIONS']
where location like '%india%'
group by location

--Vaccinations in each location 
select location, Max(total_vaccinations) as HighestVaccination from Project..['COVID VACCINATIONS']
group by location
order by HighestVaccination desc


--Joining both Covid deaths and vaccination tables using Join method
select * 
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date


--Exploring total number of people vaccinated in the world with total population
select deaths.date, deaths.location,deaths.continent, deaths.population,vaccine.new_vaccinations 
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date
where deaths.continent is not null
order by date


--Looking at total population vs vaccintions
select deaths.date, deaths.location,deaths.continent, deaths.population,vaccine.new_vaccinations,Sum(cast(vaccine.new_vaccinations as bigint))OVER (Partition by deaths.Location order by deaths.location, deaths.date) as rolling_agg_vaccinations
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date
where deaths.continent is not null
order by deaths.location, deaths.date


--Using Common Table Expressions for finding the percentage of total people vaccinated/popluations
with Pop_Vacc (continent, date, location, Population, new_vaccinations, rolling_agg_vaccinations)
as
(
select deaths.date, deaths.location,deaths.continent, deaths.population, vaccine.new_vaccinations,Sum(cast(vaccine.new_vaccinations as bigint))OVER (Partition by deaths.Location order by deaths.location, deaths.date) as rolling_agg_vaccinations
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date
where deaths.continent is not null
)
select *, ((rolling_agg_vaccinations/population)*100) as percentage_of_vaccinations from Pop_Vacc


--Creating a temporary table and storing the calculated percentage of people vaccinated

drop table if exists #Percentage_of_people_vaccinated
Create Table #Percentage_of_people_vaccinated
(
date datetime,
Location nvarchar(255),
continent nvarchar(255),
Population numeric,
New_Vaccinations numeric,
rolling_agg_vaccinations numeric
)
Insert into #Percentage_of_people_vaccinated
select deaths.date, deaths.location,deaths.continent, deaths.population, vaccine.new_vaccinations,Sum(cast(vaccine.new_vaccinations as bigint))OVER (Partition by deaths.Location order by deaths.location, deaths.date) as rolling_agg_vaccinations
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date


select *, ((rolling_agg_vaccinations/population)*100) as percentage_of_vaccinations from #Percentage_of_people_vaccinated


--Createing view
Create View Percentage_of_people_vacc as

select deaths.date, deaths.location,deaths.continent, deaths.population, vaccine.new_vaccinations,Sum(cast(vaccine.new_vaccinations as bigint))OVER (Partition by deaths.Location order by deaths.location, deaths.date) as rolling_agg_vaccinations
from Project..['COVID DEATHS'] as deaths
join Project..['COVID VACCINATIONS'] as vaccine
on deaths.location=vaccine.location
and deaths.date=vaccine.date
where deaths.continent is not null