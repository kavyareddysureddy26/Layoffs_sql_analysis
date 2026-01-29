-- DATA OVERVIEW
select * 
from layoffs;

-- DATA CLEANING 
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Check for null values and missing data
-- 4. Remove unessasary columns and rows

-- Staging Tables

create table layoffs_staging like layoffs;

select * from layoffs_staging;

insert into layoffs_staging 
select * from layoffs;

select * from layoffs_staging;

-- Remove duplicates

with no_duplicates as(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rn
from layoffs_staging )
select * from no_duplicates where rn>1;

select * from layoffs_staging where company='hibob';

-- so as we got some duplictes we will create another table same as that and then delete the duplicates
-- Staging Table 

 drop table if exists layoffs_staging2;
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2 
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rn
from layoffs_staging;

select * from layoffs_staging2
where row_num>1;

delete from layoffs_staging2 where row_num>1;

-- standardizing Data 
select company, trim(company) as trim_company from layoffs_staging2 ;
update layoffs_staging2 set company=trim(company);

select * from layoffs_staging2;
select industry from layoffs_staging2 ;
select industry from layoffs_staging2 where industry like 'crypto%';
update layoffs_staging2 set industry='Crypto' where industry like 'crypto%';
select industry from layoffs_staging2;

select location,trim(location) from layoffs_staging2;
update layoffs_staging2 set location= trim(location);
select distinct(location) from layoffs_staging2
order by 1;

select distinct(country) from layoffs_staging2;
select country from layoffs_staging2 where country like 'united states%';
update layoffs_staging2 set country='United States' where country like 'united states.%';

-- Date Conversion
select `date` from layoffs_staging2;
select `date`, str_to_date(`date`,'%m/%d/%Y') from layoffs_staging2 ;
update layoffs_staging2 set `date`=str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_staging2 modify column `date` DATE;

-- Missing & null values
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
select * from layoffs_staging2 where industry is null or industry='';
select * from layoffs_staging2 where company='airbnb';

select t1.company,t1.industry,t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoffs_staging2 set industry=null where industry=''; 

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry=t2.industry
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

-- Dropping rows
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

-- Dropping Helper Columns
select * from layoffs_staging2;
alter table layoffs_staging2 drop column row_num;

-- ---------------------------------------------------------------------
-- Exploratory Data Analysis(EDA)

-- Clean Data Validation
select * from layoffs_staging2;

-- -----------------------------------------------------------------------
-- KPI SUMMARY

-- layoff period
select min(`date`),max(`date`) from layoffs_staging2;

-- count checks
select count(distinct industry) as industry_count from layoffs_staging2;
select count(distinct company) as company_count from layoffs_staging2;
select count(distinct country) as country_count from layoffs_staging2;
select count(distinct stage) as stage_count from layoffs_staging2;

-- total and avg layoffs
select sum(total_laid_off),round(avg(percentage_laid_off)*100,2)
from layoffs_staging2;

-- -----------------------------------------------------------------------
-- KPI DRIVERS

-- company-wise total laidoff
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

-- total and avg_percent laid off industry-wise
select industry, sum(total_laid_off) total_layoff,avg(percentage_laid_off) as avg_layoff 
from layoffs_staging2
group by industry
order by 2 desc;

-- total and avg_percent laid off country-wise
select country, sum(total_laid_off) total_layoff,avg(percentage_laid_off) as avg_layoff 
from layoffs_staging2
group by country
order by 2 desc;

-- total and avg_percent laid off stage-wise
select stage, sum(total_laid_off) total_layoff,avg(percentage_laid_off) as avg_layoff 
from layoffs_staging2
group by stage
order by 2 desc;

-- total and avg_percent laid off location,country-wise
select country,location, sum(total_laid_off) total_layoff,avg(percentage_laid_off) as avg_layoff 
from layoffs_staging2
group by country,location
order by 3 desc;

-- ----------------------------------------------------------------------------------------------
-- TIME TRENDS

-- yearly layoff
select year(`date`) as years,sum(total_laid_off) as yearly_layoffs
from layoffs_staging2
where year(`date`) is not null
group by years
order by years;

-- Monthly Layoffs
with max_montly_total_layoff as
(select company,country,substring(`date`,1,7) months, sum(total_laid_off) as total_laid_off_montly
from layoffs_staging2
where substring(`date`,1,7) is not null 
group by months,country,company) , ranked as(
select company,country,months,total_laid_off_montly,
dense_rank() over(partition by company order by total_laid_off_montly) as dr
from max_montly_total_layoff )
select * from ranked where dr =1 and total_laid_off_montly is not null
order by months asc,total_laid_off_montly desc  ;

-- company wise Rolling total over time
with max_montly_total_layoff as
(select company,country,substring(`date`,1,7) months, sum(total_laid_off) as total_laid_off_montly
from layoffs_staging2
where substring(`date`,1,7) is not null 
group by months,country,company
order by months asc) 
select *
from max_montly_total_layoff
where total_laid_off_montly is not null;

-- ---------------------------------------------------------------------------------------
-- RANKING INSIGHTS

-- top 5 layoff company names(yearly layoff)
with top_5companies as(
select company,year(`date`) as years,sum(total_laid_off) as yearly_layoffs
from layoffs_staging2
group by year(`date`),company), company_year_rank as( 
select *,dense_rank() over(partition by years order by yearly_layoffs desc) as ranking from top_5companies where years is not null)
select * from company_year_rank where ranking<=5 ;

-- yearly layoff
with yearly_topcompanies as(
select company,year(`date`) as years,sum(total_laid_off) as yearly_layoffs
from layoffs_staging2
group by year(`date`),company), company_year_rank as( 
select *,dense_rank() over(partition by years order by yearly_layoffs desc) as ranking from yearly_topcompanies where years is not null)
select * from company_year_rank where ranking=1 ;

-- Peak Moth Per Comany
with max_montly_total_layoff as
(select company,substring(`date`,1,7) months, sum(total_laid_off) as total_laid_off_montly
from layoffs_staging2
where substring(`date`,1,7) is not null 
group by months,company) , ranked as(
select company,months,total_laid_off_montly,
dense_rank() over(partition by company order by total_laid_off_montly desc) as dr
from max_montly_total_layoff )
select company,months,total_laid_off_montly 
from ranked 
where dr =1 and total_laid_off_montly is not null
order by company;

-- -------------------------------------------------------------------
-- SEVERITY CHECK 

-- industry-wise avg layoff percentage for all years
select industry ,round(avg(percentage_laid_off)*100,2) as total_percentage_laidoff
from layoffs_staging2
group by industry
order by total_percentage_laidoff desc;

-- yearly industry wise layoffpercentage
with industry_total_percentage as
(select industry ,year(`date`) as years,round(avg(percentage_laid_off)*100,2) as total_percentage_laidoff
from layoffs_staging2
group by years,industry),industry_year_rank as( 
select *,dense_rank() over(partition by years order by total_percentage_laidoff desc) as ranking 
from industry_total_percentage 
where years is not null and total_percentage_laidoff is not null)
select * from industry_year_rank where ranking<=5;

-- country wise layoff percentage
with country_total_percentage as
(select country,round(avg(percentage_laid_off)*100,2) as avg_percentage_laidoff
from layoffs_staging2
group by country)
select *
from country_total_percentage 
where country is not null and avg_percentage_laidoff is not null
order by avg_percentage_laidoff desc;

-- top 5 country avg_layoffs
with country_avg_percentage as
(select country,round(avg(percentage_laid_off)*100,2) as avg_percentage_laidoff
from layoffs_staging2
where country is not null and 
percentage_laid_off is not null
group by country),country_rn as
(select *,dense_rank() over(order by avg_percentage_laidoff desc) as ranking
from country_avg_percentage)
select * from country_rn where ranking<=5 ;

-- ------------------------------------------------------------------------------
-- KEY INSIGHTS

-- 1. Layoffs impacted a large number of companies across multiple industries,
--    indicating a widespread global workforce reduction rather than isolated cases.

-- 2. A small group of companies and industries contributed most of the total layoffs,
--    showing that workforce reductions were highly concentrated among major players.

-- 3. Layoffs peaked during specific years and months, suggesting strong influence
--    from macroeconomic and market conditions rather than random company decisions.

-- 4. Impact and severity tell different stories:
--    • Some industries had high total layoffs (large absolute impact)
--    • Other industries showed high average layoff percentages (higher severity),
--      meaning deeper workforce cuts relative to company size.

-- 5. Several industries consistently appeared among the most affected by layoffs,
--    pointing to structural challenges rather than one-time events.

-- 6. For most companies, layoffs were concentrated in a single peak month,
--    indicating major restructuring events instead of gradual workforce reductions.

-- 7. Companies with strong funding levels were also affected by layoffs,
--    suggesting layoffs were driven by strategic realignment and efficiency
--    rather than only financial distress.
























