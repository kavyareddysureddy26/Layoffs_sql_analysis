-- Exploratory Data Analysis
select * from layoffs_staging2;

-- maximum of total and percentage laid off 
select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

-- minimum of total and percentage laid off 
select min(total_laid_off),min(percentage_laid_off)
from layoffs_staging2;

-- funds raised
select percentage_laid_off,funds_raised_millions
from layoffs_staging2 
order by funds_raised_millions desc;

-- company-wise total laidoff
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

select * from layoffs_staging2;

-- total laid off industry-wise
select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by 2 desc;

-- layoffs period (start to end)
select min(`date`),max(`date`)
from layoffs_staging2;

-- total_laid_offs in each year in every country
select country,year(`date`) years, sum(total_laid_off) 
from layoffs_staging2
where year(`date`) is not null
group by years,country
order by years asc;

-- montly layoffs in each month in every country in each company
select company,country,substring(`date`,6,2) months, sum(total_laid_off) total_laid_off_montly
from layoffs_staging2
where substring(`date`,6,2) is not null 
group by months,country,company
order by months asc;

-- company wise rolling total of laid off
with max_montly_total_layoff as
(select company,country,substring(`date`,6,2) months, sum(total_laid_off) as total_laid_off_montly
from layoffs_staging2
where substring(`date`,6,2) is not null 
group by months,country,company
order by months asc) 
select *, sum(total_laid_off_montly) over(partition by company) as rolling_total_monthly
from max_montly_total_layoff
order by months asc, company asc;

select * from layoffs_staging2
where company like'8X8%';

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

-- yearly layoff
select year(`date`) as years,sum(total_laid_off) as yearly_layoffs
from layoffs_staging2
where year(`date`) is not null
group by years
order by years;

-- top 5 layoff company names
-- yearly layoff
with top_5companies as(
select company,year(`date`) as years,sum(total_laid_off) as yearly_layoffs
from layoffs_staging2
group by year(`date`),company), company_year_rank as( 
select *,dense_rank() over(partition by years order by yearly_layoffs desc) as ranking from top_5companies where years is not null)
select * from company_year_rank where ranking<=5 ;

select * from layoffs_staging2;

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