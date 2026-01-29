select * 
from layoffs;

-- data cleaning--
-- 1.remove duplicates
-- 2. standardize the data
-- 3. check for null values and missing data
-- 4. remove unessasary colmn---

create table layoffs_staging like layoffs;

select * from layoffs_staging;

insert into layoffs_staging 
select * from layoffs;

select * from layoffs_staging;

-- remove duplicates

with no_duplicates as(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as rn
from layoffs_staging )
select * from no_duplicates where rn>1;

select * from layoffs_staging where company='hibob';

-- so as we got some duplictes we will create another table same as that and then delete the duplicates

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

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

select * from layoffs_staging2;
alter table layoffs_staging2 drop column row_num;