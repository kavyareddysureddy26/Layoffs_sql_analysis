

## Data Cleaning
All data cleaning was done using SQL:

- Removed duplicate records
- Standardized company, industry, and country names
- Converted date column to proper DATE format
- Handled missing and null values
- Removed unusable rows
- Created clean staging tables for analysis

Final clean table used for analysis:
**`layoffs_staging2`**



## KPI Summary
High-level KPIs created using single-value queries:

- Total layoffs
- Total companies affected
- Total industries affected
- Total countries affected
- Time period covered
- Average layoff percentage

These KPIs give a quick overview of the global layoff situation.



## Exploratory Data Analysis (EDA)
Key questions explored:

- Which companies laid off the most employees?
- Which industries were impacted the most?
- Which countries experienced the highest layoffs?
- How did layoffs change over time?
- Which companies had peak layoff months?
- Which industries ranked highest by layoff percentage each year?



## Time & Trend Analysis
- Monthly and yearly layoff trends
- Company-wise rolling layoffs
- Peak layoff month per company
- Top 5 companies by layoffs each year



## Key Insights
- Layoffs increased significantly during economic downturn periods
- Technology and Crypto industries were most impacted
- The United States had the highest total layoffs
- Some companies experienced sharp, one-time layoff spikes
- Late-stage companies showed higher layoff percentages



## Tools Used
- SQL (MySQL)
- Window Functions
- CTEs
- GitHub



## Skills Demonstrated
- SQL data cleaning
- KPI creation
- Window functions
- Trend analysis
- Business insight generation
- Real-world dataset handling



