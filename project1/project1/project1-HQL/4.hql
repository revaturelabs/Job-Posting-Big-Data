CREATE DATABASE project1_db;
USE project1_db;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

------------------------------------------------------------------------------------------------------------------------------
-- 4
-- January 25, 2021
CREATE TABLE pageviews_20210125_americas (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-150000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-160000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-170000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-180000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-190000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-200000' INTO TABLE pageviews_20210125_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-210000' INTO TABLE pageviews_20210125_americas;


--TRUNCATE TABLE pageviews_20210125_americas;
--SELECT * FROM pageviews_20210125_americas;


--SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_americas WHERE domain_code LIKE 'en%' OR domain_code LIKE 'es%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;
SELECT article_title, SUM(count_views) AS total_views 
FROM pageviews_20210125_americas 
WHERE domain_code LIKE 'en%' 
GROUP BY article_title 
ORDER BY total_views DESC 
LIMIT 50;

CREATE TABLE pageviews_20210125_other (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

-- January 25, 2021
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-090000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-100000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-110000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-120000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-130000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-140000' INTO TABLE pageviews_20210125_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-150000' INTO TABLE pageviews_20210125_other;

--TRUNCATE TABLE pageviews_20210125_other;
--SELECT * FROM pageviews_20210125_other;

-- Get the top 50 articles that does not use any english or spanish 
--SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_other WHERE domain_code NOT LIKE 'en%' AND domain_code NOT LIKE 'es%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;
SELECT article_title, SUM(count_views) AS total_views 
FROM pageviews_20210125_other 
WHERE domain_code LIKE 'en%' 
GROUP BY article_title 
ORDER BY total_views DESC LIMIT 50;

--DROP TABLE pageviews_01202021;
