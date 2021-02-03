CREATE DATABASE project1_db;
USE project1_db;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE clickstream_122020 (
	prev STRING,
	curr STRING,
	link_type STRING,
	occurences INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/jeffy892/clickstream-enwiki-2020-12.tsv' INTO TABLE clickstream_122020;

SHOW DATABASES;

SELECT * FROM clickstream_122020;

CREATE TABLE clickstream_122020_linkprev (
	prev STRING,
	curr STRING,
	occurences INT
)
PARTITIONED BY (link_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

DROP TABLE clickstream_122020_linkprev;

INSERT INTO TABLE clickstream_122020_linkprev PARTITION(link_type)
SELECT prev, curr, occurences, link_type FROM clickstream_122020;

SELECT * FROM clickstream_122020_linkprev;

CREATE TABLE pageview_dec (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/jeffy892/pageview-jan' INTO TABLE pageview_dec;
TRUNCATE TABLE pageview_dec;
DROP TABLE pageview_dec;

SELECT * FROM pageview_dec;

------------------------------------------------------------------------------------------------------------------------------
-- 2
--SELECT curr, SUM(occurences) AS occurs FROM clickstream_122020_linkprev WHERE curr='Elliot_Page' GROUP BY curr ORDER BY occurs DESC;
--SELECT prev, curr, SUM(occurences) AS occurs FROM clickstream_122020_linkprev WHERE link_type ='link' GROUP BY prev, curr ORDER BY occurs DESC;

--SELECT curr, prev, SUM(occurences) AS total_views FROM clickstream_122020 GROUP BY curr, prev;

--SELECT  t2.curr, t1.curr, ROUND(t1.link_views/t2.total_views,2) AS fraction FROM 
--(SELECT prev, SUM(occurences) AS total_views FROM clickstream_122020_linkprev GROUP BY prev) AS t2
--INNER JOIN
--(SELECT curr, prev, SUM(occurences) AS link_views FROM clickstream_122020_linkprev WHERE link_type ='link' GROUP BY curr, prev) AS t1
--ON t1.prev = t2.curr
--ORDER BY fraction DESC;

SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
	INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
ORDER BY fraction DESC;

SELECT curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY curr ORDER BY link DESC;
SELECT article_title, SUM(count_views) AS total FROM pageview_dec WHERE domain_code LIKE 'en%' AND article_title='Brotherhood_Bridge' GROUP BY article_title;

------------------------------------------------------------------------------------------------------------------------------
-- 3
SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total, 2) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
	INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
WHERE
	t1.prev='Hotel_California'
ORDER BY fraction DESC;

SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total, 2) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
	INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
WHERE
	t1.prev='Hotel_California_(Eagles_album)'
ORDER BY fraction DESC;


SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total, 2) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
	INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
WHERE
	t1.prev='Their_Greatest_Hits_(1971–1975)'
ORDER BY fraction DESC;


------------------------------------------------------------------------------------------------------------------------------
-- 4
CREATE TABLE pageviews_20210120_americas (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

-- Load the data from 9am to 3pm CST timezone
-- January 20, 2021
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-150000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-160000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-170000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-180000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-190000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-200000' INTO TABLE pageviews_20210120_americas;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-210000' INTO TABLE pageviews_20210120_americas;

TRUNCATE TABLE pageviews_20210120_americas;
SELECT * FROM pageviews_20210120_americas;

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


TRUNCATE TABLE pageviews_20210125_americas;
SELECT * FROM pageviews_20210125_americas;


SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_americas WHERE domain_code LIKE 'en%' OR domain_code LIKE 'es%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;
SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_americas WHERE domain_code LIKE 'en%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;


CREATE TABLE pageviews_20210120_other (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

-- Load the data from 9am to 3pm UTC.
-- January 20, 2021
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-090000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-100000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-110000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-120000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-130000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-140000' INTO TABLE pageviews_20210120_other;
LOAD DATA LOCAL INPATH '/home/jeffy892/wiki-dataset-pageviews/pageviews-20210120-150000' INTO TABLE pageviews_20210120_other;

TRUNCATE TABLE pageviews_20210120_other;
SELECT * FROM pageviews_20210120_other;

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

TRUNCATE TABLE pageviews_20210125_other;
SELECT * FROM pageviews_20210125_other;

-- Get the top 50 articles that does not use any english or spanish 
SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_other WHERE domain_code NOT LIKE 'en%' AND domain_code NOT LIKE 'es%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;
SELECT article_title, SUM(count_views) AS total_views FROM pageviews_20210125_other WHERE domain_code LIKE 'en%' GROUP BY article_title ORDER BY total_views DESC LIMIT 50;

DROP TABLE pageviews_01202021;


-- 5


