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
FIELDS TERMINATED BY '\t';

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

LOAD DATA LOCAL INPATH '/home/jeffy892/pageview-dec' INTO TABLE pageview_dec;
TRUNCATE TABLE pageview_dec;
DROP TABLE pageview_dec;

SELECT * FROM pageview_dec;


-- 6
SELECT COUNT(*) FROM revision_wiki WHERE event_entity = 'revision';
SELECT COUNT(*) FROM revision_wiki WHERE event_entity = 'page' AND event_type = 'delete';


-- Rockstar Games
SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total, 2) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
WHERE
	t1.prev='Rockstar_Games'
ORDER BY fraction DESC;

SELECT t1.prev, t1.curr, ROUND(t1.link / t2.total, 2) AS fraction FROM 
	(SELECT prev, curr, SUM(occurences) AS link FROM clickstream_122020 WHERE link_type='link' GROUP BY prev, curr) AS t1
INNER JOIN
	(SELECT article_title, SUM(count_views) AS total FROM pageview_dec GROUP BY article_title) AS t2
	ON t2.article_title = t1.prev
WHERE
	t1.prev='List_of_games_by_Rockstar_Games'
ORDER BY fraction DESC;
