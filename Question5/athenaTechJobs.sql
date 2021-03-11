-- This query assumes you have created the database and table following the instructions at:
-- https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/

-- Using AWS Athena, this will query 1-2 TB of data, and returns a count of all jobs
-- that contain both 'job' and at least one of the 'tech' keywords in the url that was posted in 2020
-- Change the crawl to specify a different year.

SELECT count(*) AS "Tech Jobs"
FROM 
    (SELECT LOWER(url_path) as url
    FROM "ccindex"."ccindex"
    WHERE crawl LIKE 'CC-MAIN-2020%' AND
    subset = 'warc' AND
    fetch_status = 200 AND
    content_languages = 'eng' AND
     (url LIKE '%job%') AND 
     (url LIKE '%frontend%' OR
      url LIKE '%backend%' OR
      url LIKE '%fullstack%' OR
      url LIKE '%cybersecurity%' OR
      url LIKE '%software%' OR
      url LIKE '%computer%' OR
      url LIKE '%python%' OR
      url LIKE '%java%' OR
      url LIKE '%c++%' OR
      url LIKE '%data%' OR 
      url LIKE '%web%developer%' OR 
      url LIKE '%web%design%' OR 
      url LIKE '%artificial%intelligence%' OR
      url LIKE '%network%' OR 
      url LIKE '%programmer%'))