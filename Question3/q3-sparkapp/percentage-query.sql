WITH post_count AS (SELECT url_host_name, count(*) as n
    FROM "ccindex"."ccindex"
    WHERE crawl = 'CC-MAIN-2020-05'
      AND subset = 'warc'
      AND (url_host_registry_suffix = 'com') 
      AND fetch_status = 200 
      AND content_languages = 'eng' 
      AND (LOWER(url_path) LIKE '%/job%' OR LOWER(url_path) LIKE '%/career%') 
      AND (
      LOWER(url_path) LIKE '%technology%' OR
      LOWER(url_path) LIKE '%analyst%' OR
      LOWER(url_path) LIKE '%devops%' OR
      LOWER(url_path) LIKE '%developer%' OR
      LOWER(url_path) LIKE '%frontend%' OR
      LOWER(url_path) LIKE '%backend%' OR
      LOWER(url_path) LIKE '%fullstack%' OR
      LOWER(url_path) LIKE '%cybersecurity%' OR
      LOWER(url_path) LIKE '%software%' OR
      LOWER(url_path) LIKE '%computer%' OR
      LOWER(url_path) LIKE '%python%' OR
      LOWER(url_path) LIKE '%java%' OR
      LOWER(url_path) LIKE '%c++%' OR
      LOWER(url_path) LIKE '%data%' OR 
      LOWER(url_path) LIKE '%web%developer%' OR 
      LOWER(url_path) LIKE '%web%design%' OR 
      LOWER(url_path) LIKE '%artificial%intelligence%' OR
      LOWER(url_path) LIKE '%network%' OR 
      LOWER(url_path) LIKE '%programmer%'
      )
     group by 1
     )
SELECT COUNT(*) * 100 / (SELECT COUNT(*) FROM post_count) AS percentage_posting_less_than_4
FROM post_count
WHERE n < 4
     
 
--------
results percentage_posting_less_than_4:
05 (Jan) -> 71%
10 (Feb) -> 69%
16 (March/April) -> 72%
24 (May/June) -> 68%
29 (July) -> 68%
34 (August) -> 73%
40 (September) -> 71%
45 (October) -> 71%
50 (November/December) -> 70%

Total average 70.33%