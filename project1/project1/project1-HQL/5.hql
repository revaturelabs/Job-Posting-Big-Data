CREATE DATABASE project1_db;
USE project1_db;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE revision_wiki (
	wiki_db STRING,
	event_entity STRING,
	event_type STRING,
	event_timestamp STRING,
	event_comment STRING,
	event_user_id INT,
	event_user_text_historical STRING,
	event_user_text STRING,
	event_user_blocks_historical STRING,
	event_user_blocks STRING,
	event_user_groups_historical STRING,
	event_user_groups STRING,
	event_user_is_bot_by_historical STRING,
	event_user_is_bot_by STRING,
	event_user_is_created_by_self BOOLEAN,
	event_user_is_created_by_system BOOLEAN,
	event_user_is_created_by_peer BOOLEAN,
	event_user_is_anonymous BOOLEAN, 
	event_user_registration_timestamp STRING,
	event_user_creation_timestamp STRING,
	event_user_first_edit_timestamp STRING,
	event_user_revision_count INT,
	event_user_seconds_since_previous_revision INT,
	page_id INT,
	page_title_historical  STRING,
	page_title  STRING,
	page_namespace_historical INT,
	page_namespace_is_content_historical BOOLEAN,
	page_namespace INT,
	page_namespace_is_content BOOLEAN,
	page_is_redirect BOOLEAN,
	page_is_deleted BOOLEAN,
	page_creation_timestamp STRING,
	page_first_edit_timestamp STRING,
	page_revision_count INT,
	page_seconds_since_previous_revision INT,
	user_id INT,
	user_text_historical string,	
	user_text	string,
	user_blocks_historical string,
	user_blocks	string,	
	user_groups_historical	string,	
	user_groups	string,
	user_is_bot_by_historical string,	
	user_is_bot_by	string,	
	user_is_created_by_self boolean,	
	user_is_created_by_system boolean,
	user_is_created_by_peer boolean,
	user_is_anonymous boolean,
	user_registration_timestamp	string,
	user_creation_timestamp	string,
	user_first_edit_timestamp	string,
	revision_id INT,
	revision_parent_id INT, 
	revision_minor_edit boolean, 
	revision_deleted_parts	string,
	revision_deleted_parts_are_suppressed boolean,
	revision_text_bytes INT, 
	revision_text_bytes_diff INT, 
	revision_text_sha1	string,
	revision_content_model	string, 
	revision_content_format	string, 
	revision_is_deleted_by_page_deletion boolean,	
	revision_deleted_by_page_deletion_timestamp	string, 
	revision_is_identity_reverted boolean,
	revision_first_identity_reverting_revision_id INT,
	revision_seconds_to_identity_revert INT,
	revision_is_identity_revert boolean,
	revision_is_from_before_page_creation boolean,
	revision_tags	string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/jeffy892/2020-12.enwiki.2020-12.tsv' INTO TABLE revision_wiki;

-- Table for the whole day
CREATE TABLE pageviews_01252021 (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/' INTO TABLE pageviews_01252021;

-- Table for 1 hour
CREATE TABLE pageviews_01252021_6pm (
	domain_code STRING,
	article_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';
LOAD DATA LOCAL INPATH '/home/jeffy892/pageviews-01252021/pageviews-20210125-180000' INTO TABLE pageviews_01252021_6pm;


-- Gets the average time of vandalized wikipedia articles to be reverted back moderators.
SELECT event_type, AVG(page_seconds_since_previous_revision) FROM revision_wiki WHERE event_comment LIKE '%vandalism%' AND page_seconds_since_previous_revision > 0 GROUP BY event_type;

-- Divide the results from pageviews_01252021_6pm / pageviews_01252021
SELECT SUM(count_views) FROM pageviews_01252021_6pm;
SELECT SUM(count_views) FROM pageviews_01252021;

Je