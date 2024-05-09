CREATE KEYSPACE IF NOT EXISTS jobs_stream WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS jobs_stream.jobs_postings (
    title text,
    companyName text,
    salary text,
    jobUrl text,
    companyUrl text,
    location text,
    postedTime text,
    sector text,
    description text,
    PRIMARY KEY (jobUrl),
 added_at timestamp
);

# To drop the rows of streaming for demo purposes
# TRUNCATE jobs_stream.jobs_postings;


CREATE TABLE IF NOT EXISTS jobs_stream.jobs_locations (
    location text, 
    mentioned_times int,
    PRIMARY KEY (location),
createdAt timestamp,
);


CREATE TABLE IF NOT EXISTS jobs_stream.words (
    word text, 
    mentioned_times int,
    PRIMARY KEY (word),
createdat timestamp,
);

