SELECT distinct userid, firstname, lastname, gender, level
FROM staging_events
WHERE page='NextSong'