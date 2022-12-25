-- Loading content of the txt files 
inputFile = LOAD 'hdfs:///user/Sowbhagya/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Omitting the first 2 lines
ranked = RANK inputFile;
ranked_lines = FILTER ranked BY (rank_inputFile > 2);

-- Grouping lines by name
grpd = GROUP ranked_lines BY name;

-- Count the number of lines by each character
total_count = FOREACH grpd GENERATE $0 as name, COUNT($1) as no_of_lines;
result = ORDER total_count BY no_of_lines DESC;

-- Remove output folder
-- rmf hdfs:///user/Sowbhagya/PigProjectOutput;

-- Store the results into folder
STORE result INTO 'hdfs:///user/Sowbhagya/PigProjectOut';
