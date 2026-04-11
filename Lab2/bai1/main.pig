
data = LOAD '/hotel-review' USING PigStorage(';') AS(
    id : int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment : chararray
);

-- Lower
data_lower = FOREACH data GENERATE 
    id,
    LOWER(review) AS review,
    category,
    aspect,
    sentiment;

STORE data_lower INTO '/output/lower' USING PigStorage('\t');

-- Tokenize
data_clean = FOREACH data_lower GENERATE
    id,
    REPLACE(review, '[..,@!^]', '') AS review, 
    category,
    aspect,
    sentiment;

words = FOREACH data_clean GENERATE
    id,
    FLATTEN(TOKENIZE(review)) AS word, 
    category,
    aspect,
    sentiment;  
STORE words INTO '/output/tokenize' USING PigStorage('\t');

--Remove stopword
stopword = LOAD '/stopwords' USING PigStorage() AS(
    word
);

join_words = JOIN words BY word LEFT OUTER, stopword BY word;

join_words = FILTER join_words BY stopword::word IS NULL;

STORE join_words INTO '/output/remove_stopword' USING PigStorage('\t');

samples = LIMIT join_words 10;
DUMP samples;