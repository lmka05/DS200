-- Load data
reviews = LOAD '/hotel-review' USING PigStorage(';')
          AS (id:int, text:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '/stopwords' AS (stopword:chararray);

-- LOWERCASE
lower_reviews = FOREACH reviews GENERATE LOWER(text) AS text_lower, category;

-- TOKENIZE
words = FOREACH lower_reviews GENERATE FLATTEN(TOKENIZE(text_lower, ' ')) AS word, category;

-- REMOVE STOPWORD
joined_data = JOIN words BY word LEFT OUTER, stopwords BY stopword;
clean_words_full = FILTER joined_data BY stopwords::stopword IS NULL;
clean_words = FOREACH clean_words_full GENERATE words::word AS word, words::category AS category;
--clean_words : (word, category)

-- 5 TỪ LIÊN QUAN NHẤT THEO CATEGORY
group_word = GROUP clean_words BY (category, word);
count_word = FOREACH group_word GENERATE FLATTEN(group) AS (category, word), COUNT(clean_words) AS freq;
-- count_word : (word, category, freq)

group_category = GROUP count_word BY category;
top5 = FOREACH group_category {
    sorted = ORDER count_word BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE group AS category, top AS top_words;
}

STORE top5 INTO '/output_bai5' USING PigStorage(',');