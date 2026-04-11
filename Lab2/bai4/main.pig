-- Load data
reviews = LOAD '/hotel-review' USING PigStorage(';') 
          AS (id:int, text:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '/stopwords' AS (stopword:chararray);

-- LOWERCASE
lower_reviews = FOREACH reviews GENERATE LOWER(text) AS text_lower, category, sentiment;

-- TOKENIZE
words = FOREACH lower_reviews GENERATE FLATTEN(TOKENIZE(text_lower, ' ')) AS word, category, sentiment;

-- REMOVE STOPWORD
joined_data = JOIN words BY word LEFT OUTER, stopwords BY stopword;
clean_words_full = FILTER joined_data BY stopwords::stopword IS NULL;
clean_words = FOREACH clean_words_full GENERATE words::word AS word, words::category AS category, words::sentiment AS sentiment;

-- 5 tu mang y nghia tich cuc nhat
pos_words = FILTER clean_words BY sentiment  == 'positive';

group_pos_word = GROUP pos_words BY (category, word);
count_pos_word = FOREACH group_pos_word GENERATE FLATTEN(group) AS (category, word), COUNT(pos_words) AS freq;      
-- output : (HOTEL, "sạch", 150), (SERVICE, "nhiệt tình", 200).
group_pos_category = GROUP count_pos_word BY category;

top5_pos = FOREACH group_pos_category {
    sorted_pos = ORDER count_pos_word BY freq DESC;
    top_pos = LIMIT sorted_pos 5;
    GENERATE group AS category, top_pos AS top_positive_words;                                                                                                                                                                                                                                                      
}                                                           

STORE top5_pos INTO '/output_bai4/top5_positive' USING PigStorage(',');


-- 5 tu mang y nghia tieu cuc nhat
neg_words = FILTER clean_words BY sentiment == 'negative';

group_neg_word = GROUP neg_words BY (category, word);
count_neg_word = FOREACH group_neg_word GENERATE FLATTEN(group) AS (category, word), COUNT(neg_words) AS freq;
group_neg_category = GROUP count_neg_word BY category; 

top5_neg = FOREACH group_neg_category {                                             
    sorted_neg = ORDER count_neg_word BY freq DESC;
    top_neg = LIMIT sorted_neg 5;
    GENERATE group AS category, top_neg AS top_negative_words;
}

STORE top5_neg INTO '/output_bai4/top5_negative' USING PigStorage(',');