-- Load data
reviews = LOAD '/hotel-review' USING PigStorage(';') 
          AS (id:int, text:chararray, category:chararray, aspect:chararray, sentiment:chararray);


-- KHÍA CẠNH NHẬN NHIỀU ĐÁNH GIÁ TIÊU CỰC NHẤT
neg_reviews = FILTER reviews BY sentiment == 'negative';
grouped_neg = GROUP neg_reviews BY aspect;
count_neg = FOREACH grouped_neg GENERATE group AS aspect_name, COUNT(neg_reviews) AS total_neg;

sorted_neg = ORDER count_neg BY total_neg DESC;
top_negative_aspect = LIMIT sorted_neg 1;
STORE top_negative_aspect INTO '/output_bai3/top_negative' USING PigStorage(',');


-- KHÍA CẠNH NHẬN NHIỀU ĐÁNH GIÁ TÍCH CỰC NHẤT
pos_reviews = FILTER reviews BY sentiment == 'positive';
grouped_pos = GROUP pos_reviews BY aspect;
count_pos = FOREACH grouped_pos GENERATE group AS aspect_name, COUNT(pos_reviews) AS total_pos;

sorted_pos = ORDER count_pos BY total_pos DESC;
top_positive_aspect = LIMIT sorted_pos 1;
STORE top_positive_aspect INTO '/output_bai3/top_positive' USING PigStorage(',');