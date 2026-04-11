-- Thong ke tan so xuat hien va chi ra cac tu > 500 lan
data = LOAD '/output/tokenize' USING PigStorage('\t') AS (
    id : int,
    word: chararray,
    category: chararray,
    aspect: chararray,
    sentiment : chararray
);

grouped_data = GROUP data BY word; -- output : ( khach hang, ({},{},{}) ) ,
word_count = FOREACH grouped_data GENERATE group AS word, COUNT(data) AS total_count;

freq_word = FILTER word_count BY total_count > 500;
STORE freq_word INTO '/output_bai2/count' USING PigStorage(':');

reviews = LOAD '/hotel-review' USING PigStorage(';') 
          AS (id:int, text:chararray, category:chararray, aspect:chararray, sentiment:chararray);


-- Thong ke so luong binh luan theo category
grouped_category = GROUP reviews BY category;
count_category = FOREACH grouped_category GENERATE group AS category_name, COUNT(reviews) AS total_comments;

sorted_category = ORDER count_category BY total_comments DESC;
STORE sorted_category INTO '/output_bai2/category' USING PigStorage(':');


-- Thong ke so luong binh luan theo aspect
grouped_aspect = GROUP reviews BY aspect;
count_aspect = FOREACH grouped_aspect GENERATE group AS aspect_name, COUNT(reviews) AS total_comments;

sorted_aspect = ORDER count_aspect BY total_comments DESC;
STORE sorted_aspect INTO '/output_bai2/aspect' USING PigStorage(':');

