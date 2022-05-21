select t3.* from books t3 where ratings_count > 2133.88 and t3.book_id in (
select t2.similar_book_id from similar_books t2 where t2.book_id in (
select t1.book_id from interactions t1 where t1.user_id='f7fe5196ae6a346eb1c1e00a21d5693c' and t1.rating in (
select max(t0.rating) from interactions t0 where t0.user_id='f7fe5196ae6a346eb1c1e00a21d5693c'))) order by t3.ratings_count desc limit 20;




select distinct t4.* from books t4 join (
select t2.similar_book_id from similar_books t2 join (
select t1.book_id from interactions t1 where t1.user_id='f7fe5196ae6a346eb1c1e00a21d5693c' and t1.rating in (
select max(t0.rating) from interactions t0 where t0.user_id='f7fe5196ae6a346eb1c1e00a21d5693c')) t3
on t2.book_id=t3.book_id ) t5
on t4.book_id=t5.similar_book_id where ratings_count > 2133.88 order by t4.average_rating desc limit 20;




select t1.publication_year, t2.genre, count(distinct t1.book_id) c from books t1 join genres_new t2
on t1.book_id = t2.book_id where t1.publication_year >= 1960 and t1.publication_year <= 2017 group by t1.publication_year, t2.genre
order by t1.publication_year, c



select t1.publication_year, t2.genre, count(distinct t1.book_id) c from books t1 join genres_new t2
on t1.book_id = t2.book_id where t1.publication_year >= 1960 and t1.publication_year <= 2017 group by t1.publication_year, t2.genre
order by t1.publication_year, c


select t1.book_id book_id, genre, average_rating, substr(title, 1, 10) title from books t1 left join genres_new t2
on t1.book_id=t2.book_id where ratings_count > 2133.88 order by average_rating desc limit 20;


select distinct t1.* from publication_trends t1 join (
select publication_year c1, max(count) c2 from publication_trends group by publication_year) t2
on t1.publication_year = t2.c1 and t1.count = t2.c2



select distinct t4.book_id, t6.genre, t4.average_rating, substr(t4.title, 1, 50) title  from books t4 join (
select t2.similar_book_id from similar_books t2 join (
select t1.book_id from interactions t1 where t1.user_id='f7fe5196ae6a346eb1c1e00a21d5693c' and t1.rating in (
select max(t0.rating) from interactions t0 where t0.user_id='f7fe5196ae6a346eb1c1e00a21d5693c')) t3
on t2.book_id=t3.book_id ) t5 on t4.book_id=t5.similar_book_id
join genres_new t6 on t4.book_id=t6.book_id where ratings_count > 2133.88 order by t4.average_rating desc limit 20;


select t1.book_id book_id, genre, average_rating, substr(title, 1, 50) from books t1 left join genres_new t2
on t1.book_id=t2.book_id where ratings_count > 2133.88 and genre='children' order by average_rating desc limit 20;


select distinct t1.book_id book_id, t2.genre genre, t3.average_rating average_rating, substr(t3.title, 1, 50) title
from interactions t1 join genres_new t2 on t1.book_id = t2.book_id join books t3 on t1.book_id = t3.book_id
where t1.rating in (select max(rating) from interactions where user_id='f7fe5196ae6a346eb1c1e00a21d5693c')
and t3.ratings_count > 2133.88 and t2.genre != '' order by t3.average_rating desc limit 20;



select t1.read_at_year, t2.genre, count(distinct t3.book_id, t3.user_id) from reviews t1 join genres_new t2 on t1.book_id = t2.book_id
join interactions t3 on t1.book_id = t3.book_id
group by t1.read_at_year, t2.genre


select distinct t1.* from reading_trends t1 join (
select read_at_year c1, max(count) c2 from reading_trends group by read_at_year) t2
on t1.read_at_year = t2.c1 and t1.count = t2.c2 where t1.read_at_year >= 1960 and t1.read_at_year <= 2017;