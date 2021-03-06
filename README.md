# Book-Recommendation-System
## Abstract ##
Book Recommendation Systems are widely popular these days and are used by almost all book sellers. Not only are they beneficial for the bibliophiles, but also for the book sellers for a myriad of reasons. A good book recommendation system is a key factor in increasing the sales of a book seller as it increases the visibility of their books and creates a better customer experience. In this project, we aim to build a Book Recommendation System based on the UCSD Goodreads dataset. We also aim to gain valuable insights from the data which can be used for finding the reading trends and publishing trends for a particular year. The tools and technologies used in this project are MapReduce to clean the datasets and Hive to produce relevant resultsin the form of trends and recommendations.

## Introduction ##
A book recommendation system is important for customer engagement since the more customers read, the more revenue they generate. Undoubtedly, bibliophils are the primary beneficiaries of book recommendation systems. Other beneficiaries include agencies and publishing houses. The better the recommendation system that a book seller uses, the better is their customer engagement and experience. Moreover, such recommendation systems are a key to increasing the visibility of books in book stores, whether online or offline. Online book sellers such as Kindle, Amazon, Goodreads, etc. compete on many factors and the success is majorly dependent on the goodness of their recommendation systems. Motivated by this fact, we decided to build a book recommendation system for this project.

There are many publicly available datasets online on websites such as Kaggle, Google Datasets, etc. We have chosen the UCSD Goodreads dataset for implementing this project, because it has comprehensive information about the books, authors, genres and user-book interactions. The size of this dataset is ~32 GB and the dataset contains more than 230 million interactions, which is excellent for building a good recommendation system. In this project, we aim to provide two types of results - a robust Book Recommendation System, and some insights into reading and publishing trends among the users. While recommending books to a user, we consider user's own ratings, similar user ratings, genre, number of ratings, and negatively reviewed books.

The Book Recommendation System will suggest books to the user based on -

1. top-rated books similar to the ones the user has rated the highest
2. top-rated books in a genre the user has rated the highest
3. top-rated books by other users who have read similar books as this particular user

Apart from recommendations, we also perform some analytics on the data to find the reading and publishing trends among the users. We have observed some interesting trends, which are mentioned in the Results section. The detailed process of obtaining recommendations is described in the Design Diagram and Results sections.

## Architecture ##
https://drive.google.com/file/d/1REJN5ucKlivvXSeLQTe6Yy0Xd8m7WKou/view?usp=sharing
