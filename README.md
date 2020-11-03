# PySpark-Algorithms

# Problem 1
## Given: A sample of 500 integers from uniform distribution. Integers should go from 0 to 50. 
## Task: Filter and print the values that repeat more than 10 times on RDD.

#### 500 Uniform Distributed Integers

[5, 8, 30, 43, 30, 29, 29, 5, 8, 34, 29, 38, 12, 16, 12, 22, 36, 4, 48, 35, 10, 22, 25, 18, 14, 31, 36, 35, 25, 34, 16, 41, 44, 49, 42, 4, 25, 23, 5, 50, 47, 12, 26, 28, 21, 2, 0, 7, 47, 17, 22, 39, 42, 42, 0, 16, 15, 1, 35, 50, 8, 1, 0, 37, 50, 44, 49, 12, 38, 45, 45, 24, 24, 2, 43, 2, 35, 32, 44, 17, 20, 11, 41, 3, 32, 6, 43, 35, 35, 2, 26, 35, 31, 0, 22, 16, 33, 1, 19, 35, 24, 18, 2, 14, 37, 6, 0, 17, 33, 30, 9, 24, 45, 8, 34, 40, 19, 41, 1, 40, 50, 31, 42, 4, 48, 11, 13, 26, 17, 31, 42, 16, 46, 15, 20, 42, 14, 29, 11, 7, 45, 50, 39, 36, 37, 36, 3, 4, 21, 29, 43, 0, 43, 47, 33, 48, 34, 35, 13, 48, 38, 14, 16, 27, 3, 13, 40, 16, 22, 13, 25, 18, 5, 5, 24, 35, 47, 9, 45, 40, 11, 31, 12, 4, 46, 4, 19, 47, 17, 45, 34, 0, 15, 44, 27, 27, 10, 0, 7, 15, 29, 36, 11, 31, 2, 17, 8, 8, 45, 29, 30, 37, 44, 27, 30, 1, 26, 41, 23, 45, 0, 36, 20, 35, 49, 4, 26, 37, 48, 14, 39, 3, 25, 19, 49, 37, 10, 38, 22, 2, 40, 42, 28, 34, 50, 47, 19, 36, 49, 46, 22, 21, 27, 25, 11, 49, 44, 47, 43, 5, 44, 8, 32, 6, 4, 12, 3, 13, 29, 48, 37, 13, 19, 8, 28, 9, 39, 12, 20, 0, 20, 13, 37, 38, 21, 39, 34, 21, 9, 36, 7, 50, 1, 26, 35, 15, 18, 14, 17, 11, 38, 12, 13, 45, 29, 28, 30, 32, 40, 11, 16, 10, 41, 50, 23, 3, 4, 34, 48, 46, 21, 35, 19, 8, 28, 32, 33, 19, 48, 37, 48, 38, 41, 33, 38, 9, 4, 26, 32, 38, 49, 36, 50, 42, 29, 15, 29, 38, 12, 16, 33, 4, 39, 34, 39, 27, 20, 14, 18, 10, 11, 3, 6, 11, 20, 33, 28, 42, 15, 6, 31, 8, 15, 47, 28, 8, 6, 40, 26, 36, 30, 32, 29, 1, 24, 17, 31, 4, 16, 6, 50, 45, 27, 30, 24, 50, 29, 35, 11, 34, 16, 32, 37, 10, 31, 14, 8, 49, 5, 4, 48, 23, 23, 46, 35, 41, 46, 1, 40, 34, 13, 40, 44, 41, 35, 6, 50, 34, 14, 3, 37, 23, 49, 28, 46, 12, 6, 44, 41, 30, 17, 42, 45, 13, 30, 23, 0, 35, 26, 3, 50, 6, 47, 13, 38, 41, 41, 34, 44, 33, 24, 43, 9, 9, 22, 16, 49, 27, 34, 4, 0, 27, 45, 18, 4, 8, 26, 35, 40, 48, 13, 2, 17, 29, 48, 11, 3, 47, 40, 25, 18, 41, 49, 8, 47, 16, 39, 12, 45, 40]


## Solution

I applied a step solution to solve this problem. First I aplied a map and reduce to get total counts of the random numbers. After geting key value pairs of number with frequency, I apply a filter to get key value pairs with value larger than 10.

#### Map and Reduce
8, 14), (16, 13), (48, 12), (0, 12), (24, 8), (32, 8), (40, 12), (25, 7), (41, 12), (49, 11), (17, 10), (1, 8), (33, 8), (9, 7), (34, 14), (10, 6), (18, 7), (42, 10), (50, 13), (26, 10), (2, 8), (43, 7), (35, 18), (11, 12), (3, 10), (19, 8), (27, 9), (12, 11), (36, 10), (4, 15), (44, 10), (28, 8), (20, 7), (5, 7), (29, 14), (21, 6), (37, 11), (45, 13), (13, 12), (30, 10), (38, 11), (22, 8), (14, 9), (6, 10), (46, 7), (31, 9), (23, 7), (47, 11), (7, 4), (39, 8)

#### Applying a Filter on RDD

The output of the Filter will give the desired result.

(17, 10), (42, 10), (26, 10), (3, 10), (36, 10), (44, 10), (30, 10), (6, 10), (49, 11), (12, 11), (37, 11), (38, 11), (47, 11), (48, 12), (0, 12), (40, 12), (41, 12), (11, 12), (13, 12), (16, 13), (50, 13), (45, 13), (8, 14), (34, 14), (29, 14), (4, 15), (35, 18)


# Problem 2
## Given:As input file "THE HISTORY OF THE UNITED STATES BY CHARLES A. BEARD" 
## Task: Print the top 30 most frequent words and their corresponding frequencies

#### Sample of Input File

'HISTORY', '', 'OF THE', '', 'UNITED STATES', '', '', 'BY', '', '', 'CHARLES A. BEARD', '', 'AND', '', 'MARY R. BEARD', '', '', '', 'New York', '', 'THE MACMILLAN COMPANY', '', '1921', '', '_All rights reserved_', '', 'COPYRIGHT, 1921,', '', 'BY THE MACMILLAN COMPANY.', '', '', 'Set up and electrotyped. Published March, 1921.', '', '', '', '', 'Norwood Press', '', 'J.S. Cushing Co.--Berwick & Smith Co.', '', 'NORWOOD, MASS., U.S.A.', '', '', '', '', 'PREFACE', '', '', 'As things now stand, the course of instruction in American history in', 'our public schools embraces three distinct treatments of the subject.', 'Three separate books are used. First, there is the primary book, which', 'is usually a very condensed narrative with emphasis on biographies and', 'anecdotes. Second, there is the advanced text for the seventh or eighth', 'grade, generally speaking, an expansion of the elementary book by the', 'addition of forty or fifty thousand words.


## Solution 

Similary, to the previous answer. I applied a map-reduce to get word and its frequency. I then sorted by value in ascending order and used "take" to get first 30 key value pairs.

#### Map Reduce Operation

Note this is just a sample of total words in the book.

'HISTORY': 3, 'OF': 122, 'UNITED': 13, 'A.': 18, 'R.': 8, 'New': 432, '1921': 3, '_All': 1, 'Set': 1, 'electrotyped.': 1, '1921.': 3, 'Cushing': 1, 'Co.--Berwick': 1, 'Smith': 4, 'NORWOOD,': 1, 'MASS.,': 1, 'PREFACE': 2, 'things': 64, 'now': 260, 'of': 15269, 'in': 7212, 'American': 644, 'public': 190, 'schools': 5, 'three': 140, 'treatments': 1, 'subject.': 6, 'Three': 15, 'books': 12, 'are': 712, 'there': 431, 'is': 1754, 'primary': 25, 'book,': 5, 'very': 244, 'narrative': 5, 'anecdotes.': 1, 'Second,': 1, 'seventh': 1, 'grade,': 2, 'speaking,': 10, 'an': 1079, 'book': 20, 'forty': 20, 'thousand': 101, 'Finally,': 12, 'high': 129, 'school': 16, 'manual.': 1, 'This,': 5, 'ordinarily': 4, 'follows': 5, 'beaten': 14, 'path,': 1, 'fuller': 2, 'accounts': 5, 'characters.': 1, 'put': 126, 'we': 256, 'do': 238, 'obtain': 13, 'permanent': 23,

## Final Result

Since preprocessing and parsing was not perform, it is expected to see the most common words to be stop-words.

('the', 26393), ('of', 15269), ('and', 11197), ('to', 8931), ('in', 7212), ('a', 6236), ('was', 3860), ('that', 2877), ('with', 2607), ('by', 2527), ('his', 2442), ('for', 2357), ('as', 2341), ('The', 2336), ('he', 2141), ('had', 2038), ('on', 1947), ('were', 1913), ('it', 1782), ('is', 1754), ('be', 1750), ('at', 1724), ('not', 1621), ('from', 1612), ('their', 1440), ('or', 1431), ('which', 1234), ('an', 1079), ('they', 1060), ('but', 1051)

