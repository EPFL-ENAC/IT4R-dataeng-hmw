Rational behind the data pipeline
---------------------------------

File is much messier than I would have liked for it to be, as I ran out of time with failed tests.

The idea of the pipeline was to:
1. Have a function that extract data from the excel, and saves it for subsequent use. This includes
  assuming *some* structure on the excel files (sheet names, for example), and that the data, after
  cleaning the nans and missing fields will play nicely together
2. Ingestion of csv/json data, to identify matches with the database. This required having column names and table names
  in the database as close as possible to the fields in the excel and csvs.
3. Processing of the name of the files and folder. This puzzled me a bit, as some of the information is already available
  in the metadata. The only real info I needed was the test file to extract from the test csv. This leads to semplification 
  in the handling of the files, but I could have used it as a kind of sanity check for the data. But, as the assignment 
  asks for robustness vs errors, I thoght file name were to be considered less reliable
4. Data conversion. The data types from the pandas can be safely taken from the database, provided that no other colum types are 
  added. This could become less maintainable if it happens, as i deal with it in different part of the codes
5. Separation of concerns. I should have separated the data convertion in more subroutine. In particular, I did not implement
  any more date checking than the provided one, as I ran out of time with these. But it should be separated, and potentially enhanced
6. Error handling. Sadly, I did not put any error handling for the user to know what goes wrong in case of non-ingestion of data. Should 
  rely on the database errors, but also think of failing points in the code
7. As it was asked not to modify the fastapi implementation, I tested the code in the **../insertion_test** jupyter notebook. 
8. Design: In real life, it would probably be easier to go by file, rather than by directory. The proposed approach, anyway, 
  has the advantage of processing together both the test files and the metafile

Other considerations: I did not perform any code linting, nor added tests. These would be important missed aspects in the project
