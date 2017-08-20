student_Grades = LOAD ‘/ggonihdfs/schoolGrade.txt’ using PigStorage (‘\t’) as (StName: chararray, GrMonth1: int, GrMonth2: int,GrMonth3: int,GrMonth4: int,GrMonth5: int,GrMonth6: int,GrMonth7: int,GrMonth8: int,GrMonth9: int,GrMonth10: int,GrMonth11: int,GrMonth12: int);
 
filter_data = FILTER student_Grades BY GrMonth12 == 6;
 
DUMP filter_data;