# populate measurement table
# the measurement table is seeded from the source data
# however, it is not a 1 to 1 copy as 1 or 2 rows in the source data can be joined
# into a single row into the measurement table

FILE = 'source_data/nrg_d_hhq_linear.csv'
# need to open the file and then load each row
# need to do it row by row because some pre-processing is required
# candidate for optimisation in future