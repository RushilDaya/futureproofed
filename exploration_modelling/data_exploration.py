import pandas as pd 

FILE_PATH = 'source_data/nrg_d_hhq_linear.csv'

# Read in the data
df = pd.read_csv(FILE_PATH)
print(df.head())

# show the columns
print(df.columns)
print(df.shape)
# show for each column the number of unique values
print(df.nunique())

'''
DATAFLOW, LAST_UPDATE, feq only have one unique value: not important for us
nrg_bal, siec, unit, geo, TIME_PERIOD  have small numbers of values: catagorical data
OBS_VALUE: >16 thousand values: numerical data
OBS_FLAG: 0 values (all NaN): not important for us
'''
# for the catagorical data - determine the catagories
print("nrg_bal: ", df['nrg_bal'].unique())
print("siec: ", df['siec'].unique())
print("unit: ", df['unit'].unique())
print("geo: ", df['geo'].unique())
print("TIME_PERIOD: ", df['TIME_PERIOD'].unique())

"""
nrg_bal:  ['FC_OTH_HH_E' 'FC_OTH_HH_E_CK' 'FC_OTH_HH_E_LE' 'FC_OTH_HH_E_OE'
 'FC_OTH_HH_E_SC' 'FC_OTH_HH_E_SH' 'FC_OTH_HH_E_WH']
siec:  ['E7000' 'G3000' 'G3200' 'H8000' 'O4000' 'O4669' 'O4671'
 'R5110-5150_W6000RI' 'R5300' 'RA000' 'RA410' 'RA600' 'SFF_P1000_S2000'
 'TOTAL' 'O4630']
unit:  ['GWH' 'TJ' 'TJ_GCV' 'THS_T']
geo:  ['AL' 'AT' 'BA' 'BE' 'BG' 'CY' 'CZ' 'DE' 'DK' 'EA20' 'EE' 'EL' 'ES'
 'EU27_2020' 'FI' 'FR' 'GE' 'HR' 'HU' 'IE' 'IT' 'LT' 'LU' 'LV' 'MD' 'MK'
 'MT' 'NL' 'NO' 'PL' 'PT' 'RO' 'RS' 'SE' 'SI' 'SK' 'UA' 'UK' 'XK']
TIME_PERIOD:  [2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021]

TIME_PERIOD: easy to interpret
geo: easy to interpret
unit: GWH, TJ, TJ_GCV, THS_T: GWH = gigawatt hour, TJ = terajoule, TJ_GCV = terajoule gross calorific value, THS_T = thousand tonnes of oil equivalent
    need to confirm if these units can be converted to each other (available online)
nrg_bal: uncertain what this represents -  must find mapping (available online)
siec: uncertain what this represents - must find mapping (available online)
"""

# group by nrg_bal, siec, unit, geo, TIME_PERIOD and count how many values there are for OBS_VALUE
grouped_by_candidate_key = df.groupby(['nrg_bal', 'siec', 'unit', 'geo', 'TIME_PERIOD'])['OBS_VALUE']
print(grouped_by_candidate_key.count().unique())

"""
when combined  we see that the the candidate key does provide a unique value for each value
it is possible that the real key is however some shorter combination of these columns
"""
# determine the prevelance of null values for each column
print(df.isnull().sum())
"""
DATAFLOW           0
LAST UPDATE        0
freq               0
nrg_bal            0
siec               0
unit               0
geo                0
TIME_PERIOD        0
OBS_VALUE          0
OBS_FLAG       27899

none of the columns have null values except for OBS_FLAG, this means we
can consider our candidate keys as non-nullable
"""

"""
hypothesis is that unit should not be included in the key as it seems illogical that
you can have multiple measured units for the same value. investigate this a bit
"""
# group by the catagorical values besides unit and see how many unique values there are for OBS_VALUE and unit
grouped_by_candidate_key = df.groupby(['nrg_bal', 'siec', 'geo', 'TIME_PERIOD'])
# filter out groups which only have one unique value for unit
filtered_out_unique_unit_cases = grouped_by_candidate_key.filter(lambda x: x['unit'].nunique() > 1)
non_unique_unit_cases = filtered_out_unique_unit_cases.groupby(['nrg_bal', 'siec', 'geo', 'TIME_PERIOD'])

counter = 0
for group_id, group_df in non_unique_unit_cases:
    print('******')
    print(group_df[['OBS_VALUE', 'unit']])
    counter += 1
    if counter > 10:
        break
"""
******
     OBS_VALUE unit
0      2997.45  GWH
338   10790.82   TJ
******
     OBS_VALUE unit
1     2936.634  GWH
339  10571.882   TJ
******
     OBS_VALUE unit
2     3065.799  GWH
340  11036.876   TJ
******
     OBS_VALUE unit
3     4066.970  GWH
341  14641.092   TJ
******
     OBS_VALUE unit
4     3500.001  GWH
342  12600.004   TJ
******
     OBS_VALUE unit
5     3240.375  GWH
343  11665.350   TJ
******
     OBS_VALUE unit
6     2990.930  GWH
344  10767.348   TJ
******
     OBS_VALUE unit
7     2934.535  GWH
345  10564.326   TJ
******
     OBS_VALUE unit
8     3146.976  GWH
346  11329.114   TJ
******
     OBS_VALUE unit
9     3120.242  GWH
347  11232.871   TJ
******
     OBS_VALUE unit
10    3469.079  GWH
348  12488.684   TJ

In this case the 2 values appear as duplicates of each other -> if we apply the unit conversion
between GWH and TJ we see this is a duplication in the data
Would thus not include unit in the key - rather convert all data to a single unit
should also see if this applies  to all the combinations of units
"""
all_unit_pairs = set()
for group_id, group_df in non_unique_unit_cases:
    units = group_df['unit'].unique()
    unit_pair = "|".join(units)
    all_unit_pairs.add(unit_pair)
print(all_unit_pairs)
"""
{'GWH|TJ', 'TJ|TJ_GCV', 'THS_T|TJ'}
see three combinations of units should validate if the pattern hold for all 3 cases
"""
counter_gcv = 0
counter_ths = 0
for group_id, group_df in non_unique_unit_cases:
    units = group_df['unit'].unique()
    unit_pair = "|".join(units)
    if unit_pair == 'TJ|TJ_GCV':
        counter_gcv += 1
        if counter_gcv <= 10:
            print(group_df[['OBS_VALUE', 'unit']])
    if unit_pair == 'THS_T|TJ':
        counter_ths += 1
        if counter_ths <= 10:
            print(group_df[['OBS_VALUE', 'unit']])
    if counter_gcv > 10 and counter_ths > 10:
        break
"""
     OBS_VALUE    unit
676  65468.974      TJ
990  72743.304  TJ_GCV
     OBS_VALUE    unit
677  58178.113      TJ
991  64642.348  TJ_GCV
     OBS_VALUE    unit
678  59220.172      TJ
992  65800.191  TJ_GCV
     OBS_VALUE    unit
679  59831.004      TJ
993  66478.893  TJ_GCV
     OBS_VALUE    unit
680  52399.369      TJ
994  58221.521  TJ_GCV
     OBS_VALUE    unit
681  56839.082      TJ
995  63154.536  TJ_GCV
     OBS_VALUE    unit
682  62819.067      TJ
996  69798.963  TJ_GCV
     OBS_VALUE    unit
683  61496.238      TJ
997  68329.153  TJ_GCV
     OBS_VALUE    unit
684  56280.632      TJ
998  62534.036  TJ_GCV
     OBS_VALUE    unit
685  59719.332      TJ
999  66354.813  TJ_GCV
      OBS_VALUE   unit
1304    105.015  THS_T
1317   4830.690     TJ
      OBS_VALUE   unit
1305      51.88  THS_T
1318    2386.48     TJ
      OBS_VALUE   unit
1306       69.0  THS_T
1319     3174.0     TJ
      OBS_VALUE   unit
1307       25.0  THS_T
1320     1150.0     TJ
      OBS_VALUE   unit
1308    394.440  THS_T
1321  17991.088     TJ
      OBS_VALUE   unit
1309    285.740  THS_T
1322  13059.287     TJ
      OBS_VALUE   unit
1310     297.27  THS_T
1323   13579.85     TJ
      OBS_VALUE   unit
1311    299.920  THS_T
1324  13696.612     TJ
      OBS_VALUE   unit
1312    213.130  THS_T
1325   9718.682     TJ
      OBS_VALUE   unit
1313    204.720  THS_T
1326   9332.801     TJ

The values do not seem related by a constant factor.
unit conversion thus does not seem possible / or at least it is not trivial
this contradicts the simpler case of GWH and TJ implying that multiple values
may be possible for a unit. 

However, it may be interesting to not that TJ is always provided in the pair cases
So last thing need to check is if for everytime we have a non TJ unit we also have a TJ unit
if that is the case it simplifies the analysis
"""

grouped_by_candidate_key = df.groupby(['nrg_bal', 'siec', 'geo', 'TIME_PERIOD'])
# filter out groups which only have one unique value for unit
filtered_out_non_unique_unit_cases = grouped_by_candidate_key.filter(lambda x: x['unit'].nunique() == 1)
print(filtered_out_non_unique_unit_cases['unit'].unique())
"""
['TJ' 'THS_T'] this implies an issue: should consider how common THS_T on its own is
"""
filtered_out_non_unique_unit_cases = grouped_by_candidate_key.filter(lambda x: (x['unit'].nunique() == 1) and (x['unit'].unique()[0]=='THS_T'))
print(filtered_out_non_unique_unit_cases['unit'].shape)
"""
1252 cases seem to have this case, so it is not a rare occurence
"""

