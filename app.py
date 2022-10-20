import pandas
from datetime import datetime

input_path = 'oc-stats-analytics.csv'
output_path = 'output_data.csv'

def transformColumnName(colName):
    l=colName.split(':')[1].split('.')
    return l[5]+'_'+l[3]

try:
    dataframe = pandas.read_csv(input_path, header = 0, index_col = 0)
    if dataframe.size==0: raise Exception('The input file has no records')
    listColNames=list(map(transformColumnName,dataframe.columns.to_series()))
    dataframe.columns=pandas.Index(listColNames)
    dataframe = dataframe.transpose()
    lagSeries = dataframe.stack()
    datetimeSeries=lagSeries.index.to_series().apply(lambda x: datetime.fromtimestamp(x[1]//1000))
    transformedDf = pandas.DataFrame({"Datetime": datetimeSeries, "Lag": lagSeries})
    transformedDf.index = transformedDf.index.reorder_levels(order = [1,0])
    transformedDf.index.set_names(['Timestamp','Partition'], inplace=True)
    transformedDf.to_csv(output_path)
except pandas.errors.EmptyDataError:
    print('The input file is empty')
except Exception as e:
    print(e)
