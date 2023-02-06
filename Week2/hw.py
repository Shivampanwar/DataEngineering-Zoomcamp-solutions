import pandas as pd

# df = pd.read_csv("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz")
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-04.csv.gz"
df =  pd.read_csv(url)
print (df.shape)