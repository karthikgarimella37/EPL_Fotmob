# extract data for EPL
# first check if all matches and players are available
# dump the json data into gcp bucket using terraform
# use the json to insert the data into stg tables
# insert into dim and facts after dbt transformations
# else get latest match inserted and extract from there
# functions for each action
# create a df for each table, normalize it to insert it efficiently

from dotenv import load_dotenv
