from google.cloud import bigquery
import os
import pandas as pd


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="marketup-public-a0e625c7733e.json"
client = bigquery.Client()

# Perform a query
tree_query		 = ("SELECT * FROM `marketup-public.tree_census.tree_species` LIMIT 1000")
tree_census_2015 = ("SELECT * FROM `marketup-public.tree_census.tree_census_2015` LIMIT 1000")

query_tree_species = client.query(tree_query)  # API request
tree_species_table = query_tree_species.to_dataframe()
# print(tree_species_table)
# tree_species_table.to_excel("tree_species_table.xlsx")

query_tree_census_2015 = client.query(tree_census_2015)  # API request
tree_census_2015_table = query_tree_census_2015.to_dataframe()


for i, row in tree_census_2015_table.iterrows():
	if row["status"] != "Alive":
		tree_census_2015_table.drop(i, inplace = True)


tree_census_2015_table_root_stone = tree_census_2015_table[tree_census_2015_table["root_stone"] == "Yes"]
tree_census_2015_table_root_grate = tree_census_2015_table[tree_census_2015_table["root_grate"] == "Yes"]
tree_census_2015_table_root_other = tree_census_2015_table[tree_census_2015_table["root_other"] == "Yes"]

# tree_census_2015_table_root_stone.to_excel("tree_census_2015_table_root_stone.xlsx")
# tree_census_2015_table_root_grate.to_excel("tree_census_2015_table_root_grate.xlsx")
# tree_census_2015_table_root_other.to_excel("tree_census_2015_table_root_other.xlsx")

tree_census_2015_table_root_stone_json = tree_census_2015_table_root_stone.to_json()
tree_census_2015_table_root_grate_json = tree_census_2015_table_root_grate.to_json()
tree_census_2015_table_root_other_json = tree_census_2015_table_root_other.to_json()



tree_census_2015_table_root = tree_census_2015_table[(tree_census_2015_table["root_stone"] == "Yes") | \
	(tree_census_2015_table["root_grate"] == "Yes") | (tree_census_2015_table["root_other"] == "Yes")]

# tree_census_2015_table_root.to_excel("tree_census_2015_table_root.xlsx")
tree_census_2015_table_root_json = tree_census_2015_table_root.to_json()



tree_spc_root_occ = tree_census_2015_table_root["spc_common"].value_counts(ascending = False)

tree_spc_root_occ_json = tree_spc_root_occ.to_json()




print("DONE")




# tree_census_2015_table_root_stone_BQ = "marketup-public.tree_census.tree_census_2015_table_root_stone_json"
# tree_census_2015_table_root_grate_BQ = "marketup-public.tree_census.tree_census_2015_table_root_grate_json"
# tree_census_2015_table_root_other_BQ = "marketup-public.tree_census.tree_census_2015_table_root_other_json"
# tree_census_2015_table_root_BQ 		 = "marketup-public.tree_census.tree_census_2015_table_root_json"
# tree_spc_root_occ_BQ 				 = "marketup-public.tree_census.tree_spc_occ_root"

# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField("name", "STRING"),
#         bigquery.SchemaField("post_abbr", "STRING"),
#     ],
#     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
# )

# uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"

# load_job = client.load_table_from_uri(
#     uri,

#     tree_census_2015_table_root_stone_BQ,
#     tree_census_2015_table_root_grate_BQ,
#     tree_census_2015_table_root_other_BQ,
#     tree_census_2015_table_root_BQ,
#     tree_spc_root_occ_BQ,

#     location = "EU",  # Must match the destination dataset location.
#     job_config = job_config,
# )  # Make an API request.

# load_job.result()  # Waits for the job to complete.

# destination_table = client.get_table(table_id)
# print("Loaded {} rows.".format(destination_table.num_rows))




print("LOADED")




