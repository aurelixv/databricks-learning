from app.functions.helper_functions import get_spark
from app.functions.df_functions import print_df, count_df

spark = get_spark()

print(f"Catalog: {spark.catalog.currentCatalog()}")
print(f"Database: {spark.catalog.currentDatabase()}")
print(f"Spark Version: {spark.version}")

df = spark.table("samples.bakehouse.sales_suppliers")

# Print the first 10 rows of the DataFrame
df.transform(print_df, fast=True)

# Count the number of rows grouped by 'supplierId' and print the first 100 rows
df.transform(count_df, ["supplierId"]).transform(print_df, 100)

# Count the number of rows grouped by 'supplierId' and sort by count, then print the first 100 rows
df.transform(count_df, ["supplierId"]).transform(count_df, ["count"]).transform(
    print_df, 100
)
