from pyspark.sql import DataFrame


def print_df(df: DataFrame, n: int = 10, fast: bool = False) -> DataFrame:
    """
    Print the first n rows of a DataFrame.

    Parameters:
    df (DataFrame): The DataFrame to print.
    n (int): The number of rows to print. Default is 10.
    fast (bool): If True, skips printing the total row count for faster output. Default is False.

    Returns:
    None
    """
    if not fast:
        count = df.count()
        if count < n:
            print(f"Printing {count}/{count} rows of the DataFrame:")
        else:
            print(f"Printing first {n}/{count} rows of the DataFrame:")

    df.show(n, truncate=False)
    return df


def count_df(df: DataFrame, columns: list[str], ascending: bool = False) -> DataFrame:
    """
    Group the DataFrame by the specified columns and count the number of rows for each group.

    Parameters:
    df (DataFrame): The DataFrame to group and count.
    columns (list[str]): The columns to group by.
    ascending (bool): Whether to sort the counts in ascending order. Default is False.

    Returns:
    DataFrame: A DataFrame with the group columns and their corresponding counts, sorted by count.
    """
    return df.groupBy(columns).count().orderBy("count", ascending=ascending)


def save_df(df: DataFrame, table_name: str, partition_column: str = "") -> DataFrame:
    """
    Save the DataFrame to a table in the Spark catalog.

    Parameters:
    df (DataFrame): The DataFrame to save.
    table_name (str): The name of the table to save the DataFrame to.
    partition_column (str): Optional; if provided, partitions the table by this column.

    Returns:
    DataFrame: The saved DataFrame.
    """
    if partition_column:
        df.write.mode("overwrite").option("overwriteSchema", "true").partitionBy(
            partition_column
        ).saveAsTable(table_name)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

    return spark.table(table_name)
