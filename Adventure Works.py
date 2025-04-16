import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo
    import ibis
    return ibis, mo


@app.cell
def sql_settings(ibis):
    sqlserver_host = "localhost"
    sqlserver_db = "AdventureWorksDW2020"
    sqlserver_user = "mcosta"
    sqlserver_pass = "123"
    sqlserver_port = "1433"
    sqlserver_driver = "SQL Server"

    con = ibis.mssql.connect(
        user=sqlserver_user,
        password=sqlserver_pass,
        host=sqlserver_host,
        database=sqlserver_db,
        driver=sqlserver_driver,
        port=sqlserver_port,
    )
    return (
        con,
        sqlserver_db,
        sqlserver_driver,
        sqlserver_host,
        sqlserver_pass,
        sqlserver_port,
        sqlserver_user,
    )


@app.cell
def db_tables():
    table_date = "DimDate"
    table_resellersales = "FactResellerSales"
    table_internetsales = "FactInternetSales"
    return table_date, table_internetsales, table_resellersales


@app.cell
def table_queries(con, table_date):
    query_table_date = con.sql(f'SELECT * FROM {table_date}').execute()
    return (query_table_date,)


@app.cell
def _(query_table_date):
    query_table_date
    return


@app.cell
def _(con, table_date, table_internetsales, table_resellersales):
    internet_sales = con.sql(f"""SELECT SUM(i.SalesAmount) AS TotalSales
    FROM {table_internetsales} AS i
    JOIN {table_date} AS d
    ON i.OrderDateKey = d.DateKey
    WHERE d.FiscalYear = 2020
    """).execute()

    reseller_sales = con.sql(f"""SELECT SUM(i.SalesAmount) AS TotalSales
    FROM {table_resellersales} AS i
    JOIN {table_date} AS d
    ON i.OrderDateKey = d.DateKey
    WHERE d.FiscalYear = 2020
    """).execute()

    internet_sales + reseller_sales
    return internet_sales, reseller_sales


if __name__ == "__main__":
    app.run()
