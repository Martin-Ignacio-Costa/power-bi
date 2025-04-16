import marimo

__generated_with = "0.12.10"
app = marimo.App(width="medium")


@app.cell
def imports():
    import os
    import marimo as mo
    import ibis
    return ibis, mo, os


@app.cell
def sql_settings(ibis, os):
    con = ibis.mssql.connect(
        user=os.environ["SQLSERVER_USER"],
        password=os.environ["SQLSERVER_PASS"],
        host=os.environ["SQLSERVER_HOST"],
        database=os.environ["SQLSERVER_DB"],
        driver="SQL Server",
        port=os.environ["SQLSERVER_PORT"],
    )
    return (con,)


@app.cell
def db_tables():
    table_date = "DimDate"
    table_resellersales = "FactResellerSales"
    table_internetsales = "FactInternetSales"
    return table_date, table_internetsales, table_resellersales


@app.cell
def quick_queries(con, table_date, table_internetsales, table_resellersales):
    qq_table_date = con.sql(f"SELECT * FROM {table_date}").execute()
    qq_table_resellersales = con.sql(
        f"SELECT * FROM {table_resellersales}"
    ).execute()
    qq_table_internetsales = con.sql(
        f"SELECT * FROM {table_internetsales}"
    ).execute()
    return qq_table_date, qq_table_internetsales, qq_table_resellersales


@app.cell
def inputs(mo):
    input_fiscal_year = mo.ui.dropdown(
        options={
            "FY2018": "2018",
            "FY2019": "2019",
            "FY2020": "2020",
        },
        value="FY2018",
        label="Fiscal Year: ",
    )
    return (input_fiscal_year,)


@app.cell
def _(input_fiscal_year):
    input_fiscal_year
    return


@app.cell
def _(
    con,
    input_fiscal_year,
    table_date,
    table_internetsales,
    table_resellersales,
):
    internet_sales = con.sql(f"""SELECT ROUND(SUM(SalesAmount), 0) AS TotalInternetSales
    FROM {table_internetsales}
    JOIN {table_date}
    ON {table_internetsales}.OrderDateKey = {table_date}.DateKey
    WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
    """).execute()

    reseller_sales = con.sql(f"""SELECT ROUND(SUM(SalesAmount), 0) AS TotalResellerSales
    FROM {table_resellersales}
    JOIN {table_date}
    ON {table_resellersales}.OrderDateKey = {table_date}.DateKey
    WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
    """).execute()

    # Alternativa usando alias para las tablas
    # reseller_sales = con.sql(f"""SELECT SUM(i.SalesAmount) AS TotalResellerSales
    # FROM {table_resellersales} AS i
    # JOIN {table_date} AS d
    # ON i.OrderDateKey = d.DateKey
    # WHERE d.FiscalYear = 2020
    # """).execute()

    internet_sales = internet_sales.iat[0, 0]
    reseller_sales = reseller_sales.iat[0, 0]
    return internet_sales, reseller_sales


@app.cell
def _(internet_sales):
    internet_sales
    return


@app.cell
def _(reseller_sales):
    reseller_sales
    return


if __name__ == "__main__":
    app.run()
