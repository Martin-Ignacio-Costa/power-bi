import marimo

__generated_with = "0.12.10"
app = marimo.App(width="medium")


@app.cell
def imports():
    import os
    import marimo as mo
    import altair as alt
    import pandas as pd
    import ibis
    from decimal import Decimal
    import ibis.expr.datatypes as dt
    return Decimal, alt, dt, ibis, mo, os, pd


@app.cell
def sql_settings(ibis, os):
    con = ibis.duckdb.connect()
    ibis.set_backend(con)

    sqlcon = ibis.mssql.connect(
        user=os.environ["SQLSERVER_USER"],
        password=os.environ["SQLSERVER_PASS"],
        host=os.environ["SQLSERVER_HOST"],
        database=os.environ["SQLSERVER_DB"],
        driver="SQL Server",
        port=os.environ["SQLSERVER_PORT"],
    )
    return con, sqlcon


@app.cell
def db_tables():
    table_date = "DimDate"
    table_resellersales = "FactResellerSales"
    table_internetsales = "FactInternetSales"
    return table_date, table_internetsales, table_resellersales


@app.cell
def quick_queries(
    sqlcon,
    table_date,
    table_internetsales,
    table_resellersales,
):
    qq_table_date = sqlcon.sql(f"SELECT * FROM {table_date}")
    qq_table_resellersales = sqlcon.sql(f"SELECT * FROM {table_resellersales}")
    qq_table_internetsales = sqlcon.sql(f"SELECT * FROM {table_internetsales}")
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
    dt,
    ibis,
    input_fiscal_year,
    sqlcon,
    table_date,
    table_internetsales,
    table_resellersales,
):
    internet_sales = sqlcon.sql(f"""
    SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS TotalInternetSales
    FROM {table_internetsales}
    JOIN {table_date}
    ON {table_internetsales}.OrderDateKey = {table_date}.DateKey
    WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
    """).execute()

    reseller_sales = sqlcon.sql(f"""
    SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS TotalResellerSales
    FROM {table_resellersales}
    JOIN {table_date}
    ON {table_resellersales}.OrderDateKey = {table_date}.DateKey
    WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
    """).execute()

    table_internet_sales = con.create_table(
        "internet_sales",
        schema=ibis.schema(
            {
                "TotalInternetSales": dt.Decimal(13, 2),
            }
        ),
    )
    tis = ibis.memtable(internet_sales)

    # Alternativa usando alias para las tablas
    # reseller_sales = sqlcon.sql(f"""SELECT SUM(i.SalesAmount) AS TotalResellerSales
    # FROM {table_resellersales} AS i
    # JOIN {table_date} AS d
    # ON i.OrderDateKey = d.DateKey
    # WHERE d.FiscalYear = 2020
    # """).execute()

    internet_sales = internet_sales.iat[0, 0]
    # internet_sales = float(internet_sales)
    # internet_sales = ibis.literal(internet_sales)
    # internet_sales = internet_sales.quantize(Decimal('0.00'))
    reseller_sales = reseller_sales.iat[0, 0]
    return internet_sales, reseller_sales, table_internet_sales, tis


@app.cell
def _(mo, tis):
    mo.ui.table(tis)
    return


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
