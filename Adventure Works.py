import marimo

__generated_with = "0.12.10"
app = marimo.App(
    width="medium",
    layout_file="layouts/Adventure Works.grid.json",
)


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
    # Settings for database connections

    con = ibis.duckdb.connect()
    ibis.set_backend(con)
    ibis.options.interactive = True

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
    # List of SQL Server tables for AdventureWorksDW2020 DB

    # Dimension tables
    table_date = "DimDate"
    table_productcategory = "DimProductCategory"
    table_productsubcategory = "DimProductSubcategory"
    table_product = "DimProduct"

    # Fact tables
    table_resellersales = "FactResellerSales"
    table_internetsales = "FactInternetSales"
    return (
        table_date,
        table_internetsales,
        table_product,
        table_productcategory,
        table_productsubcategory,
        table_resellersales,
    )


@app.cell
def quick_queries(
    sqlcon,
    table_date,
    table_internetsales,
    table_product,
    table_productcategory,
    table_productsubcategory,
    table_resellersales,
):
    # Quick queries for data exploration

    qq_table_date = sqlcon.sql(f"SELECT * FROM {table_date}")
    qq_table_productcategory = sqlcon.sql(f"SELECT * FROM {table_productcategory}")
    qq_table_productsubcategory = sqlcon.sql(
        f"SELECT * FROM {table_productsubcategory}"
    )
    qq_table_product = sqlcon.sql(f"SELECT * FROM {table_product}")
    qq_table_resellersales = sqlcon.sql(f"SELECT * FROM {table_resellersales}")
    qq_table_internetsales = sqlcon.sql(f"SELECT * FROM {table_internetsales}")
    return (
        qq_table_date,
        qq_table_internetsales,
        qq_table_product,
        qq_table_productcategory,
        qq_table_productsubcategory,
        qq_table_resellersales,
    )


@app.cell
def filter_sources(
    mo,
    sqlcon,
    table_product,
    table_productcategory,
    table_productsubcategory,
):
    # Data sources for the different filtering criteria

    list_fiscalyear = {
        "FY2018": "2018",
        "FY2019": "2019",
        "FY2020": "2020",
    }

    input_channel_internet = mo.ui.checkbox(label="Internet", value=True)
    input_channel_resellers = mo.ui.checkbox(label="Resellers", value=True)

    list_category = sqlcon.sql(f"""
    SELECT DISTINCT EnglishProductCategoryName, ProductCategoryKey
    FROM {table_productcategory}
    ORDER BY ProductCategoryKey
    """).execute()
    list_category = list_category["EnglishProductCategoryName"].to_list()

    list_subcategory = sqlcon.sql(f"""
    SELECT DISTINCT EnglishProductSubcategoryName, ProductSubcategoryKey
    FROM {table_productsubcategory}
    ORDER BY ProductSubcategoryKey
    """).execute()
    list_subcategory = list_subcategory["EnglishProductSubcategoryName"].to_list()

    # Dynamic variable generation methods
    # input_category = {}
    # for category in list_category["EnglishProductCategoryName"]:
    #     input_category[f"{category.lower()}"] = mo.ui.checkbox(
    #         label=f"{category}",
    #         value=True,
    #     )

    # list_category = list_category["EnglishProductCategoryName"].to_list()
    # for category in list_category:
    #     category = category.lower()
    #     category_capital = category.capitalize()
    #     globals()[f"input_category_{category}"] = mo.ui.checkbox(
    #         label=f"{category_capital}",
    #         value=True,
    #     )

    list_product = sqlcon.sql(f"""
    SELECT DISTINCT EnglishProductName, ProductKey
    FROM {table_product}
    ORDER BY ProductKey
    """).execute()

    # subcategory_list = subcategory_list["EnglishProductSubcategoryName"].to_list()
    return (
        input_channel_internet,
        input_channel_resellers,
        list_category,
        list_fiscalyear,
        list_product,
        list_subcategory,
    )


@app.cell
def inputs(
    input_channel_internet,
    input_channel_resellers,
    list_category,
    list_fiscalyear,
    list_subcategory,
    mo,
):
    # User inputs for data visualization and analysis

    input_fiscal_year = mo.ui.dropdown(
        options=list_fiscalyear,
        value="FY2018",
        label="Fiscal Year: ",
    )

    input_sales_channel = mo.vstack(
        [
            mo.md("Sales channels: "),
            input_channel_internet,
            input_channel_resellers,
        ],
        justify="start",
        align="start",
    )

    input_product_category = mo.ui.multiselect(
        options=list_category,
        value=list_category,
        label="Product categories: ",
    )

    input_product_subcategory = mo.ui.multiselect(
        options=list_subcategory,
        value=list_subcategory,
        label="Product subcategories: ",
    )
    return (
        input_fiscal_year,
        input_product_category,
        input_product_subcategory,
        input_sales_channel,
    )


@app.cell
def _():
    # if input_product_category_bikes.value == True:
    #     input_product_subcategory_mountainbikes = mo.ui.checkbox(
    #         label="Mountain Bikes", value=True
    #     )
    #     input_product_subcategory_roadbikes = mo.ui.checkbox(
    #         label="Road Bikes", value=True
    #     )
    #     input_product_subcategory_touringbikes = mo.ui.checkbox(
    #         label="Touring Bikes", value=True
    #     )

    # input_product_subcategory = mo.vstack(
    #     [
    #         mo.md("Product subcategories: "),
    #         input_product_subcategory_mountainbikes,
    #         input_product_subcategory_roadbikes,
    #         input_product_subcategory_touringbikes,
    #     ],
    #     justify="start",
    #     align="start",
    # )
    return


@app.cell
def _(input_product_subcategory):
    input_product_subcategory
    return


@app.cell
def _(input_fiscal_year, input_product_category, input_sales_channel, mo):
    mo.vstack(
        [
            input_fiscal_year,
            input_sales_channel,
            input_product_category,
        ]
    )
    return


@app.cell
def _(
    con,
    input_fiscal_year,
    sqlcon,
    table_date,
    table_internetsales,
    table_resellersales,
):
    # Channel sales

    if "internet_sales" in con.list_tables():
        con.drop_table("internet_sales")
    con.create_table(
        "internet_sales",
        sqlcon.sql(f"""
        SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS InternetSales
        FROM {table_internetsales}
        JOIN {table_date}
        ON {table_internetsales}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
        """).execute(),
    )

    sales_channel_internet = con.table("internet_sales").execute().iat[0, 0]

    if ("reseller_sales") in con.list_tables():
        con.drop_table("reseller_sales")
    con.create_table(
        "reseller_sales",
        sqlcon.sql(f"""
        SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS ResellerSales
        FROM {table_resellersales}
        JOIN {table_date}
        ON {table_resellersales}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
        """).execute(),
    )

    sales_channel_reseller = con.table("reseller_sales").execute().iat[0, 0]

    sales_channel_all = sales_channel_internet + sales_channel_reseller

    # Format results with thousands and decimal separators
    sales_channel_internet = f"{sales_channel_internet:,.0f}"
    sales_channel_reseller = f"{sales_channel_reseller:,.0f}"
    sales_channel_all = f"{sales_channel_all:,.0f}"

    # Alternativa usando alias para las tablas
    # reseller_sales = sqlcon.sql(f"""SELECT SUM(i.SalesAmount) AS TotalResellerSales
    # FROM {table_resellersales} AS i
    # JOIN {table_date} AS d
    # ON i.OrderDateKey = d.DateKey
    # WHERE d.FiscalYear = 2020
    # """).execute()
    return sales_channel_all, sales_channel_internet, sales_channel_reseller


@app.cell
def _(sales_channel_internet):
    sales_channel_internet
    return


@app.cell
def _(sales_channel_reseller):
    sales_channel_reseller
    return


@app.cell
def _(sales_channel_all):
    sales_channel_all
    return


@app.cell
def _(mo, sales_channel_all):
    mo.callout(f"Sales ${sales_channel_all}")
    return


if __name__ == "__main__":
    app.run()
