import marimo

__generated_with = "0.12.10"
app = marimo.App(
    width="medium",
    app_title="Adventure Works Sales",
    layout_file="layouts/Adventure Works.grid.json",
    sql_output="native",
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
def settings(ibis, mo, os):
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

    # Language settings
    input_language = mo.ui.dropdown(
        options={
            "English / Inglés": "0",
            "Spanish / Español": "1",
        },
        value="English / Inglés",
        label="Language / Lenguaje: ",
    )
    return con, input_language, sqlcon


@app.cell
def _(input_language):
    input_language
    return


@app.cell
def language_variations(input_language):
    # Label variations for different languages

    match input_language.value:
        # English labels
        case "0":
            input_fiscal_year_label = "Fiscal Year: "
            input_sales_channel_title = "Sales channels: "
            input_channel_internet_label = "Internet"
            input_channel_resellers_label = "Resellers"
            product_category_name = "EnglishProductCategoryName"
            product_subcategory_name = "EnglishProductSubcategoryName"
            product_name = "EnglishProductName"
            input_product_category_label = "Product categories: "
            input_product_subcategory_label = "Product subcategories: "
            input_product_label = "Products: "
            sales_total_title = "Sales $"

        # Spanish labels
        case "1":
            input_fiscal_year_label = "Año Fiscal: "
            input_sales_channel_title = "Canales de venta: "
            input_channel_internet_label = "Internet"
            input_channel_resellers_label = "Revendedores"
            product_category_name = "SpanishProductCategoryName"
            product_subcategory_name = "SpanishProductSubcategoryName"
            product_name = "SpanishProductName"
            input_product_category_label = "Categorías de productos: "
            input_product_subcategory_label = "Subcategorías de productos: "
            input_product_label = "Productos: "
            sales_total_title = "Ventas US$"
    return (
        input_channel_internet_label,
        input_channel_resellers_label,
        input_fiscal_year_label,
        input_product_category_label,
        input_product_label,
        input_product_subcategory_label,
        input_sales_channel_title,
        product_category_name,
        product_name,
        product_subcategory_name,
        sales_total_title,
    )


@app.cell
def db_tables():
    # List of SQL Server tables for AdventureWorksDW2020 DB

    # Dimension tables
    table_date = "DimDate"
    table_product_category = "DimProductCategory"
    table_product_subcategory = "DimProductSubcategory"
    table_product = "DimProduct"

    # Fact tables
    table_sales_reseller = "FactResellerSales"
    table_sales_internet = "FactInternetSales"
    return (
        table_date,
        table_product,
        table_product_category,
        table_product_subcategory,
        table_sales_internet,
        table_sales_reseller,
    )


@app.cell
def quick_queries(
    sqlcon,
    table_date,
    table_product,
    table_product_category,
    table_product_subcategory,
    table_sales_internet,
    table_sales_reseller,
):
    # Quick queries for data exploration

    qq_table_date = sqlcon.sql(f"SELECT * FROM {table_date}")
    qq_table_product_category = sqlcon.sql(
        f"SELECT * FROM {table_product_category}"
    )
    qq_table_product_subcategory = sqlcon.sql(
        f"SELECT * FROM {table_product_subcategory}"
    )
    qq_table_product = sqlcon.sql(f"SELECT * FROM {table_product}")
    qq_table_sales_reseller = sqlcon.sql(f"SELECT * FROM {table_sales_reseller}")
    qq_table_sales_internet = sqlcon.sql(f"SELECT * FROM {table_sales_internet}")
    return (
        qq_table_date,
        qq_table_product,
        qq_table_product_category,
        qq_table_product_subcategory,
        qq_table_sales_internet,
        qq_table_sales_reseller,
    )


@app.cell
def relationships(
    con,
    product_category_name,
    product_name,
    product_subcategory_name,
    sqlcon,
    table_product,
    table_product_category,
    table_product_subcategory,
):
    # Table relationships for use in inputs, filtering and analysis

    category_subcategory_product = sqlcon.sql(f"""
    SELECT
    {table_product_category}.ProductCategoryKey,
    {table_product_category}.{product_category_name},
    {table_product_subcategory}.ProductSubcategoryKey,
    {table_product_subcategory}.{product_subcategory_name},
    {table_product}.ProductKey,
    {table_product}.{product_name}
    FROM {table_product_category}
    JOIN {table_product_subcategory} 
        ON {table_product_category}.ProductCategoryKey = {table_product_subcategory}.ProductCategoryKey
    JOIN {table_product}
        ON {table_product_subcategory}.ProductSubcategoryKey = {table_product}.ProductSubcategoryKey
    ORDER BY {table_product_category}.{product_category_name},
    {table_product_subcategory}.{product_subcategory_name},
    {table_product}.{product_name}
    """).execute()

    if "category_subcategory_product" in con.list_tables():
        con.drop_table("category_subcategory_product")
    con.create_table(
        "category_subcategory_product",
        category_subcategory_product,
    )
    table_category_subcategory_product = con.table("category_subcategory_product")
    return category_subcategory_product, table_category_subcategory_product


@app.cell
def filter_sources(
    con,
    input_channel_internet_label,
    input_channel_resellers_label,
    mo,
    product_category_name,
):
    # Data sources for the different filtering criteria

    list_fiscalyear = {
        "FY2018": "2018",
        "FY2019": "2019",
        "FY2020": "2020",
    }

    input_channel_internet = mo.ui.checkbox(
        label=input_channel_internet_label, value=True
    )
    input_channel_resellers = mo.ui.checkbox(
        label=input_channel_resellers_label, value=True
    )

    # # Generate a list of product categories in the DB to use as filtering criteria
    # list_category = sqlcon.sql(f"""
    # SELECT DISTINCT {product_category_name}, ProductCategoryKey
    # FROM {table_product_category}
    # ORDER BY ProductCategoryKey
    # """).execute()

    # # Generate a list of product subcategories in the DB to use as filtering criteria
    # list_subcategory = sqlcon.sql(f"""
    # SELECT DISTINCT {product_subcategory_name}, ProductSubcategoryKey
    # FROM {table_product_subcategory}
    # ORDER BY ProductSubcategoryKey
    # """).execute()

    # # Generate a list of products in the DB to use as filtering criteria
    # list_product = sqlcon.sql(f"""
    # SELECT DISTINCT {product_name}, ProductKey
    # FROM {table_product}
    # ORDER BY ProductKey
    # """).execute()

    # Generate a list of product categories in the DB to use as filtering criteria
    list_category = con.sql(f"""
    SELECT DISTINCT {product_category_name}, ProductCategoryKey
    FROM category_subcategory_product
    ORDER BY ProductCategoryKey
    """).execute()

    # # Generate a list of product subcategories in the DB to use as filtering criteria
    # list_subcategory = con.sql(f"""
    # SELECT DISTINCT {product_subcategory_name}, ProductSubcategoryKey
    # FROM category_subcategory_product
    # ORDER BY ProductSubcategoryKey
    # """).execute()

    # # Generate a list of products in the DB to use as filtering criteria
    # list_product = con.sql(f"""
    # SELECT DISTINCT {product_name}, ProductKey
    # FROM category_subcategory_product
    # ORDER BY ProductKey
    # """).execute()
    return (
        input_channel_internet,
        input_channel_resellers,
        list_category,
        list_fiscalyear,
    )


@app.cell
def inputs(
    input_channel_internet,
    input_channel_resellers,
    input_fiscal_year_label,
    input_product_category_label,
    input_sales_channel_title,
    list_category,
    list_fiscalyear,
    mo,
    product_category_name,
):
    # User inputs for data visualization and analysis

    input_fiscal_year = mo.ui.dropdown(
        options=list_fiscalyear,
        value="FY2018",
        label=input_fiscal_year_label,
    )

    input_sales_channel = mo.vstack(
        [
            mo.md(input_sales_channel_title),
            input_channel_internet,
            input_channel_resellers,
        ],
        justify="start",
        align="start",
    )

    input_product_category = mo.ui.multiselect.from_series(
        list_category[f"{product_category_name}"],
        value=list_category[f"{product_category_name}"],
        label=input_product_category_label,
    )
    return input_fiscal_year, input_product_category, input_sales_channel


@app.cell
def _(
    con,
    input_product_category,
    input_product_subcategory_label,
    mo,
    product_category_name,
    product_subcategory_name,
):
    # Generate a list of product subcategories in the DB to use as filtering criteria
    selected_categories = "', '".join(input_product_category.value)
    list_subcategory = con.sql(f"""
    SELECT DISTINCT {product_subcategory_name}, ProductSubcategoryKey
    FROM category_subcategory_product
    WHERE {product_category_name} IN ('{selected_categories}');
    """).execute()

    input_product_subcategory = mo.ui.multiselect.from_series(
        list_subcategory[f"{product_subcategory_name}"],
        value=list_subcategory[f"{product_subcategory_name}"],
        label=input_product_subcategory_label,
    )
    return input_product_subcategory, list_subcategory, selected_categories


@app.cell
def _(con, input_product_label, input_product_subcategory, mo, product_name):
    # Generate a list of products in the DB to use as filtering criteria
    selected_subcategories = "', '".join(input_product_subcategory.value)
    list_product = con.sql(f"""
    SELECT DISTINCT {product_name}, ProductKey
    FROM category_subcategory_product
    WHERE {product_name} IN ('{selected_subcategories}');
    """).execute()

    input_product = mo.ui.multiselect.from_series(
        list_product[f"{product_name}"],
        value=list_product[f"{product_name}"],
        label=input_product_label,
    )
    return input_product, list_product, selected_subcategories


@app.cell
def _(
    input_fiscal_year,
    input_product,
    input_product_category,
    input_product_subcategory,
    input_sales_channel,
    mo,
):
    mo.vstack(
        [
            input_fiscal_year,
            input_sales_channel,
            input_product_category,
            input_product_subcategory,
            input_product,
        ]
    )
    return


@app.cell
def _():
    return


@app.cell
def _(
    con,
    input_fiscal_year,
    sqlcon,
    table_date,
    table_sales_internet,
    table_sales_reseller,
):
    # Channel sales

    if "internet_sales" in con.list_tables():
        con.drop_table("internet_sales")
    con.create_table(
        "internet_sales",
        sqlcon.sql(f"""
        SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS InternetSales
        FROM {table_sales_internet}
        JOIN {table_date}
        ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
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
        FROM {table_sales_reseller}
        JOIN {table_date}
        ON {table_sales_reseller}.OrderDateKey = {table_date}.DateKey
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
    # FROM {table_sales_reseller} AS i
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
def _(mo, sales_channel_all, sales_total_title):
    mo.callout(f"{sales_total_title} {sales_channel_all}")
    return


if __name__ == "__main__":
    app.run()
