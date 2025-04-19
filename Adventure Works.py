

import marimo

__generated_with = "0.13.0"
app = marimo.App(
    width="medium",
    app_title="Adventure Works Sales",
    layout_file="layouts/Adventure Works.grid.json",
    sql_output="native",
)

with app.setup:
    # Initialization code that runs before all other cells
    import os
    import marimo as mo
    import altair as alt
    import pandas as pd
    import ibis
    from decimal import Decimal
    import ibis.expr.datatypes as dt
    import locale


@app.cell
def settings():
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
        label="<strong>Language / Idioma: </strong>",
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
            locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
            input_fiscal_year_label = "<strong>Fiscal Year: </strong>"
            input_sales_channel_title = (
                "<strong>Filter sales channels by: </strong>"
            )
            input_product_title = mo.md("<strong>Filter products by: </strong>")
            input_channel_internet_label = "Internet"
            input_channel_resellers_label = "Resellers"
            product_category_name = "EnglishProductCategoryName"
            product_subcategory_name = "EnglishProductSubcategoryName"
            product_name = "EnglishProductName"
            input_product_category_label = "Categories: "
            input_product_subcategory_label = "Subcategories: "
            input_product_label = "Products: "
            sales_total_title = "Sales $"

        # Spanish labels
        case "1":
            locale.setlocale(locale.LC_ALL, "es_AR.UTF-8")
            thousands_separator = "."
            decimal_separator = ","
            input_fiscal_year_label = "<strong>Año Fiscal: </strong>"
            input_sales_channel_title = (
                "<strong>Filtrar canales de venta por: </strong>"
            )
            input_product_title = mo.md("<strong>Filtrar productos por: </strong>")
            input_channel_internet_label = "Internet"
            input_channel_resellers_label = "Revendedores"
            product_category_name = "SpanishProductCategoryName"
            product_subcategory_name = "SpanishProductSubcategoryName"
            product_name = "SpanishProductName"
            input_product_category_label = "Categorías: "
            input_product_subcategory_label = "Subcategorías: "
            input_product_label = "Productos: "
            sales_total_title = "Ventas US$"

    # Language-independent variables
    product_category_key = "ProductCategoryKey"
    product_subcategory_key = "ProductSubcategoryKey"
    product_key = "ProductKey"
    return (
        input_channel_internet_label,
        input_channel_resellers_label,
        input_fiscal_year_label,
        input_product_category_label,
        input_product_label,
        input_product_subcategory_label,
        input_product_title,
        input_sales_channel_title,
        product_category_key,
        product_category_name,
        product_key,
        product_name,
        product_subcategory_key,
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
    return


@app.cell
def relationships():
    # Table relationships for use in inputs, filtering and analysis

    # category_subcategory_product = sqlcon.sql(f"""
    # SELECT
    # {table_product_category}.ProductCategoryKey,
    # {table_product_category}.{product_category_name},
    # {table_product_subcategory}.ProductSubcategoryKey,
    # {table_product_subcategory}.{product_subcategory_name},
    # {table_product}.ProductKey,
    # {table_product}.{product_name}
    # FROM {table_product_category}
    # JOIN {table_product_subcategory}
    #     ON {table_product_category}.ProductCategoryKey = {table_product_subcategory}.ProductCategoryKey
    # JOIN {table_product}
    #     ON {table_product_subcategory}.ProductSubcategoryKey = {table_product}.ProductSubcategoryKey
    # ORDER BY {table_product_category}.{product_category_name},
    # {table_product_subcategory}.{product_subcategory_name},
    # {table_product}.{product_name}
    # """).execute()

    # if "category_subcategory_product" in con.list_tables():
    #     con.drop_table("category_subcategory_product")
    # con.create_table(
    #     "category_subcategory_product",
    #     category_subcategory_product,
    # )
    # table_category_subcategory_product = con.table("category_subcategory_product")
    return


@app.cell
def _(input_channel_internet_label, input_channel_resellers_label):
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
    # list_category = con.sql(f"""
    # SELECT DISTINCT {product_category_name}, ProductCategoryKey
    # FROM category_subcategory_product
    # ORDER BY ProductCategoryKey
    # """).execute()

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
    return input_channel_internet, input_channel_resellers, list_fiscalyear


@app.cell
def _(
    input_channel_internet,
    input_channel_resellers,
    input_fiscal_year_label,
    input_product_category_label,
    input_sales_channel_title,
    list_fiscalyear,
    product_category_key,
    product_category_name,
    sqlcon,
    table_product_category,
):
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

    # Generate a list of product categories in the DB to use as filtering criteria
    list_category = sqlcon.sql(f"""
    SELECT DISTINCT {product_category_name}, {product_category_key}
    FROM {table_product_category}
    ORDER BY ProductCategoryKey
    """).execute()

    input_product_category = mo.ui.multiselect.from_series(
        list_category[f"{product_category_name}"],
        value=list_category[f"{product_category_name}"],
        label=input_product_category_label,
    )
    return input_fiscal_year, input_product_category, input_sales_channel


@app.cell
def _(
    input_product_category,
    input_product_subcategory_label,
    product_category_key,
    product_category_name,
    product_subcategory_key,
    product_subcategory_name,
    sqlcon,
    table_product_category,
    table_product_subcategory,
):
    # Generate a list of product subcategories in the DB to use as filtering criteria
    selected_categories = "', '".join(input_product_category.value)
    list_subcategory = sqlcon.sql(f"""
    SELECT DISTINCT {product_subcategory_name}, {product_subcategory_key}
    FROM {table_product_subcategory}
    WHERE {product_category_key} IN (
        SELECT {product_category_key}
        FROM {table_product_category}
        WHERE {product_category_name} IN ('{selected_categories}')
    )
    ORDER BY {product_subcategory_key}
    """).execute()

    input_product_subcategory = mo.ui.multiselect.from_series(
        list_subcategory[f"{product_subcategory_name}"],
        value=list_subcategory[f"{product_subcategory_name}"],
        label=input_product_subcategory_label,
    )
    return (input_product_subcategory,)


@app.cell
def _(
    input_product_label,
    input_product_subcategory,
    product_key,
    product_name,
    product_subcategory_key,
    product_subcategory_name,
    sqlcon,
    table_product,
    table_product_subcategory,
):
    # Generate a list of products in the DB to use as filtering criteria
    selected_subcategories = "', '".join(input_product_subcategory.value)
    list_product = sqlcon.sql(f"""
    SELECT DISTINCT {product_name}, {product_key}
    FROM {table_product}
    WHERE {product_subcategory_key} IN (
        SELECT {product_subcategory_key}
        FROM {table_product_subcategory}
        WHERE {product_subcategory_name} IN ('{selected_subcategories}')
    )
    ORDER BY {product_key}
    """).execute()

    input_product = mo.ui.multiselect.from_series(
        list_product[f"{product_name}"],
        value=list_product[f"{product_name}"],
        label=input_product_label,
    )
    return (input_product,)


@app.cell
def _(
    input_fiscal_year,
    input_product,
    input_product_category,
    input_product_subcategory,
    input_product_title,
    input_sales_channel,
):
    mo.vstack(
        [
            input_fiscal_year,
            input_sales_channel,
            input_product_title,
            input_product_category,
            input_product_subcategory,
            input_product,
        ]
    )
    return


@app.cell
def _(
    con,
    input_fiscal_year,
    input_product,
    product_key,
    product_name,
    sqlcon,
    table_date,
    table_product,
    table_sales_internet,
    table_sales_reseller,
):
    selected_products = "', '".join(input_product.value)
    # Channel sales

    sales_channel_internet = sqlcon.sql(f"""
    SELECT CAST(ROUND(SUM(SalesAmount), 0) AS DECIMAL(13, 2)) AS InternetSales
    FROM {table_sales_internet}
    JOIN {table_date}
    ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
    WHERE {table_date}.FiscalYear = {input_fiscal_year.value}
    AND {table_sales_internet}.{product_key} IN (
        SELECT {product_key}
        FROM {table_product}
        WHERE {product_name} IN ('{selected_products}')
    )
    """).execute().iat[0, 0]

    # sales_channel_internet = con.table("internet_sales").execute().iat[0, 0]

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

    # sales_channel_reseller = con.table("reseller_sales").execute().iat[0, 0]

    # sales_channel_all = sales_channel_internet + sales_channel_reseller


    # # Format results with thousands and decimal separators
    # sales_channel_internet = locale.format_string(
    #     "%.2f", sales_channel_internet, grouping=True
    # )
    # sales_channel_reseller = locale.format_string(
    #     "%.2f", sales_channel_reseller, grouping=True
    # )
    # sales_channel_all = locale.format_string(
    #     "%.2f", sales_channel_all, grouping=True
    # )

    # Alternativa usando alias para las tablas
    # reseller_sales = sqlcon.sql(f"""SELECT SUM(i.SalesAmount) AS TotalResellerSales
    # FROM {table_sales_reseller} AS i
    # JOIN {table_date} AS d
    # ON i.OrderDateKey = d.DateKey
    # WHERE d.FiscalYear = 2020
    # """).execute()
    return (sales_channel_internet,)


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
def _(sales_channel_all, sales_total_title):
    mo.callout(f"{sales_total_title} {sales_channel_all}")
    return


if __name__ == "__main__":
    app.run()
