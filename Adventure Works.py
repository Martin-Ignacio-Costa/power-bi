

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
    import ibis
    import ibis.expr.datatypes as dt
    from decimal import Decimal
    from datetime import datetime
    from datetime import date
    import locale


@app.cell
def functions(input_language):
    def locale_decimal(value):
        """
        Transforms a numeric value into a string formatted as a decimal using the corresponding regional thousands and decimals separators
        """
        if hasattr(value, "iloc"):
            value = value.iloc[0]
        return locale.format_string("%.2f", value, grouping=True)


    def locale_date(date):
        """
        Converts a date into a string formatted for the regional configuration
        """
        if input_language.value == "0":
            return date.strftime("%m/%d/%Y")
        elif input_language.value == "1":
            return date.strftime("%d/%m/%Y")
    return locale_date, locale_decimal


@app.cell
def db_settings():
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
    return (sqlcon,)


@app.cell
def language_settings():
    # Language settings
    input_language = mo.ui.dropdown(
        options={
            "English / Inglés": "0",
            "Spanish / Español": "1",
        },
        value="English / Inglés",
        label="<strong>Language / Idioma: </strong>",
    )
    return (input_language,)


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
            current_sales_total_title = "Sales $"
            current_profit_total_title = "Profit $"
            fy_dates_title = "Sales between: "
            sales_millions_label = "Sales (in millions)"
            profit_millions_label = "Profit (in millions)"
            volume_thousands_label = "Volume (# of orders in thousands)"

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
            current_sales_total_title = "Ventas US$"
            current_profit_total_title = "Ganancias US$"
            fy_dates_title = "Ventas entre: "
            sales_millions_label = "Ventas (en millones US$)"
            profit_millions_label = "Ganancias (en millones US$)"
            volume_thousands_label = "Volúmen (# de pedidos en miles)"

    # Language-independent variables
    product_category_key = "ProductCategoryKey"
    product_subcategory_key = "ProductSubcategoryKey"
    product_key = "ProductKey"
    sales_order_number = "SalesOrderNumber"
    return (
        current_profit_total_title,
        current_sales_total_title,
        fy_dates_title,
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
        profit_millions_label,
        sales_millions_label,
        sales_order_number,
        volume_thousands_label,
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
def fiscal_year(input_fiscal_year_label):
    list_fiscalyear = {
        "FY2018": "2018",
        "FY2019": "2019",
        "FY2020": "2020",
    }

    input_fiscal_year = mo.ui.dropdown(
        options=list_fiscalyear,
        value="FY2018",
        label=input_fiscal_year_label,
    )
    return (input_fiscal_year,)


@app.cell
def sales_channels(
    input_channel_internet_label,
    input_channel_resellers_label,
):
    input_channel_internet = mo.ui.checkbox(
        label=input_channel_internet_label, value=True
    )
    input_channel_resellers = mo.ui.checkbox(
        label=input_channel_resellers_label, value=True
    )
    return input_channel_internet, input_channel_resellers


@app.cell
def product_categories(
    input_product_category_label,
    product_category_key,
    product_category_name,
    sqlcon,
    table_product_category,
):
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
    return (input_product_category,)


@app.cell
def product_subcategories(
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
def products(
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
def input_filters(
    input_channel_internet,
    input_channel_resellers,
    input_fiscal_year,
    input_language,
    input_product,
    input_product_category,
    input_product_subcategory,
    input_product_title,
    input_sales_channel_title,
):
    mo.vstack(
        [
            input_language,
            input_fiscal_year,
            mo.md(input_sales_channel_title),
            input_channel_internet,
            input_channel_resellers,
            input_product_title,
            input_product_category,
            input_product_subcategory,
            input_product,
        ],
        align="start",
        justify="start",
    )
    return


@app.cell
def fy_dates(input_fiscal_year, locale_date, sqlcon, table_date):
    current_fy = int(input_fiscal_year.value)
    previous_fy = current_fy - 1

    # Fiscal year dates
    fy_min_dates = sqlcon.sql(f"""
    SELECT 
        DayNumberOfMonth AS StartDay,
        MonthNumberOfYear AS StartMonth,
        CalendarYear AS StartYear
    FROM {table_date}
    WHERE DateKey = (
        SELECT MIN(DateKey)
        FROM {table_date}
        WHERE FiscalYear = {current_fy}
    )
    """).execute()

    fy_max_dates = sqlcon.sql(f"""
    SELECT 
        DayNumberOfMonth AS EndDay,
        MonthNumberOfYear AS EndMonth,
        CalendarYear AS EndYear
    FROM {table_date}
    WHERE DateKey = (
        SELECT MAX(DateKey)
        FROM {table_date}
        WHERE FiscalYear = {current_fy}
    )
    """).execute()

    fy_start_day = fy_min_dates["StartDay"].iat[0]
    fy_start_month = fy_min_dates["StartMonth"].iat[0]
    fy_start_year = fy_min_dates["StartYear"].iat[0]

    fy_end_day = fy_max_dates["EndDay"].iat[0]
    fy_end_month = fy_max_dates["EndMonth"].iat[0]
    fy_end_year = fy_max_dates["EndYear"].iat[0]

    fy_start_date = locale_date(date(fy_start_year, fy_start_month, fy_start_day))
    fy_end_date = locale_date(date(fy_end_year, fy_end_month, fy_end_day))
    return current_fy, fy_end_date, fy_start_date, previous_fy


@app.cell
def sales_profit(
    current_fy,
    input_channel_internet,
    input_channel_resellers,
    input_product,
    locale_decimal,
    previous_fy,
    product_key,
    product_name,
    sqlcon,
    table_date,
    table_product,
    table_sales_internet,
    table_sales_reseller,
):
    selected_products = "', '".join(
        product.replace("'", "''") for product in input_product.value
    )

    # Channel sales and profit

    if input_channel_internet.value == True:
        sales_profit_channel_internet = sqlcon.sql(f"""
        SELECT 
            FiscalYear,
            CASE WHEN COUNT(*) = 0 THEN 0
                ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2)) 
                END AS InternetSales,
            CASE WHEN COUNT(*) = 0 THEN 0
                ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2)) 
                END AS InternetProfit
        FROM {table_sales_internet}
        JOIN {table_date}
        ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear IN (
            {current_fy},
            CASE WHEN EXISTS (
                SELECT 1
                FROM {table_date}
                WHERE {table_date}.FiscalYear = {previous_fy}
            ) THEN {previous_fy} 
            ELSE NULL
            END
        )
        AND {table_sales_internet}.{product_key} IN (
            SELECT {product_key}
            FROM {table_product}
            WHERE {product_name} IN ('{selected_products}')
        )
        GROUP BY FiscalYear
        """)
        current_sales_channel_internet = (
            sales_profit_channel_internet.filter(
                sales_profit_channel_internet["FiscalYear"] == current_fy
            )
            .select("InternetSales")
            .as_scalar()
            .execute()
        )
        current_profit_channel_internet = (
            sales_profit_channel_internet.filter(
                sales_profit_channel_internet["FiscalYear"] == current_fy
            )
            .select("InternetProfit")
            .as_scalar()
            .execute()
        )
        previous_sales_channel_internet = (
            sales_profit_channel_internet.filter(
                sales_profit_channel_internet["FiscalYear"] == previous_fy
            )
            .select("InternetSales")
            .as_scalar()
            .execute()
        )
        previous_profit_channel_internet = (
            sales_profit_channel_internet.filter(
                sales_profit_channel_internet["FiscalYear"] == previous_fy
            )
            .select("InternetProfit")
            .as_scalar()
            .execute()
        )
        if previous_sales_channel_internet and previous_profit_channel_internet is None:
            previous_sales_channel_internet = 0
            previous_profit_channel_internet = 0
    else:
        current_sales_channel_internet = 0
        current_profit_channel_internet = 0
        previous_sales_channel_internet = 0
        previous_profit_channel_internet = 0

    if input_channel_resellers.value == True:
        sales_profit_channel_resellers = sqlcon.sql(f"""
        SELECT 
            FiscalYear,
            CASE WHEN COUNT(*) = 0 THEN 0
                ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2)) 
                END AS ResellerSales,
            CASE WHEN COUNT(*) = 0 THEN 0
                ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2)) 
                END AS ResellerProfit
        FROM {table_sales_reseller}
        JOIN {table_date}
        ON {table_sales_reseller}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear IN (
            {current_fy}, 
            CASE WHEN EXISTS (
                SELECT 1
                FROM {table_date}
                WHERE {table_date}.FiscalYear = {previous_fy}
                ) THEN {previous_fy}
                ELSE NULL
                END
            )
        AND {table_sales_reseller}.{product_key} IN (
            SELECT {product_key}
            FROM {table_product}
            WHERE {product_name} IN ('{selected_products}')
        )
        GROUP BY FiscalYear
        """)
        current_sales_channel_resellers = (
            sales_profit_channel_resellers.filter(
                sales_profit_channel_resellers["FiscalYear"] == current_fy
            )
            .select("ResellerSales")
            .as_scalar()
            .execute()
        )
        current_profit_channel_resellers = (
            sales_profit_channel_resellers.filter(
                sales_profit_channel_resellers["FiscalYear"] == current_fy
            )
            .select("ResellerProfit")
            .as_scalar()
            .execute()
        )
        previous_sales_channel_resellers = (
            sales_profit_channel_resellers.filter(
                sales_profit_channel_resellers["FiscalYear"] == current_fy
            )
            .select("ResellerSales")
            .as_scalar()
            .execute()
        )
        previous_profit_channel_resellers = (
            sales_profit_channel_resellers.filter(
                sales_profit_channel_resellers["FiscalYear"] == current_fy
            )
            .select("ResellerProfit")
            .as_scalar()
            .execute()
        )
        if previous_sales_channel_resellers and previous_profit_channel_resellers is None:
            previous_sales_channel_resellers = 0
            previous_profit_channel_resellers = 0
    else:
        current_sales_channel_resellers = 0
        current_profit_channel_resellers = 0
        previous_sales_channel_resellers = 0
        previous_profit_channel_resellers = 0

    current_sales_channel_all = Decimal(
        current_sales_channel_internet + current_sales_channel_resellers
    )
    current_profit_channel_all = Decimal(
        current_profit_channel_internet + current_profit_channel_resellers
    )
    previous_sales_channel_all = Decimal(
        previous_sales_channel_internet + previous_sales_channel_resellers
    )
    previous_profit_channel_all = Decimal(
        previous_profit_channel_internet + previous_profit_channel_resellers
    )

    sales_millions = str(round(current_sales_channel_all / 1_000_000, 2))
    profit_millions = str(round(current_profit_channel_all / 1_000_000, 2))

    # Format results with thousands and decimal separators
    current_sales_channel_all, current_profit_channel_all = (
        locale_decimal(current_sales_channel_all),
        locale_decimal(current_profit_channel_all),
    )
    return (
        current_profit_channel_all,
        current_sales_channel_all,
        profit_millions,
        sales_millions,
        selected_products,
    )


@app.cell
def order_volume(
    current_fy,
    input_channel_internet,
    input_channel_resellers,
    previous_fy,
    product_key,
    product_name,
    product_subcategory_key,
    sales_order_number,
    selected_products,
    sqlcon,
    table_date,
    table_product,
    table_sales_internet,
    table_sales_reseller,
):
    # Volume of orders
    if input_channel_internet.value == True:
        volume_channel_internet = sqlcon.sql(f"""
        SELECT 
            FiscalYear,
            COUNT(DISTINCT {sales_order_number}) AS OrderVolume
        FROM {table_sales_internet}
        JOIN {table_date}
        ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear IN ({current_fy}, {previous_fy})
        AND {table_sales_internet}.{product_key} IN (
            SELECT {product_key}
            FROM {table_product}
            WHERE {product_name} IN ('{selected_products}')
        )
        GROUP BY FiscalYear
        """)
        current_volume_channel_internet = (volume_channel_internet.filter(
            volume_channel_internet["FiscalYear"] == current_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute())
        previous_volume_channel_internet = (volume_channel_internet.filter(
            volume_channel_internet["FiscalYear"] == previous_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute())
    else:
        current_volume_channel_internet = 0
        previous_volume_channel_internet = 0

    if input_channel_resellers.value == True:
        volume_channel_resellers = sqlcon.sql(f"""
        SELECT 
            FiscalYear,
            COUNT(DISTINCT {sales_order_number}) AS OrderVolume
        FROM {table_sales_reseller}
        JOIN {table_date}
        ON {table_sales_reseller}.OrderDateKey = {table_date}.DateKey
        WHERE {table_date}.FiscalYear IN ({current_fy}, {previous_fy})
        AND {table_sales_reseller}.{product_key} IN (
            SELECT {product_key}
            FROM {table_product}
            WHERE {product_name} IN ('{selected_products}')
            --AND {product_subcategory_key} IS NOT NULL
        )
        GROUP BY FiscalYear
        """)
        current_volume_channel_resellers = (volume_channel_resellers.filter(
            volume_channel_resellers["FiscalYear"] == current_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute())
        previous_volume_channel_resellers = (volume_channel_resellers.filter(
            volume_channel_resellers["FiscalYear"] == previous_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute())
    else:
        current_volume_channel_resellers = 0
        previous_volume_channel_resellers = 0

    current_volume_channel_all = Decimal(current_volume_channel_internet + current_volume_channel_resellers)
    previous_volume_channel_all = Decimal(previous_volume_channel_internet + previous_volume_channel_resellers)

    current_volume_thousands = str(round(current_volume_channel_all / 1_000, 2))


    # # Format results with thousands and decimal separators
    # sales_channel_all, profit_channel_all = (
    #     locale_decimal(sales_channel_all),
    # )
    return (current_volume_thousands,)


@app.cell
def lastyear_sales_profit():
    # Sales and profit from the previous year

    # if input_channel_internet.value == True:
    #     lastyear_sales_profit_channel_internet = sqlcon.sql(f"""
    #     SELECT
    #         CASE WHEN COUNT(*) = 0 THEN 0
    #             ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2))
    #             END AS InternetSales,
    #         CASE WHEN COUNT(*) = 0 THEN 0
    #             ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2))
    #             END AS InternetProfit
    #     FROM {table_sales_internet}
    #     JOIN {table_date}
    #     ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
    #     WHERE {table_date}.FiscalYear = {current_fy} - 1
    #     AND {table_sales_internet}.{product_key} IN (
    #         SELECT {product_key}
    #         FROM {table_product}
    #         WHERE {product_name} IN ('{selected_products}')
    #         --AND {product_subcategory_key} IS NOT NULL
    #     )
    #     """).execute()
    #     lastyear_sales_channel_internet = lastyear_sales_profit_channel_internet[
    #         "InternetSales"
    #     ].iat[0]
    #     lastyear_profit_channel_internet = lastyear_sales_profit_channel_internet[
    #         "InternetProfit"
    #     ].iat[0]
    # else:
    #     sales_channel_internet = 0
    #     profit_channel_internet = 0

    # if input_channel_resellers.value == True:
    #     sales_profit_channel_resellers = sqlcon.sql(f"""
    #     SELECT
    #         CASE WHEN COUNT(*) = 0 THEN 0
    #             ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2))
    #             END AS ResellerSales,
    #         CASE WHEN COUNT(*) = 0 THEN 0
    #             ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2))
    #             END AS ResellerProfit
    #     FROM {table_sales_reseller}
    #     JOIN {table_date}
    #     ON {table_sales_reseller}.OrderDateKey = {table_date}.DateKey
    #     WHERE {table_date}.FiscalYear = {current_fy}
    #     AND {table_sales_reseller}.{product_key} IN (
    #         SELECT {product_key}
    #         FROM {table_product}
    #         WHERE {product_name} IN ('{selected_products}')
    #         --AND {product_subcategory_key} IS NOT NULL
    #     )
    #     """).execute()
    #     sales_channel_resellers = sales_profit_channel_resellers[
    #         "ResellerSales"
    #     ].iat[0]
    #     profit_channel_resellers = sales_profit_channel_resellers[
    #         "ResellerProfit"
    #     ].iat[0]
    # else:
    #     sales_channel_resellers = 0
    #     profit_channel_resellers = 0

    # sales_channel_all = sales_channel_internet + sales_channel_resellers
    # profit_channel_all = profit_channel_internet + profit_channel_resellers

    # sales_millions = str(round(sales_channel_all / 1_000_000, 2))
    # profit_millions = str(round(profit_channel_all / 1_000_000, 2))

    # # Format results with thousands and decimal separators
    # sales_channel_all, profit_channel_all = (
    #     locale_decimal(sales_channel_all),
    #     locale_decimal(profit_channel_all),
    # )
    return


@app.cell
def _(fy_dates_title, fy_end_date, fy_start_date):
    mo.md(f"{fy_dates_title} {fy_start_date} - {fy_end_date}")
    return


@app.cell
def _(current_sales_channel_all, current_sales_total_title):
    mo.callout(f"{current_sales_total_title} {current_sales_channel_all}")
    return


@app.cell
def _(sales_millions, sales_millions_label):
    mo.md(f"""{sales_millions_label}\n
    {sales_millions}M""")
    return


@app.cell
def _(profit_millions, profit_millions_label):
    mo.md(f"""{profit_millions_label}\n
    {profit_millions}M""")
    return


@app.cell
def _(current_volume_thousands, volume_thousands_label):
    mo.md(f"""{volume_thousands_label}\n
    {current_volume_thousands}K""")
    return


@app.cell
def _(current_profit_channel_all, current_profit_total_title):
    mo.callout(f"{current_profit_total_title} {current_profit_channel_all}")
    return


@app.cell
def _():
    hello_world = mo.Html("<h2>Hello, World</h2>")
    mo.Html(
        f"""
        <h1>Hello, Universe!</h1>
        {hello_world}
        <span style="color: red; font-family: Arial; font-size: 20px;">This text is red, Arial, and 20px!</span>
        """
    )
    return


if __name__ == "__main__":
    app.run()
