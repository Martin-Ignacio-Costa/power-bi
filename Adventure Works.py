# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==5.5.0",
#     "ibis-framework[duckdb]==10.5.0",
#     "ibis-framework[mssql]==10.5.0",
#     "marimo",
#     "pandas==2.2.3",
# ]
# ///

import marimo

__generated_with = "0.13.3"
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
    import pandas as pd
    from ibis.common.collections import FrozenOrderedDict
    from decimal import Decimal
    from datetime import datetime
    from datetime import date
    import locale


@app.cell
def functions(input_language):
    def locale_decimal(value):
        """
        Transforms a numeric value into a "VARCHAR" formatted as a decimal using the corresponding regional thousands and decimals separators
        """
        if hasattr(value, "iloc"):
            value = value.iloc[0]
        return locale.format_string("%.2f", value, grouping=True)


    def locale_date(date):
        """
        Converts a date into a "VARCHAR" formatted for the regional configuration
        """
        if input_language.value == "0":
            return date.strftime("%m/%d/%Y")
        elif input_language.value == "1":
            return date.strftime("%d/%m/%Y")
    return locale_date, locale_decimal


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
            input_data_source_label = "Data source: "
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
            input_data_source_label = "Orígen datos: "
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
            volume_thousands_label = "Cantidad de pedidos (en miles)"

    # Language-independent variables
    product_category_key = "ProductCategoryKey"
    product_subcategory_key = "ProductSubcategoryKey"
    product_key = "ProductKey"
    sales_order_number = "SalesOrderNumber"
    return (
        fy_dates_title,
        input_channel_internet_label,
        input_channel_resellers_label,
        input_data_source_label,
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
def _(input_data_source_label):
    input_data_source = mo.ui.radio(
        options={
            "CSV": "0",
            "SQL": "1",
        },
        value="CSV",
        label=input_data_source_label,
    )
    return (input_data_source,)


@app.cell
def _(input_data_source):
    input_data_source
    return


@app.cell
def con_settings(input_data_source):
    # Settings for database and csv connections

    con = ibis.duckdb.connect()
    ibis.set_backend(con)
    ibis.options.interactive = True

    csv_path = os.environ["CSV_PATH"]
    date_csv = rf"{csv_path}\Date.csv"
    product_category_csv = rf"{csv_path}\ProductCategory.csv"
    product_subcategory_csv = rf"{csv_path}\ProductSubcategory.csv"
    product_csv = rf"{csv_path}\Product.csv"
    internet_sales_csv = rf"{csv_path}\InternetSales.csv"
    reseller_sales_csv = rf"{csv_path}\ResellerSales.csv"

    if input_data_source.value == "0":
        dscon = con

        csv_table_date = con.read_csv(
            date_csv,
            auto_detect=False,
            header=True,
            dateformat="%Y-%m-%d",
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "DateKey": "INT32",
                "FullDateAlternateKey": "DATE",
                "DayNumberOfWeek": "INT8",
                "EnglishDayNameOfWeek": "VARCHAR",
                "SpanishDayNameOfWeek": "VARCHAR",
                "FrenchDayNameOfWeek": "VARCHAR",
                "DayNumberOfMonth": "INT8",
                "DayNumberOfYear": "INT16",
                "WeekNumberOfYear": "INT8",
                "EnglishMonthName": "VARCHAR",
                "SpanishMonthName": "VARCHAR",
                "FrenchMonthName": "VARCHAR",
                "MonthNumberOfYear": "INT8",
                "CalendarQuarter": "INT8",
                "CalendarYear": "INT16",
                "CalendarSemester": "INT8",
                "FiscalQuarter": "INT8",
                "FiscalYear": "INT32",
                "FiscalSemester": "INT8",
            },
        )

        csv_table_product_category = con.read_csv(
            product_category_csv,
            auto_detect=False,
            header=True,
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "ProductCategoryKey": "INT8",
                "ProductCategoryAlternateKey": "INT8",
                "EnglishProductCategoryName": "VARCHAR",
                "SpanishProductCategoryName": "VARCHAR",
                "FrenchProductCategoryName": "VARCHAR",
            },
        )

        csv_table_product_subcategory = con.read_csv(
            product_subcategory_csv,
            auto_detect=False,
            header=True,
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "ProductSubcategoryKey": "INT8",
                "ProductSubcategoryAlternateKey": "INT8",
                "EnglishProductSubcategoryName": "VARCHAR",
                "SpanishProductSubcategoryName": "VARCHAR",
                "FrenchProductSubcategoryName": "VARCHAR",
                "ProductCategoryKey": "INT8",
            },
        )

        csv_table_product = con.read_csv(
            product_csv,
            auto_detect=False,
            header=True,
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "ProductKey": "INT16",
                "ProductAlternateKey": "VARCHAR",
                "ProductSubcategoryKey": "INT8",
                "WeightUnitMeasureCode": "VARCHAR",
                "SizeUnitMeasureCode": "VARCHAR",
                "EnglishProductName": "VARCHAR",
                "SpanishProductName": "VARCHAR",
                "FrenchProductName": "VARCHAR",
                "StandardCost": "VARCHAR",
                "FinishedGoodsFlag": "BOOLEAN",
                "Color": "VARCHAR",
                "SafetyStockLevel": "INT16",
                "ReorderPoint": "INT16",
                "ListPrice": "VARCHAR",
                "Size": "VARCHAR",
                "SizeRange": "VARCHAR",
                "Weight": "VARCHAR",
                "DaysToManufacture": "INT16",
                "ProductLine": "VARCHAR",
                "DealerPrice": "VARCHAR",
                "Class": "VARCHAR",
                "Style": "VARCHAR",
                "ModelName": "VARCHAR",
                "EnglishDescription": "VARCHAR",
                "FrenchDescription": "VARCHAR",
                "StartDate": "DATE",
                "EndDate": "VARCHAR",
                "Status": "VARCHAR",
            },
        )

        csv_table_internet_sales = con.read_csv(
            internet_sales_csv,
            auto_detect=False,
            header=True,
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "SalesOrderNumber": "VARCHAR",
                "SalesOrderLineNumber": "INT8",
                "CustomerKey": "INT32",
                "ProductKey": "INT16",
                "OrderDateKey": "INT32",
                "DueDateKey": "INT32",
                "ShipDateKey": "INT32",
                "PromotionKey": "INT8",
                "CurrencyKey": "INT16",
                "SalesTerritoryKey": "INT8",
                "OrderQuantity": "INT8",
                "UnitPrice": "DECIMAL(13, 2)",
                "ExtendedAmount": "DECIMAL(13, 2)",
                "UnitPriceDiscountPct": "VARCHAR",
                "DiscountAmount": "INT8",
                "ProductStandardCost": "DECIMAL(13, 2)",
                "TotalProductCost": "DECIMAL(13, 2)",
                "SalesAmount": "DECIMAL(13, 2)",
                "TaxAmount": "DECIMAL(13, 2)",
                "FreightAmount": "DECIMAL(13, 2)",
                "CarrierTrackingNumber": "VARCHAR",
                "CustomerPONumber": "VARCHAR",
                "RevisionNumber": "INT8",
            },
        )

        csv_table_reseller_sales = con.read_csv(
            reseller_sales_csv,
            auto_detect=False,
            header=True,
            decimal_separator=",",
            delim=";",
            encoding="utf-8",
            nullstr="NULL",
            columns={
                "SalesOrderNumber": "VARCHAR",
                "SalesOrderLineNumber": "INT8",
                "ResellerKey": "INT32",
                "ProductKey": "INT16",
                "OrderDateKey": "INT32",
                "DueDateKey": "INT32",
                "ShipDateKey": "INT32",
                "EmployeeKey": "INT16",
                "PromotionKey": "INT8",
                "CurrencyKey": "INT8",
                "SalesTerritoryKey": "INT8",
                "OrderQuantity": "INT8",
                "UnitPrice": "DECIMAL(13, 2)",
                "ExtendedAmount": "DECIMAL(13, 2)",
                "UnitPriceDiscountPct": "VARCHAR",
                "DiscountAmount": "DECIMAL(13, 2)",
                "ProductStandardCost": "DECIMAL(13, 2)",
                "TotalProductCost": "DECIMAL(13, 2)",
                "SalesAmount": "DECIMAL(13, 2)",
                "TaxAmount": "DECIMAL(13, 2)",
                "FreightAmount": "DECIMAL(13, 2)",
                "CarrierTrackingNumber": "VARCHAR",
                "CustomerPONumber": "VARCHAR",
                "RevisionNumber": "INT8",
            },
        )

        table_date = dscon.create_table("DimDate", csv_table_date)
        table_product_category = dscon.create_table(
            "DimProductCategory", csv_table_product_category
        )
        table_product_subcategory = dscon.create_table(
            "DimProductSubcategory", csv_table_product_subcategory
        )
        table_product = dscon.create_table("DimProduct", csv_table_product)
        table_sales_reseller = dscon.create_table(
            "FactResellerSales", csv_table_reseller_sales
        )
        table_sales_internet = dscon.create_table(
            "FactInternetSales", csv_table_internet_sales
        )

    elif input_data_source.value == "1":
        dscon = ibis.mssql.connect(
            user=os.environ["SQLSERVER_USER"],
            password=os.environ["SQLSERVER_PASS"],
            host=os.environ["SQLSERVER_HOST"],
            database=os.environ["SQLSERVER_DB"],
            driver="SQL Server",
            port=os.environ["SQLSERVER_PORT"],
        )

        # Dimension tables
        table_date = "DimDate"
        table_product_category = "DimProductCategory"
        table_product_subcategory = "DimProductSubcategory"
        table_product = "DimProduct"

        # Fact tables
        table_sales_reseller = "FactResellerSales"
        table_sales_internet = "FactInternetSales"

        # Quick queries for data exploration
        qq_table_date = dscon.sql(f"SELECT * FROM {table_date}")
        qq_table_product_category = dscon.sql(
            f"SELECT * FROM {table_product_category}"
        )
        qq_table_product_subcategory = dscon.sql(
            f"SELECT * FROM {table_product_subcategory}"
        )
        qq_table_product = dscon.sql(f"SELECT * FROM {table_product}")
        qq_table_sales_reseller = dscon.sql(
            f"SELECT * FROM {table_sales_reseller}"
        )
        qq_table_sales_internet = dscon.sql(
            f"SELECT * FROM {table_sales_internet}"
        )
    return (
        dscon,
        table_date,
        table_product,
        table_product_category,
        table_product_subcategory,
        table_sales_internet,
        table_sales_reseller,
    )


@app.cell
def relationships():
    # Table relationships for use in inputs, filtering and analysis

    # category_subcategory_product = dscon.sql(f"""
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
    dscon,
    input_data_source,
    input_product_category_label,
    product_category_key,
    product_category_name,
    table_product_category,
):
    # Generate a list of product categories in the DB to use as filtering criteria
    if input_data_source.value == "0":
        list_category = (
            table_product_category.select(
                [product_category_key, product_category_name]
            )
            .distinct()
            .order_by(product_category_key)
            .execute()
        )

    elif input_data_source.value == "1":
        list_category = dscon.sql(f"""
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
    dscon,
    input_data_source,
    input_product_category,
    input_product_subcategory_label,
    product_category_key,
    product_category_name,
    product_subcategory_key,
    product_subcategory_name,
    table_product_category,
    table_product_subcategory,
):
    # Generate a list of product subcategories in the DB to use as filtering criteria
    if input_data_source.value == "0":
        selected_categories = input_product_category.value

        selected_category_keys = table_product_category.filter(
            table_product_category[product_category_name].isin(selected_categories)
        ).select(product_category_key, product_category_name)

        list_subcategory = (
            selected_category_keys.join(
                table_product_subcategory, [product_category_key], how="left"
            )
            .select(
                table_product_subcategory[product_subcategory_key],
                table_product_subcategory[product_subcategory_name],
            )
            .execute()
        )

    elif input_data_source.value == "1":
        selected_categories = "', '".join(input_product_category.value)

        list_subcategory = dscon.sql(f"""
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
    dscon,
    input_data_source,
    input_product_label,
    input_product_subcategory,
    product_key,
    product_name,
    product_subcategory_key,
    product_subcategory_name,
    table_product,
    table_product_subcategory,
):
    # Generate a list of products in the DB to use as filtering criteria
    if input_data_source.value == "0":
        selected_subcategories = input_product_subcategory.value

        selected_subcategory_keys = table_product_subcategory.filter(
            table_product_subcategory[product_subcategory_name].isin(
                selected_subcategories
            )
        ).select(product_subcategory_key, product_subcategory_name)

        list_product = (
            selected_subcategory_keys.join(
                table_product, [product_subcategory_key], how="left"
            )
            .select(
                table_product[product_key],
                table_product[product_name],
            )
            .execute()
        )

    elif input_data_source.value == "1":
        selected_subcategories = "', '".join(input_product_subcategory.value)
        list_product = dscon.sql(f"""
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
    input_product,
    input_product_category,
    input_product_subcategory,
    input_product_title,
    input_sales_channel_title,
):
    mo.vstack(
        [
            # input_language,
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
def fy_dates(
    dscon,
    input_data_source,
    input_fiscal_year,
    locale_date,
    table_date,
):
    current_fy = int(input_fiscal_year.value)
    previous_fy = current_fy - 1

    # Fiscal year dates
    if input_data_source.value == "0":
        min_date_key = table_date.filter(table_date["FiscalYear"] == current_fy)[
            "DateKey"
        ].min()
        max_date_key = table_date.filter(table_date["FiscalYear"] == current_fy)[
            "DateKey"
        ].max()

        fy_min_dates = table_date.filter(
            table_date["DateKey"] == min_date_key
        ).select(
            table_date["DayNumberOfMonth"].name("StartDay"),
            table_date["MonthNumberOfYear"].name("StartMonth"),
            table_date["CalendarYear"].name("StartYear"),
        )

        fy_max_dates = table_date.filter(
            table_date["DateKey"] == max_date_key
        ).select(
            table_date["DayNumberOfMonth"].name("EndDay"),
            table_date["MonthNumberOfYear"].name("EndMonth"),
            table_date["CalendarYear"].name("EndYear"),
        )

    if input_data_source.value == "1":
        fy_min_dates = dscon.sql(f"""
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
        """)

        fy_max_dates = dscon.sql(f"""
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
        """)

    fy_start_day = fy_min_dates["StartDay"].as_scalar().execute()
    fy_start_month = fy_min_dates["StartMonth"].as_scalar().execute()
    fy_start_year = fy_min_dates["StartYear"].as_scalar().execute()

    fy_end_day = fy_max_dates["EndDay"].as_scalar().execute()
    fy_end_month = fy_max_dates["EndMonth"].as_scalar().execute()
    fy_end_year = fy_max_dates["EndYear"].as_scalar().execute()

    fy_start_date = locale_date(date(fy_start_year, fy_start_month, fy_start_day))
    fy_end_date = locale_date(date(fy_end_year, fy_end_month, fy_end_day))
    return current_fy, fy_end_date, fy_start_date, previous_fy


@app.cell
def sales_profit_volume(
    current_fy,
    dscon,
    input_channel_internet,
    input_channel_resellers,
    input_data_source,
    input_product,
    locale_decimal,
    previous_fy,
    product_key,
    product_name,
    sales_order_number,
    table_date,
    table_product,
    table_sales_internet,
    table_sales_reseller,
):
    # Channel sales and profit
    if input_data_source.value == "0":
        selected_products = input_product.value

        if input_channel_internet.value == True:
            filtered_internet_sales = table_sales_internet.join(
                table_date,
                table_sales_internet["OrderDateKey"] == table_date["DateKey"],
            ).filter(table_date["FiscalYear"].isin([current_fy, previous_fy]))
            filtered_products = table_product.filter(
                table_product[product_name].isin(selected_products)
            )

            channel_internet = (
                filtered_internet_sales.filter(
                    filtered_internet_sales[product_key].isin(
                        filtered_products[product_key]
                    )
                )
            )

            channel_internet = channel_internet.group_by(["FiscalYear"]).aggregate(
                InternetSales=ibis.case()
                .when(channel_internet.count() == 0, 0)
                .else_(
                    channel_internet["SalesAmount"]
                    .fill_null(0)
                    .sum()
                    .round(0)
                    .cast(dt.Decimal(13, 2))
                )
                .end(),
                InternetProfit=ibis.case()
                .when(channel_internet.count() == 0, 0)
                .else_(
                    (
                        channel_internet["SalesAmount"].fill_null(0)
                        - channel_internet["TotalProductCost"].fill_null(0)
                    )
                    .sum()
                    .round(0)
                    .cast(dt.Decimal(13, 2))
                )
                .end(),
                OrderVolume=channel_internet[sales_order_number].nunique(),
            )
        else:
            current_sales_channel_internet = 0
            current_profit_channel_internet = 0
            previous_sales_channel_internet = 0
            previous_profit_channel_internet = 0
            current_volume_channel_internet = 0
            previous_volume_channel_internet = 0

        if input_channel_internet.value == True:
            filtered_reseller_sales = table_sales_reseller.join(
                table_date,
                table_sales_reseller["OrderDateKey"] == table_date["DateKey"],
            ).filter(table_date["FiscalYear"].isin([current_fy, previous_fy]))
            filtered_products = table_product.filter(
                table_product[product_name].isin(selected_products)
            )

            channel_resellers = (
                filtered_reseller_sales.filter(
                    filtered_reseller_sales[product_key].isin(
                        filtered_products[product_key]
                    )
                )
            )

            channel_resellers = channel_resellers.group_by(["FiscalYear"]).aggregate(
                ResellerSales=ibis.case()
                .when(channel_resellers.count() == 0, 0)
                .else_(
                    channel_resellers["SalesAmount"]
                    .fill_null(0)
                    .sum()
                    .round(0)
                    .cast(dt.Decimal(13, 2))
                )
                .end(),
                ResellerProfit=ibis.case()
                .when(channel_resellers.count() == 0, 0)
                .else_(
                    (
                        channel_resellers["SalesAmount"].fill_null(0)
                        - channel_resellers["TotalProductCost"].fill_null(0)
                    )
                    .sum()
                    .round(0)
                    .cast(dt.Decimal(13, 2))
                )
                .end(),
                OrderVolume=channel_resellers[sales_order_number].nunique(),
            )
        else:
            current_sales_channel_internet = 0
            current_profit_channel_internet = 0
            previous_sales_channel_internet = 0
            previous_profit_channel_internet = 0
            current_volume_channel_internet = 0
            previous_volume_channel_internet = 0

    elif input_data_source.value == "1":
        selected_products = "', '".join(
            product.replace("'", "''") for product in input_product.value
        )

        if input_channel_internet.value == True:
            channel_internet = dscon.sql(f"""
                SELECT
                    FiscalYear,
                    CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2))
                        END AS InternetSales,
                    CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2))
                        END AS InternetProfit,
                    COUNT(DISTINCT {sales_order_number}) AS OrderVolume
                FROM {table_sales_internet}
                JOIN {table_date}
                ON {table_sales_internet}.OrderDateKey = {table_date}.DateKey
                WHERE {table_date}.FiscalYear IN (
                    {current_fy},
                    {previous_fy}
                )
                AND {table_sales_internet}.{product_key} IN (
                    SELECT {product_key}
                    FROM {table_product}
                    WHERE {product_name} IN ('{selected_products}')
                )
                GROUP BY FiscalYear
                """)
        else:
            current_sales_channel_internet = 0
            current_profit_channel_internet = 0
            previous_sales_channel_internet = 0
            previous_profit_channel_internet = 0
            current_volume_channel_internet = 0
            previous_volume_channel_internet = 0

        if input_channel_resellers.value == True:
             channel_resellers = dscon.sql(f"""
                SELECT
                    FiscalYear,
                    CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0)), 0) AS DECIMAL(13, 2))
                        END AS ResellerSales,
                    CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE CAST(ROUND(SUM(COALESCE(SalesAmount, 0) - COALESCE(TotalProductCost, 0)), 0) AS DECIMAL(13, 2))
                        END AS ResellerProfit,
                    COUNT(DISTINCT {sales_order_number}) AS OrderVolume
                FROM {table_sales_reseller}
                JOIN {table_date}
                ON {table_sales_reseller}.OrderDateKey = {table_date}.DateKey
                WHERE {table_date}.FiscalYear IN (
                    {current_fy},
                    {previous_fy}
                    )
                AND {table_sales_reseller}.{product_key} IN (
                    SELECT {product_key}
                    FROM {table_product}
                    WHERE {product_name} IN ('{selected_products}')
                )
                GROUP BY FiscalYear
                """)
        else:
            current_sales_channel_resellers = 0
            current_profit_channel_resellers = 0
            previous_sales_channel_resellers = 0
            previous_profit_channel_resellers = 0
            current_volume_channel_resellers = 0
            previous_volume_channel_resellers = 0

    current_sales_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == current_fy)
        .select("InternetSales")
        .as_scalar()
        .execute()
    )
    current_profit_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == current_fy)
        .select("InternetProfit")
        .as_scalar()
        .execute()
    )
    current_volume_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == current_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute()
    )
    previous_sales_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == previous_fy)
        .select("InternetSales")
        .as_scalar()
        .execute()
    )
    previous_profit_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == previous_fy)
        .select("InternetProfit")
        .as_scalar()
        .execute()
    )
    previous_volume_channel_internet = (
        channel_internet.filter(channel_internet["FiscalYear"] == previous_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute()
    )
    if previous_sales_channel_internet is None:
        previous_sales_channel_internet = 0
    if previous_profit_channel_internet is None:
        previous_profit_channel_internet = 0
    if previous_volume_channel_internet is None:
        previous_volume_channel_internet = 0

    current_sales_channel_resellers = (
        channel_resellers.filter(channel_resellers["FiscalYear"] == current_fy)
        .select("ResellerSales")
        .as_scalar()
        .execute()
    )
    current_profit_channel_resellers = (
        channel_resellers.filter(channel_resellers["FiscalYear"] == current_fy)
        .select("ResellerProfit")
        .as_scalar()
        .execute()
    )
    current_volume_channel_resellers = (
        channel_resellers.filter(channel_resellers["FiscalYear"] == current_fy)
        .select("OrderVolume")
        .as_scalar()
        .execute()
    )
    previous_sales_channel_resellers = (
        channel_resellers.filter(
            channel_resellers["FiscalYear"] == previous_fy
        )
        .select("ResellerSales")
        .as_scalar()
        .execute()
    )
    previous_profit_channel_resellers = (
        channel_resellers.filter(
            channel_resellers["FiscalYear"] == previous_fy
        )
        .select("ResellerProfit")
        .as_scalar()
        .execute()
    )
    previous_volume_channel_resellers = (
        channel_resellers.filter(
            channel_resellers["FiscalYear"] == previous_fy
        )
        .select("OrderVolume")
        .as_scalar()
        .execute()
    )
    if previous_sales_channel_resellers is None:
        previous_sales_channel_resellers = 0
    if previous_profit_channel_resellers is None:
        previous_profit_channel_resellers = 0
    if previous_volume_channel_resellers is None:
        previous_volume_channel_resellers = 0

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
    current_volume_channel_all = Decimal(
        current_volume_channel_internet + current_volume_channel_resellers
    )
    previous_volume_channel_all = Decimal(
        previous_volume_channel_internet + previous_volume_channel_resellers
    )

    if previous_sales_channel_all == 0:
        sales_yoy = "N/A"
    else:
        sales_yoy = str(
            round(
                (current_sales_channel_all / previous_sales_channel_all) * 100
                - 100,
                2,
            )
        )
    if previous_profit_channel_all == 0:
        profit_yoy = "N/A"
    else:
        profit_yoy = str(
            round(
                (current_profit_channel_all / previous_profit_channel_all) * 100
                - 100,
                2,
            )
        )
    if previous_volume_channel_all == 0:
        volume_yoy = "N/A"
    else:
        volume_yoy = str(
            round(
                (current_volume_channel_all / previous_volume_channel_all) * 100
                - 100,
                2,
            )
        )

    sales_millions = str(round(current_sales_channel_all / 1_000_000, 2))
    profit_millions = str(round(current_profit_channel_all / 1_000_000, 2))
    current_volume_thousands = str(round(current_volume_channel_all / 1_000, 2))

    # Format results with thousands and decimal separators
    current_sales_channel_all, current_profit_channel_all = (
        locale_decimal(current_sales_channel_all),
        locale_decimal(current_profit_channel_all),
    )
    return (
        current_volume_thousands,
        filtered_internet_sales,
        profit_millions,
        sales_millions,
    )


@app.cell
def _(fy_dates_title, fy_end_date, fy_start_date):
    mo.Html(
        f"""
        <h4 style="color: white; background-color: #41A4FF; padding: 5px;">
            <strong>{fy_dates_title}</strong> {fy_start_date} - {fy_end_date}
        </h4>
        """
    )
    return


@app.cell
def _(sales_millions, sales_millions_label):
    mo.Html(
        f"""
        <div style="background-color: white; color: #41A4FF; padding: 5px;">
            <strong>{sales_millions_label}</strong><br>
            {sales_millions}M
        </div>
        """
    )
    return


@app.cell
def _(profit_millions, profit_millions_label):
    mo.Html(
        f"""
        <div style="background-color: white; color: #41A4FF; padding: 5px;">
            <strong>{profit_millions_label}</strong><br>
            {profit_millions}M
        </div>
        """
    )
    return


@app.cell
def _(current_volume_thousands, volume_thousands_label):
    mo.Html(
        f"""
        <div style="background-color: white; color: #41A4FF; padding: 5px;">
            <strong>{volume_thousands_label}</strong><br>
            {current_volume_thousands}K
        </div>
        """ 
    )
    return


@app.cell
def _(filtered_internet_sales):
    current_year_sales = filtered_internet_sales 
    return (current_year_sales,)


@app.cell
def _(current_year_sales):
    mo.ui.table(current_year_sales)
    return


@app.cell
def _(current_year_sales):

    # # Group sales by date and sum SalesAmount
    # sales_by_date = current_year_sales.group_by("FullDateAlternateKey").aggregate(
    #     total_sales=current_year_sales["SalesAmount"].sum()
    # )

    # # Convert Ibis expression to Pandas DataFrame
    # sales_df = sales_by_date.execute()

    # # Ensure the date column is properly formatted
    # sales_df["FullDateAlternateKey"] = pd.to_datetime(sales_df["FullDateAlternateKey"])
    # sales_df["total_sales"] = sales_df["total_sales"].astype(float)  # Convert Decimal to float


    # Extract the month and year, then group by them
    sales_by_month = current_year_sales.mutate(
        Year=current_year_sales["FullDateAlternateKey"].year(),
        Month=current_year_sales["FullDateAlternateKey"].month()
    ).group_by(["Year", "Month"]).aggregate(
        total_sales=current_year_sales["SalesAmount"].sum()
    )

    # Convert Ibis result to Pandas DataFrame for visualization
    sales_df = sales_by_month.execute()
    sales_df["YearMonth"] = pd.to_datetime(sales_df[["Year", "Month"]].astype(str).agg("-".join, axis=1))  # Create Year-Month column
    sales_df = sales_df.sort_values("YearMonth")  # Sort for proper plotting
    return (sales_df,)


@app.cell
def _(sales_df):

    # # Create the area chart
    # chart = (
    #     alt.Chart(sales_df)
    #     .mark_area(color="#41A4FF", opacity=0.5)  # Light blue fill
    #     .encode(
    #         x=alt.X("FullDateAlternateKey:T", title="Date"),
    #         y=alt.Y("total_sales:Q", title="Total Sales Amount"),
    #         tooltip=["FullDateAlternateKey:T", "total_sales:Q"]
    #     )
    #     .properties(title="Sales Over the Year")
    # )

    # Convert sales data to float for Altair compatibility
    sales_df["total_sales"] = sales_df["total_sales"].astype(float)

    # Create a custom ordering list for fiscal year (July to June)
    fiscal_months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun"]

    # Extract month names for x-axis labeling
    sales_df["Month"] = sales_df["YearMonth"].dt.strftime("%b")  # Convert dates to abbreviated months (Jul, Aug, etc.)

    # Create the area chart with fiscal year ordering
    chart = (
        alt.Chart(sales_df)
        .mark_area(color="#41A4FF", opacity=0.5)  # Light blue fill
        .encode(
            x=alt.X("Month:N", title="Month", sort=fiscal_months),  # Enforce custom fiscal order
            y=alt.Y("total_sales:Q", title="Total Sales Amount"),
            tooltip=["Month:N", "total_sales:Q"]
        )
        .properties(title="Fiscal Year Sales Trend (July - June)")
    )

    return (chart,)


@app.cell
def _(chart):
    chart
    return


if __name__ == "__main__":
    app.run()
