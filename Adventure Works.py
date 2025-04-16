import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def imports():
    import marimo as mo
    import ibis
    return ibis, mo


@app.cell
def settings(ibis):
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

    con.sql("SELECT * FROM DimDate").execute()
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
def _(mo):
    _df = mo.sql(
        f"""

        """
    )
    return


if __name__ == "__main__":
    app.run()
