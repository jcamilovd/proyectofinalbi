import pandas as pd
import psycopg2
import json
import io

def almacenarDatos():
    try:
        # creamos las variables de conexion a la DB
        PSQL_HOST = "dbetl-bi.c8w24k27veqn.us-east-1.rds.amazonaws.com"
        PSQL_PORT = "5432"
        PSQL_USER = "postgres"
        PSQL_PASS = "qwer1234"
        PSQL_DB = "postgres"

        # traemos los path
        pathEmpresas = './files/alphavantage/detalleEmpresas/detalleEmpresas-20220609423242.json'
        pathBalances = './files/alphavantage/banlancesAnuales/banlancesAnuales-20220609423242.json'
        pathGanancias = './files/alphavantage/gananciasAnuales/gananciasAnuales-20220609423242.json'
        pathHistoricos = './files/alphavantage/historicos/historicos-20220609423242.json'
        pathMoneda = './files/alphavantage/cambioMoneda/cambioMoneda-20220609423242.json'
        scDetalleEmpresa='./scripts/detalle_empresa.txt'

        detalleEmpresas = pd.read_json(pathEmpresas, orient='records')
        balancesAnuales = pd.read_json(pathBalances, orient='records')
        gananciasAnuales = pd.read_json(pathGanancias, orient='records')
        historicos = pd.read_json(pathHistoricos, orient='records')
        cambiosMoneda = pd.read_json(pathMoneda, orient='records')
        tblDetalleEmpresa="""create table detalle_empresa(
            symbol varchar(5) primary key,	asset_type varchar(250),	name varchar(250),	descripcion varchar(4000),	exchange varchar(10),	currency varchar(5),	county varchar(5),
            sector varchar(250),	industry varchar(250),	address varchar(250),	fulltimeemployees int,	fiscal_year_end varchar(10),	latest_quarter varchar(15),
            market_capitalization bigint,	ebitda varchar(50),	pe_ratio float,    peg_ratio float,	book_value float,	dividend_perShare float,	dividend_yield float,
            eps float,	revenue_per_share_ttm float,	profit_margin float,    operating_margin_ttm float,	return_onAssets_ttm float,	return_on_equity_ttm float,
            revenue_ttm bigint,	gross_profit_ttm bigint,	diluted_EPSttm float,	quarterly_earnings_growth_YOY float,	quarterly_revenue_growth_YOY float,	analyst_target_price float,
            trailing_pe float,	forward_df float,	price_to_sales_ratio_TTM float,	price_to_book_ratio float,	ev_to_revenue float,	ev_to_ebitda float,	beta float,	week_high_52 float,
            week_low_52 float,	day_moving_average_50 float,	day_moving_average_200 float,	shares_outstanding bigint,	shares_float bigint,	shares_short bigint,	shares_short_prior_month bigint,
            short_ratio float,	short_percent_outstanding float,	short_percent_float float,	percent_insiders float,	percent_institutions float,	forward_annual_dividend_rate float,
            forward_annual_dividend_yield float,	payout_ratio float,	dividend_date varchar(15),	ex_dividend_date varchar(15),	last_split_factor varchar(10),
            last_split_date varchar(15))"""

        # pasamos los archivos JSON a un dataFrame para ser almacenados en la DB
        dfEmpresas = pd.DataFrame(detalleEmpresas)
        dfBalances = pd.DataFrame(balancesAnuales)
        dfGanancias = pd.DataFrame(gananciasAnuales)
        dfHistoricos = pd.DataFrame(historicos)
        dfMoneda = pd.DataFrame(cambiosMoneda)

        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (
        PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()

        # almacenamos los datos en base de datos, copiando el dataframe directamente a las tablas
        # ouEmpresas = io.StringIO()
        # dfEmpresas.to_csv(ouEmpresas, sep='\t', header=False, index=False)
        # ouEmpresas.seek(0)
        # contentsE = ouEmpresas.getvalue()
        # cur.copy_from(ouEmpresas, 'detalle_empresa', null="")  # null values become ''

        with open(pathEmpresas) as file:
            # change json.load(file) to file.read()
            data_json = json.load(file)

        if type(data_json) == list:
            first_record = data_json[0]

        # get the column names from the first record
        columns = list(first_record.keys())
        print("\ncolumn names:", columns)

        columns = [list(x.keys()) for x in data_json][0]


        table_name = "detalle_empresa"
        sql_string = 'INSERT INTO {} '.format(table_name)
        sql_string += "\nVALUES "

        # enumerate over the record
        for i, record_dict in enumerate(data_json):

            # iterate over the values of each record dict object
            values = []
            for col_names, val in record_dict.items():

                # Postgres strings must be enclosed with single quotes
                if type(val) == str:
                    # escape apostrophies with two single quotations
                    val = val.replace("'", "''")
                    val = "'" + val + "'"

                values += [str(val)]

            # join the list of values and enclose record in parenthesis
            sql_string += "(" + ', '.join(values) + "),\n"

        # remove the last comma and end statement with a semicolon
        sql_string = sql_string[:-2] + ";"

        cur.execute(tblDetalleEmpresa)
        cur.execute(sql_string)
        # Ejecutamos el commit
        conn.commit()

        # Cerrar la conexión con la base de datos
        cur.close()
        conn.close()

        # Confirmamos la ejecusion
        print("Datos almacenadas en DB")

    except psycopg2.Error as e:
        print("Error de base de datos: ", e)

almacenarDatos()
