from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'Axel Aleman',
    'start_date': days_ago(7)
}

dag_args = {
    'dag_id': 'BigQueryML_Airquality',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}

with DAG(**dag_args) as dag:

    cargar_datos_clima = GCSToBigQueryOperator(
        task_id='cargar_datos_clima',
        bucket='air_quality_ml',
        source_objects=['*_bme280sof.csv'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table='bustling-surf-310323.airquality.weather_table',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    cargar_datos_contaminacion = GCSToBigQueryOperator(
        task_id='cargar_datos_contaminacion',
        bucket='air_quality_ml',
        source_objects=['*_sds011sof.csv'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table='bustling-surf-310323.airquality.pollution_table',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )


    crear_tabla_airquality_data_query = '''
        select      w.lat,
            w.lon,
            w.timestamp,
            EXTRACT(DAY from w.timestamp) as day,
            EXTRACT(MONTH from w.timestamp) as month,
            EXTRACT(YEAR from w.timestamp) as year,
            EXTRACT(HOUR from w.timestamp) as hour,
            CASE
               WHEN EXTRACT(MONTH from w.timestamp) >= 6 AND EXTRACT(MONTH from w.timestamp) <=9 THEN 1
               ELSE 0
            END AS IS_SUMMER,
            CASE
               WHEN EXTRACT(HOUR from w.timestamp) >= 20 and EXTRACT(HOUR from w.timestamp) <= 6 THEN 1
               ELSE 0
            END as IS_NIGHT,
            w.pressure,
            w.temperature,
            w.humidity,
            p.P1,
            p.P2,
            rand() as split_column
        from `bustling-surf-310323.airquality.weather_table` w
        inner join `bustling-surf-310323.airquality.pollution_table` p
        on w.timestamp = p.timestamp and w.location = p.location
        where w.lat is not null and w.lon is not null and w.timestamp is not null
        and w.pressure is not null and w.temperature is not null and w.humidity is not null
        and p.P1 is not null and p.P2 is not null
        '''
    

    tabla_airquality = BigQueryExecuteQueryOperator(
        task_id='tabla_airquality',
        sql=crear_tabla_airquality_data_query,
        destination_dataset_table='bustling-surf-310323.airquality.airquality_data',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us',
        bigquery_conn_id='google_cloud_default'
    )

    crear_modelo_query = '''
        CREATE MODEL
        `bustling-surf-310323.airquality.reg_lineal_pollution_p1` OPTIONS( model_type='LINEAR_REG',
            DATA_SPLIT_METHOD='SEQ',
            DATA_SPLIT_COL='split_column',
            DATA_SPLIT_EVAL_FRACTION=0.7,
            input_label_cols=['P1'] ) AS
        SELECT
        lat,
        lon,
        year,
        month,
        day,
        hour,
        pressure,
        temperature,
        humidity,
        IS_SUMMER,
        IS_NIGHT,
        P1,
        P2,
        split_column
        FROM
        `bustling-surf-310323.airquality.airquality_data`
    '''
    

    crear_modelo_bqml = BigQueryExecuteQueryOperator(
        task_id='crear_modelo_bqml',
        sql=crear_modelo_query,
        use_legacy_sql=False,
        location='us',
        bigquery_conn_id='google_cloud_default'
    )

    hacer_predicciones_query = '''
        select *
        from ml.predict(
            model airquality.reg_lineal_pollution_p1,
            (
                select
                    lat,
                    lon,
                    year,
                    month,
                    day,
                    hour,
                    pressure,
                    temperature,
                    humidity,           
                    IS_SUMMER,
                    IS_NIGHT,
                    P1,
                    P2
                from airquality.airquality_data
                where split_column >= 0.7
            )
        )
    '''
    

    tabla_predicciones = BigQueryExecuteQueryOperator(
        task_id='tabla_predicciones',
        sql=hacer_predicciones_query,
        destination_dataset_table='bustling-surf-310323.airquality.airquality_pred',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us',
        bigquery_conn_id='google_cloud_default'
    )

    hacer_evaluacion_query = '''
        select *
        from ml.evaluate(model airquality.reg_lineal_pollution_p1)
    '''
    

    tabla_evaluacion = BigQueryExecuteQueryOperator(
        task_id='tabla_evaluacion',
        sql=hacer_evaluacion_query,
        destination_dataset_table='bustling-surf-310323.airquality.airquality_eval',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us',
        bigquery_conn_id='google_cloud_default'
    )

cargar_datos_clima >> tabla_airquality
cargar_datos_contaminacion >> tabla_airquality 
tabla_airquality >> crear_modelo_bqml >> tabla_predicciones >> tabla_evaluacion
