ETL-Пайплайн выгружает из ClickHouse данные из двух таблиц, объединяет их, считает метрики по полученным данным и в конце выгружает их в отдельную таблицу обратно в ClickHouse, таблица обновляется ежедневно.
DAG запускается каждый день в 07:00, количество попыток выполнить DAG - 2, интервал между запусками - 5 минут. 

---

The ETL pipeline extracts data from two tables in ClickHouse, merges them, calculates metrics based on the obtained data, and finally reloads them back into a separate table in ClickHouse. The table is updated daily.
The DAG is scheduled to run every day at 07:00. The number of execution attempts for the DAG is set to 2, with a 5-minute interval between retries.
