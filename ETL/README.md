## Overview
В представленном код реализован ETL-Пайплайн, который выгружает из ClickHouse данные из двух таблиц, объединяет их, считает метрики по полученным данным и в конце выгружает их в отдельную таблицу обратно в ClickHouse, таблица обновляется ежедневно.

DAG запускается каждый день в 07:00, количество попыток выполнить DAG - 2, интервал между запусками - 5 минут. 

---

The presented code implements an ETL Pipeline, which downloads data from two tables from ClickHouse, combines them, calculates metrics based on the received data, and finally uploads them to a separate table back to ClickHouse, the table is updated daily.

DAG runs every day at 07:00, the number of attempts to execute DAG is 2, the interval between starts is 5 minutes.
