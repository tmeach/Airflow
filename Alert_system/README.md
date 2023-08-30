Система проверяет ключевые метрики в продукте (DAU, WAU, MAU, CTR, LPU) с помощью статистических методов детектирования аномалий. В случае обнаружения аномального значения в чат в Telegram отправляется сообщение с названием метрики, ее значением, величиной отклонения, и ссылкой на дашборд в Apache Superset. 
DAG выполняется каждый день в 11:00, количество попыток выполнить DAG - 2, интервал между запусками - 5 минут.

--- 

The system monitors key metrics in the product (DAU, WAU, MAU, CTR, LPU) using statistical anomaly detection methods. Upon detecting an anomalous value, a message is sent to a Telegram chat containing the metric's name, its value, deviation magnitude, and a link to the dashboard in Apache Superset. The DAG is executed every day at 11:00. The number of execution attempts for the DAG is set to 2, with a 5-minute interval between retries.
