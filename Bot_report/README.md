Проект по автоматизации отчетности в приложении: лента новостей и сервис отправки сообщений. Первый скрипт собирает отчет отдельно по ленте новосетй: текст с информацией о значениях ключевых метрик за вчерашний день (DAU, Просмотры, Лайки, CTR) и график со значением метрик за предыдущие 7 дней. Второй скрипт собирает отчет по работе всего приложения (лента новостей и сервис отправки сообщений). Отчет отправляется ежедневно. 
Подготовка отчета и отправка автоматизированы с помощью Apache Airflow. DAG выполняется ежедневно в 11:00, количество попыток выполнить DAG - 2, интервал между запусками - 5 минут.

---

Project for automating reporting in the application: news feed and messaging service. The first script generates a report specifically for the news feed: text with information about the values of key metrics for the previous day (DAU, Views, Likes, CTR) and a chart displaying metric values for the past 7 days. The second script compiles a report on the overall performance of the entire application (news feed and messaging service). The report is sent daily.
Report preparation and sending are automated using Apache Airflow. The DAG is executed daily at 11:00. The number of execution attempts for the DAG is set to 2, with a 5-minute interval between retries.
