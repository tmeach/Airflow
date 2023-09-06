## Overview
Реализация системы выявления отклонений в ключевых метриках продукта компании.

## Описание проблемы
Компания - разработчик продуктуа - приложения, объединяющего ленту новостей и сервис обмена сообщениями. 

Компания отслеживает определенные метрики своего продукта, такие как:
- **DAU** - количество уникальных пользователей, которые взаимодействуют с продуктом или сервисом ежедневно.
- **WAU** - количество уникальных пользователей, которые взаимодействуют с продуктом или сервисом еженедельно.
- **MAU** - количество уникальных пользователей, которые взаимодействуют с продуктом или сервисом ежемесячно.
- **CTR** -  процентное соотношение между числом кликов (например, на рекламу) и числом просмотров или показов.
- **LPU** - измеряет, как долго пользователи остаются активными и продолжают использовать продукт после первой активации или установки.

Компания хотела бы отслеживать аномалии в данных метриках, процесс должен быть автоматизирован. В случае обнаружения аномалий, информация об этом отправляется в телеграм месенджер.

## Решение
Система, реализованная с помощью представленного кода проверяет продуктовые метрики компании с помощью статистических методов детектирования аномалий. В случае обнаружения аномального значения в чат в Telegram отправляется сообщение с названием метрики, ее значением, величиной отклонения, и ссылкой на дашборд в Apache Superset. 
DAG выполняется каждый день в 11:00, количество попыток выполнить DAG - 2, интервал между запусками - 5 минут.

--- 

Implementation of a system for identifying deviations in key metrics of the company's product.

## Description of the problem
The company is a product developer - an application that combines a news feed and a messaging service.

The company tracks certain metrics for its product, such as:
- **DAU** - the number of unique users who interact with a product or service daily.
- **WAU** - the number of unique users who interact with a product or service weekly.
- **MAU** - the number of unique users who interact with a product or service monthly.
- **CTR** - the percentage ratio between the number of clicks (for example, on an advertisement) and the number of views or impressions.
- **LPU** - measures how long users remain active and continue to use the product after the first activation or installation.

The company would like to monitor anomalies in these metrics, a process that should be automated. If anomalies are detected, information about this is sent to the telegram messenger.

## Solution
The system implemented using the presented code checks the company's product metrics using statistical methods for detecting anomalies. If an anomalous value is detected, a message is sent to the Telegram chat with the name of the metric, its value, the magnitude of the deviation, and a link to the dashboard in Apache Superset.
DAG is executed every day at 11:00, the number of attempts to execute DAG is 2, the interval between starts is 5 minutes.