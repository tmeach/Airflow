## Overview
Автоматизация отчетности по продукту компании - лента новостей и сервис отправки сообщений.

## Описание проблемы: 
Компания хотела бы автоматизировать анализ продуктовых метрик своего продукта. В качестве инструмента выбран телеграм месенджер, в который должен отправляться отчет со значениями метрик, с определенной периодичностью.

## Решение
Написал двух телеграм-ботов:

1. bot_report_1: 
   
   Бот собирает и отправляет в Telegram Bot текстовый отчет по ленте новостей с информацией о значениях ключевых метрик за вчерашний день (DAU, Просмотры, Лайки, CTR) и визуальный график со значением метрик за предыдущие 7 дней. 
    
2. bot_report_2:
   
   Бот собирает и отправляет в Telegram Bot отчет по работе всего приложения (лента новостей и сервис отправки сообщений).

---

Automation of reporting on the company's product - news feed and message sending service.

## Description of the problem:
A company would like to automate the analysis of product metrics for its product. Telegram messenger was selected as a tool, to which a report with metrics values should be sent at a certain frequency.

## Solution
I wrote two telegram bots:

1. bot_report_1:
   
    The bot collects and sends to Telegram Bot a text report on the news feed with information about the values of key metrics for yesterday (DAU, Views, Likes, CTR) and a visual graph with the values of metrics for the previous 7 days.
    
2. bot_report_2:
   
    The bot collects and sends to Telegram Bot a report on the operation of the entire application (news feed and message sending service).