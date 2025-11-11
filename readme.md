---
jupyter:
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
  language_info:
    codemirror_mode:
      name: ipython
      version: 3
    file_extension: .py
    mimetype: text/x-python
    name: python
    nbconvert_exporter: python
    pygments_lexer: ipython3
    version: 3.13.4
  nbformat: 4
  nbformat_minor: 5
---

::: {#db0a0808 .cell .markdown}
# Проект разработки витрины для маркетплейса

## Задачи:

-   Построить с помощью SQL витрину клиентских данных
    product_user_features, используя информацию о заказах пользователей.
    Витрина должна содержать информацию о заказах каждого клиента с
    учётом его региона.
-   Решить четыре ad hoc задачи на исследование данных в витрине и
    сформулировать по каждой из них выводы, которые следуют из
    полученных результатов.

## Описание данных:

Таблица orders --- информация о заказах. Содержит поля:

-   order_id --- идентификатор заказа.
-   buyer_id --- идентификатор покупателя, то есть номер пользователя
    при оформлении конкретного заказа.
-   order_status --- статус заказа (Доставлен, В процессе и так далее).
-   order_purchase_ts --- время покупки.
-   order_approved_at --- время подтверждения заказа.
-   order_delivered_carrier_ts --- время передачи заказа в службу
    доставки.
-   order_delivered_ts --- время доставки заказа покупателю.
-   order_estimated_delivery_dt --- ожидаемая дата доставки заказа.

Таблица order_items --- информация о товарах, которые входят в заказ.
Содержит поля:

-   order_id --- идентификатор заказа.
-   order_item_id --- позиция товара в заказе (первая, вторая и так
    далее).
-   product_id --- идентификатор товара.
-   seller_id --- идентификатор продавца, который продаёт этот товар.
-   shipping_limit_date --- дата, до которой товар должен быть
    доставлен.
-   price --- цена товара.
-   delivery_cost --- стоимость доставки товара.

Таблица order_payments --- информация о способах оплаты заказов.Содержит
поля:

-   order_id --- идентификатор заказа.
-   payment_sequential --- порядковый номер платежа.
-   payment_type --- тип оплаты в платеже (например, банковская карта,
    промокод, оплата по счёту и так далее).
-   payment_installments --- количество рассрочек платежа (1 --- платёж
    совершён в полном объёме).

Таблица order_reviews --- отзывы о заказах, оставленные покупателями.
Содержит поля:

-   review_id --- идентификатор отзыва.
-   order_id --- идентификатор заказа, к которому относится отзыв.
-   review_score --- оценка заказа, от 1 до 5.
-   review_creation_date --- дата создания отзыва.
-   review_answer_timestamp --- время ответа продавца на отзыв.

Таблица users --- информация о покупателях.Содержит поля:

-   buyer_id --- идентификатор покупателя, то есть номер пользователя
    при оформлении конкретного заказа.
-   user_id --- уникальный идентификатор пользователя.
-   region --- регион совершения покупки.
-   zip_code_prefix --- индекс региона совершения покупки.
:::

::: {#f662371e .cell .markdown}
## Зависимости данных вы можете увидеть на ER-диаграмме:

![](https://pictures.s3.yandex.net/resources/PPROD-15191_scheme_1752834421.png)
:::

::: {#92759f5c .cell .markdown}
# Разработка витрины данных
:::

::: {#9346a360 .cell .code}
``` python
WITH payments AS
  (SELECT op.order_id,
          op.payment_sequential,
          op.payment_type,
          op.payment_installments
   FROM ds_ecom.order_payments op),

-- первый тип оплаты в заказе
first_payment AS
  (SELECT order_id,
          payment_type AS first_payment_type
   FROM
     (SELECT order_id,
             payment_type,
             ROW_NUMBER() OVER (PARTITION BY order_id
                                ORDER BY payment_sequential) AS rn
      FROM payments) t
   WHERE rn = 1),

-- признак промо
order_promo AS
  (SELECT order_id,
          CASE WHEN COUNT(*) FILTER (
                                    WHERE payment_type = 'промокод') > 0 THEN 1
              ELSE 0
          END AS used_promo_in_order
   FROM payments
   GROUP BY order_id),

-- признак рассрочки
order_installments AS
  (SELECT order_id,
          CASE
              WHEN MAX(payment_installments) > 1 THEN 1
              ELSE 0
          END AS used_installments_in_order
   FROM payments
   GROUP BY order_id),

-- собираем признаки на уровне заказа
order_level AS
  (SELECT fp.order_id,
          fp.first_payment_type,
          opm.used_promo_in_order,
          oi.used_installments_in_order
   FROM first_payment fp
   JOIN order_promo opm USING (order_id)
   JOIN order_installments oi USING (order_id)),

-- стоимость каждого заказа
orders_price AS
  (SELECT o.order_id,
          sum(oi.price) + sum(oi.delivery_cost) AS total_price
   FROM ds_ecom.orders o
   JOIN ds_ecom.order_items oi USING (order_id)
   WHERE order_status = 'Доставлено'
   GROUP BY o.order_id),

-- предварительные расчеты под оценки
reviews_numbers AS
  (SELECT o.order_id,
          sum(CASE
                  WHEN ore.review_score > 5 THEN ore.review_score / 10
                  ELSE ore.review_score
              END) AS total_score,
          count(ore.review_score) AS count_review
   FROM ds_ecom.orders o
   JOIN ds_ecom.order_reviews ore USING(order_id)
   WHERE order_status = 'Доставлено' OR order_status = 'Отменено'
   GROUP BY o.order_id
   ORDER BY count_review DESC),

-- средняя оценка по заказам
order_avg_score AS
  (SELECT order_id,
          total_score::float / count_review::float AS avg_review_score
   FROM reviews_numbers),

-- сводим все признаки по заказам
orders_finals AS
  (SELECT ol.order_id,
          ol.first_payment_type,
          ol.used_promo_in_order,
          ol.used_installments_in_order,
          op.total_price,
          oas.avg_review_score
   FROM order_level ol
   LEFT JOIN order_avg_score oas USING (order_id)
   LEFT JOIN orders_price op USING (order_id)),
						 
-- топ 3 региона
top_regions AS
  (SELECT u.region,
          count(o.order_id) AS cnt
   FROM ds_ecom.users u
   JOIN ds_ecom.orders o USING (buyer_id)
   GROUP BY u.region
   ORDER BY cnt DESC
   LIMIT 3),
		
-- начинаем сводить инфу по парам юзер/регион
usreg_time_orders AS
  (SELECT u.region,
          u.user_id,
          min(o.order_purchase_ts) AS first_order_ts,
          max(o.order_purchase_ts) AS last_order_ts,
          max(o.order_purchase_ts) - min(o.order_purchase_ts) AS lifetime,
          count(of.order_id) AS total_orders,
          avg(of.avg_review_score) AS avg_order_rating,
          count(oas.order_id) AS num_orders_with_rating,
          sum(of.total_price) AS total_order_costs,
          avg(of.total_price) AS avg_order_cost,
          sum(of.used_installments_in_order) AS num_installment_orders,
          sum(OF.used_promo_in_order) AS num_orders_with_promo,
          sum(of.used_installments_in_order) AS sum_installments
   FROM ds_ecom.users u
   JOIN ds_ecom.orders o USING (buyer_id)
   JOIN orders_finals OF ON o.order_id = OF.order_id
   LEFT JOIN order_avg_score oas ON of.order_id = oas.order_id
   WHERE o.order_status = 'Доставлено'
     OR o.order_status = 'Отменено'
   GROUP BY u.region,
            u.user_id
   HAVING u.region IN
     (SELECT region
      FROM top_regions)),

-- количество отмененных заказов
canceled_orders_count AS
  (SELECT u.region,
          u.user_id,
          count(*) AS num_canceled_orders
   FROM ds_ecom.users u
   JOIN ds_ecom.orders o USING (buyer_id)
   WHERE o.order_status = 'Отменено'
   GROUP BY u.region,
            u.user_id),

-- делаем флаг под использование денежного перевода
money_flg AS
  (SELECT u.user_id,
          u.region,
          CASE
              WHEN ol.first_payment_type = 'денежный перевод' THEN 1
              ELSE 0
          END AS money_flag
   FROM ds_ecom.users u
   JOIN ds_ecom.orders o USING (buyer_id)
   JOIN order_level ol ON o.order_id = ol.order_id),
   
used_money_transfer AS
  (SELECT mf.user_id,
          mf.region,
          CASE
              WHEN sum(mf.money_flag) > 0 THEN 1
              ELSE 0
          END AS used_money_transfer
   FROM money_flg mf
   GROUP BY mf.user_id,
            mf.region)

-- витрина финальная
SELECT uto.user_id,
       uto.region,
       uto.first_order_ts,
       uto.last_order_ts,
       uto.lifetime,
       uto.total_orders,
       COALESCE(uto.avg_order_rating, 0) AS avg_order_rating,
       uto.num_orders_with_rating,
       COALESCE(coc.num_canceled_orders, 0) AS num_canceled_orders,
       COALESCE(coc.num_canceled_orders, 0)::float / uto.total_orders::float AS canceled_orders_ratio,
       COALESCE(uto.total_order_costs, 0) AS total_order_costs,
       round(COALESCE(uto.avg_order_cost, 0), 2) AS avg_order_cost,
       uto.num_installment_orders,
       uto.num_orders_with_promo,
       umt.used_money_transfer,
       CASE
           WHEN uto.sum_installments > 0 THEN 1
           ELSE 0
       END AS used_installments,
       CASE
           WHEN coc.num_canceled_orders > 0 THEN 1
           ELSE 0
       END AS used_cancel
FROM usreg_time_orders uto
LEFT JOIN canceled_orders_count coc ON uto.user_id = coc.user_id
AND uto.region = coc.region
LEFT JOIN used_money_transfer umt ON uto.user_id = umt.user_id
AND uto.region = umt.region
```
:::

::: {#4142b430 .cell .markdown}
## Пример первых 10 записей с витрины данных
:::

::: {#8244366e .cell .code execution_count="2"}
``` python
import pandas as pd
import numpy as np
```
:::

::: {#0ba2c18d .cell .code execution_count="4"}
``` python
vitrina = pd.read_csv('csv/vitrina.csv')
display(vitrina.head(10))
```

::: {.output .display_data}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>region</th>
      <th>first_order_ts</th>
      <th>last_order_ts</th>
      <th>lifetime</th>
      <th>total_orders</th>
      <th>avg_order_rating</th>
      <th>num_orders_with_rating</th>
      <th>num_canceled_orders</th>
      <th>canceled_orders_ratio</th>
      <th>total_order_costs</th>
      <th>avg_order_cost</th>
      <th>num_installment_orders</th>
      <th>num_orders_with_promo</th>
      <th>used_money_transfer</th>
      <th>used_installments</th>
      <th>used_cancel</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c79346a04ff9e3c861f7df7e50c6c523</td>
      <td>Москва</td>
      <td>2024-02-12 09:16:15.000</td>
      <td>2024-02-12 09:16:15.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>5.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>840.0</td>
      <td>840.0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>8b8a28ea9c0d1b98fb471b9d408b2b16</td>
      <td>Москва</td>
      <td>2023-07-04 09:40:53.000</td>
      <td>2023-07-04 09:40:53.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>4.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>2200.0</td>
      <td>2200.0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2d85f83f5d81495502e3fde6cd2c628c</td>
      <td>Санкт-Петербург</td>
      <td>2024-03-26 19:28:08.000</td>
      <td>2024-03-26 19:28:08.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>5.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>2325.0</td>
      <td>2325.0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>e4d800060f85d896b83b4c3e3570065b</td>
      <td>Москва</td>
      <td>2023-09-05 14:20:41.000</td>
      <td>2024-03-22 14:01:17.000</td>
      <td>198 days 23:40:36</td>
      <td>2</td>
      <td>5.0</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>4370.0</td>
      <td>2185.0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>970f8e94d1e9ff19f5362c239c803768</td>
      <td>Москва</td>
      <td>2024-02-15 14:58:55.000</td>
      <td>2024-02-15 14:58:55.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>0.0</td>
      <td>0</td>
      <td>1</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>875a707bdfd8d6544f1b74a123b3010b</td>
      <td>Москва</td>
      <td>2024-07-22 21:40:20.000</td>
      <td>2024-07-22 21:40:20.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>3.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>3040.0</td>
      <td>3040.0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6de06281a46d0d459dbe284683e0134c</td>
      <td>Санкт-Петербург</td>
      <td>2024-05-10 11:18:48.000</td>
      <td>2024-05-10 11:18:48.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>1.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>3982.0</td>
      <td>3982.0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>512f0635790564850446909ce00be3df</td>
      <td>Москва</td>
      <td>2024-05-08 20:10:20.000</td>
      <td>2024-05-08 20:10:20.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>5.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>4450.0</td>
      <td>4450.0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>fc3ae503d8cbc4c02a2d8e467b7bbfe5</td>
      <td>Москва</td>
      <td>2023-05-10 16:20:36.000</td>
      <td>2023-05-10 16:20:36.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>1.0</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>2860.0</td>
      <td>2860.0</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>eede3c924dcfa1c4340e70b50010be26</td>
      <td>Санкт-Петербург</td>
      <td>2024-05-30 18:09:10.000</td>
      <td>2024-05-30 18:09:10.000</td>
      <td>00:00:00</td>
      <td>1</td>
      <td>0.0</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>2320.0</td>
      <td>2320.0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::

::: {#1b6fe0a7 .cell .markdown}
# Решение Ad-Hoc задач

## Задача 1. Сегментация пользователей {#задача-1-сегментация-пользователей}

-   Разделите пользователей на группы по количеству совершённых ими
    заказов.
-   Подсчитайте для каждой группы общее количество пользователей,
-   среднее количество заказов, среднюю стоимость заказа.

Выделите такие сегменты:

-   1 заказ --- сегмент 1 заказ
-   от 2 до 5 заказов --- сегмент 2-5 заказов
-   от 6 до 10 заказов --- сегмент 6-10 заказов
-   11 и более заказов --- сегмент 11 и более заказов
:::

::: {#6d801bf5 .cell .code}
``` python
WITH segmentiruem AS
  (SELECT user_id,
          CASE
              WHEN total_orders > 10 THEN '11 и более заказов'
              WHEN total_orders > 5
                   AND total_orders < 11 THEN '6-10 заказов'
              WHEN total_orders > 1
                   AND total_orders < 6 THEN '2-5 заказов'
              ELSE '1 заказ'
          END AS segment,
          total_orders,
          total_order_costs
   FROM ds_ecom.product_user_features)
   
SELECT segment,
       count(user_id) AS total_users,
       avg(total_orders) AS avg_orders,
       avg(total_order_costs / total_orders) AS avg_costs
FROM segmentiruem
GROUP BY segment
```
:::

::: {#4fe66784 .cell .markdown}
### Результат 1 задачи
:::

::: {#c501a47a .cell .code execution_count="5"}
``` python
adhoc1 = pd.read_csv('csv/adhoc1.csv')
display(adhoc1)
```

::: {.output .display_data}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>segment</th>
      <th>total_users</th>
      <th>avg_orders</th>
      <th>avg_costs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6-10 заказов</td>
      <td>5</td>
      <td>7.000000</td>
      <td>2772.896825</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2-5 заказов</td>
      <td>1934</td>
      <td>2.091003</td>
      <td>3056.675465</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1 заказ</td>
      <td>60468</td>
      <td>1.000000</td>
      <td>3324.077229</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11 и более заказов</td>
      <td>1</td>
      <td>15.000000</td>
      <td>1244.800000</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::

::: {#754080f4 .cell .markdown}
### Выводы по 1 задаче

Большинство пользователей маркетплейса совершают по 1 заказу, при этом
видно что средние траты падают с количеством заказов. Получается
ценность клиента растет за счет частоты заказов и не стоимости.\
Основная точка роста - конвертировать \"1 заказ\" в \"2-5
заказов\"(например через ретаргет или персональные офферы после 1-го
заказа) Для повторных покупателей работать над апселлами/бандлами чтобы
растить средний чек.
:::

::: {#840bd635 .cell .markdown}
## Задача 2. Ранжирование пользователей {#задача-2-ранжирование-пользователей}

-   Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию
    среднего чека покупки,
-   Выведите 15 пользователей с самым большим средним чеком среди
    указанной группы.
:::

::: {#d7d923da .cell .code}
``` python
SELECT *
FROM ds_ecom.product_user_features
WHERE total_orders > 2
ORDER BY avg_order_cost DESC
LIMIT 15
```
:::

::: {#4e3a5d05 .cell .markdown}
### Результат 2 задачи
:::

::: {#2b9b9d09 .cell .code execution_count="6"}
``` python
adhoc2 = pd.read_csv('csv/adhoc2.csv')
display(adhoc2)
```

::: {.output .display_data}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>region</th>
      <th>first_order_ts</th>
      <th>last_order_ts</th>
      <th>lifetime</th>
      <th>total_orders</th>
      <th>avg_order_rating</th>
      <th>num_orders_with_rating</th>
      <th>num_canceled_orders</th>
      <th>canceled_orders_ratio</th>
      <th>total_order_costs</th>
      <th>avg_order_cost</th>
      <th>num_installment_orders</th>
      <th>num_orders_with_promo</th>
      <th>used_money_transfer</th>
      <th>used_installments</th>
      <th>used_cancel</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1da09dd64e235e7c2f29a4faff33535c</td>
      <td>Санкт-Петербург</td>
      <td>2023-05-10 14:04:15.000</td>
      <td>2024-01-11 11:16:49.000</td>
      <td>245 days 21:12:34</td>
      <td>3</td>
      <td>3.666667</td>
      <td>3</td>
      <td>0</td>
      <td>0.0</td>
      <td>44150.0</td>
      <td>14716.67</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>297ec5afd18366f5ba27520cc4954151</td>
      <td>Москва</td>
      <td>2024-02-16 13:30:10.000</td>
      <td>2024-05-12 21:23:13.000</td>
      <td>86 days 07:53:03</td>
      <td>3</td>
      <td>5.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>37435.0</td>
      <td>12478.33</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>cef29e793e232d30250331804cdb7000</td>
      <td>Новосибирская область</td>
      <td>2023-03-09 18:15:38.000</td>
      <td>2024-01-18 19:59:07.000</td>
      <td>315 days 01:43:29</td>
      <td>3</td>
      <td>5.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>34555.0</td>
      <td>11518.33</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>397b44d5bb99eabf54ea9c2b41ebb905</td>
      <td>Санкт-Петербург</td>
      <td>2024-01-11 12:16:24.000</td>
      <td>2024-06-17 19:21:01.000</td>
      <td>158 days 07:04:37</td>
      <td>4</td>
      <td>5.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>43713.0</td>
      <td>10928.25</td>
      <td>3</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>d132b863416f85f2abb1a988ca05dd12</td>
      <td>Москва</td>
      <td>2023-08-14 13:42:08.000</td>
      <td>2024-07-21 19:57:05.000</td>
      <td>342 days 06:14:57</td>
      <td>3</td>
      <td>5.000000</td>
      <td>3</td>
      <td>0</td>
      <td>0.0</td>
      <td>29114.0</td>
      <td>9704.67</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>d387ea85dc301a91740e31360d355686</td>
      <td>Москва</td>
      <td>2024-01-23 10:24:15.000</td>
      <td>2024-05-28 16:08:37.000</td>
      <td>126 days 05:44:22</td>
      <td>3</td>
      <td>5.000000</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>27425.0</td>
      <td>9141.67</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4e1cce07cd5937c69dacac3c8b13d965</td>
      <td>Москва</td>
      <td>2023-11-25 15:48:03.000</td>
      <td>2024-07-30 16:04:45.000</td>
      <td>248 days 00:16:42</td>
      <td>3</td>
      <td>3.500000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>25810.0</td>
      <td>8603.33</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>9832ae2f7d3e5fa4c7a1a06e9551bc61</td>
      <td>Санкт-Петербург</td>
      <td>2023-07-14 00:08:37.000</td>
      <td>2024-04-24 23:25:11.000</td>
      <td>285 days 23:16:34</td>
      <td>3</td>
      <td>2.666667</td>
      <td>3</td>
      <td>0</td>
      <td>0.0</td>
      <td>23915.0</td>
      <td>7971.67</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>fe81bb32c243a86b2f86fbf053fe6140</td>
      <td>Москва</td>
      <td>2023-10-22 11:29:22.000</td>
      <td>2024-06-21 12:10:25.000</td>
      <td>243 days 00:41:03</td>
      <td>5</td>
      <td>5.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>37983.0</td>
      <td>7596.60</td>
      <td>2</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>8961b4ca2c5aceb7a78ea72c6e0c840a</td>
      <td>Москва</td>
      <td>2023-11-27 22:10:49.000</td>
      <td>2024-06-23 17:45:33.000</td>
      <td>208 days 19:34:44</td>
      <td>3</td>
      <td>3.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>19055.0</td>
      <td>6351.67</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>b2e9a05d23ea17713b5d7799f2004f8e</td>
      <td>Москва</td>
      <td>2024-07-26 14:26:07.000</td>
      <td>2024-07-29 05:12:32.000</td>
      <td>2 days 14:46:25</td>
      <td>3</td>
      <td>5.000000</td>
      <td>1</td>
      <td>0</td>
      <td>0.0</td>
      <td>18140.0</td>
      <td>6046.67</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>6419a1be8feac26ec793667b71cbaeb4</td>
      <td>Санкт-Петербург</td>
      <td>2023-06-04 23:49:13.000</td>
      <td>2023-12-26 14:57:27.000</td>
      <td>204 days 15:08:14</td>
      <td>3</td>
      <td>5.000000</td>
      <td>3</td>
      <td>0</td>
      <td>0.0</td>
      <td>18120.0</td>
      <td>6040.00</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ab243cd9e788d689cc822380f59616e1</td>
      <td>Москва</td>
      <td>2023-09-24 10:47:39.000</td>
      <td>2024-02-01 09:39:48.000</td>
      <td>129 days 22:52:09</td>
      <td>3</td>
      <td>4.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>17725.0</td>
      <td>5908.33</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>e30b83af13d6ff0b0f427b2a67c43b39</td>
      <td>Новосибирская область</td>
      <td>2023-01-19 14:38:27.000</td>
      <td>2023-11-28 10:44:35.000</td>
      <td>312 days 20:06:08</td>
      <td>3</td>
      <td>4.666667</td>
      <td>3</td>
      <td>0</td>
      <td>0.0</td>
      <td>16675.0</td>
      <td>5558.33</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>3de0c9303f39b7ccfe69ca11aee19cc6</td>
      <td>Москва</td>
      <td>2024-01-19 14:06:44.000</td>
      <td>2024-04-25 11:42:05.000</td>
      <td>96 days 21:35:21</td>
      <td>3</td>
      <td>4.000000</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>16580.0</td>
      <td>5526.67</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::

::: {#771462f4 .cell .markdown}
### Выводы по 2 задаче

В данной выборке преобладает столица как регион. Рассрочка очень
распространена, 13 из 15 клиентов использовали её хотя бы раз. Промокоды
практически не влияют, всего 1 заказ с промо. Отмененных заказов в
группе нет. Можно сделать фокус на \"высокий чек в столице\", затестить
персональные лимиты/условия рассрочки, т.к. она ключевой драйвер.
:::

::: {#1a0d954c .cell .markdown}
## Задача 3. Статистика по регионам. {#задача-3-статистика-по-регионам}

Для каждого региона подсчитайте:

-   общее число клиентов и заказов;
-   среднюю стоимость одного заказа;
-   долю заказов, которые были куплены в рассрочку;
-   долю заказов, которые были куплены с использованием промокодов;
-   долю пользователей, совершивших отмену заказа хотя бы один раз.
:::

::: {#a339beec .cell .code}
``` python
SELECT region,
       count(DISTINCT user_id) AS total_users,
       sum(total_orders) AS total_orders,
       avg(total_order_costs / total_orders) AS avg_costs_per_order,
       sum(num_installment_orders)::float / sum(total_orders)::float AS installments_ratio,
       sum(num_orders_with_promo)::float / sum(total_orders)::float AS promo_ratio,
       sum(used_cancel)::float / count(DISTINCT user_id)::float AS cancel_ratio
FROM ds_ecom.product_user_features
GROUP BY region
```
:::

::: {#5e69a3a3 .cell .markdown}
### Результат 3 задачи
:::

::: {#84b7abb5 .cell .code execution_count="7"}
``` python
adhoc3 = pd.read_csv('csv/adhoc3.csv')
display(adhoc3)
```

::: {.output .display_data}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>region</th>
      <th>total_users</th>
      <th>total_orders</th>
      <th>avg_costs_per_order</th>
      <th>installments_ratio</th>
      <th>promo_ratio</th>
      <th>cancel_ratio</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Москва</td>
      <td>39386</td>
      <td>40747</td>
      <td>3166.438771</td>
      <td>0.477262</td>
      <td>0.037402</td>
      <td>0.006271</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Новосибирская область</td>
      <td>11044</td>
      <td>11401</td>
      <td>3517.562013</td>
      <td>0.541444</td>
      <td>0.036751</td>
      <td>0.004256</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Санкт-Петербург</td>
      <td>11978</td>
      <td>12414</td>
      <td>3619.606095</td>
      <td>0.546560</td>
      <td>0.041647</td>
      <td>0.005343</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::

::: {#b59a9c71 .cell .markdown}
### Выводы по 3 задаче

Больше всего пользователей и заказов даёт столица, при этом средний чек
по столице меньше чем у остальных регионов. В Москве реже пользуются
рассрочкой, чем в остальных регионах. Промокоды в целом используются
довольно редко, чаще всего в Питере. Больше всего отмен наблюдается в
Москве.

У СПБ и Новосиба больше потенциал для роста чека, т.к. выше готовность к
рассрочке. Для Москвы подойдут меры для увеличения среднего чека из
разряда апселлов/пакетных решений и снижению количества отмен(например
коммуникации по доставке/срокам). Предположу, что Москва, будучи
\"быстрым\" городом по сравнению с СПБ и Новосибом, ожидает быстрой
доставки. Можно затестить новую \"быструю доставку\" и посмотреть как
повлияет на метрики.
:::

::: {#63ca4527 .cell .markdown}
## Задача 4. Активность пользователей по первому месяцу заказа в 2023 году {#задача-4-активность-пользователей-по-первому-месяцу-заказа-в-2023-году}

Разбейте пользователей на группы в зависимости от того, в какой месяц
2023 года они совершили первый заказ. Для каждой группы посчитайте:

-   общее количество клиентов, число заказов и среднюю стоимость одного
    заказа;
-   средний рейтинг заказа;
-   долю пользователей, использующих денежные переводы при оплате;
-   среднюю продолжительность активности пользователя.
:::

::: {#af58184b .cell .code}
``` python
SELECT extract(MONTH
               FROM first_order_ts) AS MONTH,
       count(DISTINCT user_id) AS total_users,
       sum(total_orders) AS total_orders,
       avg(total_order_costs / total_orders) AS avg_costs,
       avg(avg_order_rating) AS avg_rating,
       sum(used_money_transfer)::float / count(DISTINCT user_id)::float AS money_transfer_ratio,
       avg(lifetime) AS avg_lifetime
FROM ds_ecom.product_user_features
WHERE extract(YEAR
              FROM first_order_ts) = 2023
GROUP BY extract(MONTH
                 FROM first_order_ts)
```
:::

::: {#f5fc898a .cell .markdown}
### Результат 4 задачи
:::

::: {#2e267d3f .cell .code execution_count="8"}
``` python
adhoc4 = pd.read_csv('csv/adhoc4.csv')
display(adhoc4)
```

::: {.output .display_data}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>month</th>
      <th>total_users</th>
      <th>total_orders</th>
      <th>avg_costs</th>
      <th>avg_rating</th>
      <th>money_transfer_ratio</th>
      <th>avg_lifetime</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>465</td>
      <td>499</td>
      <td>2904.693606</td>
      <td>4.175248</td>
      <td>0.210753</td>
      <td>12 days 19:37:09.617205</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1063</td>
      <td>1115</td>
      <td>2581.278673</td>
      <td>4.168239</td>
      <td>0.221072</td>
      <td>7 days 00:47:00.06397</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1663</td>
      <td>1762</td>
      <td>2770.311795</td>
      <td>4.185868</td>
      <td>0.211064</td>
      <td>8 days 03:58:47.521346</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1435</td>
      <td>1511</td>
      <td>3123.730554</td>
      <td>4.140474</td>
      <td>0.193728</td>
      <td>7 days 01:58:58.324739</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2197</td>
      <td>2322</td>
      <td>2791.694775</td>
      <td>4.222940</td>
      <td>0.197997</td>
      <td>7 days 16:51:16.78152</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>1984</td>
      <td>2107</td>
      <td>2944.638183</td>
      <td>4.185179</td>
      <td>0.200101</td>
      <td>7 days 12:53:36.892695</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2463</td>
      <td>2604</td>
      <td>2795.942069</td>
      <td>4.224278</td>
      <td>0.206659</td>
      <td>6 days 04:54:37.658546</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>2595</td>
      <td>2742</td>
      <td>2838.273625</td>
      <td>4.315146</td>
      <td>0.204239</td>
      <td>5 days 12:20:18.236224</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>2591</td>
      <td>2737</td>
      <td>3311.923151</td>
      <td>4.268803</td>
      <td>0.208414</td>
      <td>5 days 02:02:35.498263</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>2832</td>
      <td>2954</td>
      <td>3254.036507</td>
      <td>4.199610</td>
      <td>0.209040</td>
      <td>3 days 18:11:35.39195</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>4703</td>
      <td>4892</td>
      <td>3191.056333</td>
      <td>4.002647</td>
      <td>0.190091</td>
      <td>2 days 09:56:05.941952</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>3589</td>
      <td>3696</td>
      <td>3163.727835</td>
      <td>4.077451</td>
      <td>0.201728</td>
      <td>2 days 05:54:20.789357</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::

::: {#d4e86e22 .cell .markdown}
### Выводы по 4 задаче

Больше всего пользователей пришло в ноябре, здесь же и большее
количество заказов. Средний чек выше к концу года. Больше всего в
сентябре, меньше в феврале. Средний рейтинг максимальный так же в
сентябре. Денежные переводы как первый тип оплаты использует очень мало
пользователей, при этом максимум так же у сентября. Средняя жизнь
пользователя от первого до последнего заказа длиннее всего в январе.
Осенняя когорта на фоне данных выглядит лучше остальных. Благодаря этому
её можно использовать как ориентир для различных кампаний, ведь там выше
средний чек и рейтинг. Для ранних зимних-весенних когорт можно
тестировать механики удержания, у них ниже чеки и короче активность.
Денежные переводы не играют заметной роли, лучше сфокусироваться на
рассрочке, если мы ставим целью увеличение среднего чека.
:::
