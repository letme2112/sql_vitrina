/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Носов Артём Александрович
 * Дата: 08.10.2025
*/



/* Часть 1. Разработка витрины данных
 * Напишите ниже запрос для создания витрины данных
*/

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



/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

-- сегментируем
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

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 * 
Большинство пользователей маркетплейса совершают по 1 заказу, при этом видно что средние траты падают с количеством заказов.
Получается ценность клиента растет за счет частоты заказов и не стоимости.  
Основная точка роста - конвертировать "1 заказ" в "2-5 заказов"(например через ретаргет или персональные офферы после 1-го заказа)
Для повторных покупателей работать над апселлами/бандлами чтобы растить средний чек.
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки, 
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

SELECT *
FROM ds_ecom.product_user_features
WHERE total_orders > 2
ORDER BY avg_order_cost DESC
LIMIT 15

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * 
В данной выборке преобладает столица как регион. Рассрочка очень распространена, 13 из 15 клиентов использовали её хотя бы раз.
Промокоды практически не влияют, всего 1 заказ с промо. Отмененных заказов в группе нет.
Можно сделать фокус на "высокий чек в столице", затестить персональные лимиты/условия рассрочки, т.к. она ключевой драйвер.
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/

SELECT region,
       count(DISTINCT user_id) AS total_users,
       sum(total_orders) AS total_orders,
       avg(total_order_costs / total_orders) AS avg_costs_per_order,
       sum(num_installment_orders)::float / sum(total_orders)::float AS installments_ratio,
       sum(num_orders_with_promo)::float / sum(total_orders)::float AS promo_ratio,
       sum(used_cancel)::float / count(DISTINCT user_id)::float AS cancel_ratio
FROM ds_ecom.product_user_features
GROUP BY region

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * 
Больше всего пользователей и заказов даёт столица, при этом средний чек по столице меньше чем у остальных регионов.
В Москве реже пользуются рассрочкой, чем в остальных регионах. Промокоды в целом используются довольно редко, чаще всего в Питере.
Больше всего отмен наблюдается в Москве.

У СПБ и Новосиба больше потенциал для роста чека, т.к. выше готовность к рассрочке.
Для Москвы подойдут меры для увеличения среднего чека из разряда апселлов/пакетных решений и снижению количества отмен(например коммуникации по доставке/срокам).
Предположу, что Москва, будучи "быстрым" городом по сравнению с СПБ и Новосибом, ожидает быстрой доставки. Можно затестить новую "быструю доставку" и посмотреть как повлияет на метрики.
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/

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

/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * 
Больше всего пользователей пришло в ноябре, здесь же и большее количество заказов.
Средний чек выше к концу года. Больше всего в сентябре, меньше в феврале.
Средний рейтинг максимальный так же в сентябре. 
Денежные переводы как первый тип оплаты использует очень мало пользователей, при этом максимум так же у сентября.
Средняя жизнь пользователя от первого до последнего заказа длиннее всего в январе.
Осенняя когорта на фоне данных выглядит лучше остальных. Благодаря этому её можно использовать как ориентир для различных кампаний, ведь там выше средний чек и рейтинг.
Для ранних зимних-весенних когорт можно тестировать механики удержания, у них ниже чеки и короче активность.
Денежные переводы не играют заметной роли, лучше сфокусироваться на рассрочке, если мы ставим целью увеличение среднего чека.
*/
