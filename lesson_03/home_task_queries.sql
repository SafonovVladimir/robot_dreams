/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT category.name AS category_name, COUNT(film.film_id) AS film_count
FROM category
LEFT JOIN film_category ON category.category_id = film_category.category_id
LEFT JOIN film ON film_category.film_id = film.film_id
GROUP BY category.name
ORDER BY film_count DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT CONCAT(actor.first_name, ' ', actor.last_name) AS actor_full_name,
       COUNT(rental.rental_id) AS rental_count
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film ON film_actor.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY actor.actor_id
ORDER BY rental_count DESC
LIMIT 10;


/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
SELECT SUM(payment.amount) AS amount, category.name AS categoty
FROM payment
JOIN rental ON payment.rental_id = rental.rental_id
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
JOIN film_category ON film.film_id = film_category.film_id
JOIN category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY amount DESC
LIMIT 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT film.title AS title
FROM film
WHERE NOT EXISTS(
    SELECT inventory.film_id AS film_id
    FROM inventory
    WHERE inventory.film_id = film.film_id);


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/

SELECT CONCAT(actor.first_name, ' ', actor.last_name) AS actor_full_name
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film_category ON film_actor.film_id = film_category.film_id
JOIN category ON film_category.category_id = category.category_id
WHERE category.name = 'Children'
GROUP BY actor.actor_id, actor.first_name, actor.last_name
ORDER BY COUNT(film_actor.film_id) DESC
LIMIT 3;