SELECT (SELECT COUNT(1) FROM payment)       AS payment,
       (SELECT COUNT(1) FROM rental)        AS rental,
       (SELECT COUNT(1) FROM customer)      AS customer,
       (SELECT COUNT(1) FROM store)         AS store,
       (SELECT COUNT(1) FROM staff)         AS staff,
       (SELECT COUNT(1) FROM address)       AS address,
       (SELECT COUNT(1) FROM film_category) AS film_category,
       (SELECT COUNT(1) FROM inventory)     AS inventory,
       (SELECT COUNT(1) FROM film)          AS film,
       (SELECT COUNT(1) FROM film_actor)    AS film_actor,
       (SELECT COUNT(1) FROM category)      AS category,
       (SELECT COUNT(1) FROM actor)         AS actor,
       (SELECT COUNT(1) FROM language)      AS language,
       (SELECT COUNT(1) FROM city)          AS city,
       (SELECT COUNT(1) FROM country)       AS country;


select (select count(1) from account.balances) AS balances, (select count(1) from account.transactions) AS transactions;