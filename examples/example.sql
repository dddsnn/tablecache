CREATE TABLE users (
    user_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    name text NOT NULL
);

CREATE TABLE cities (
    city_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    name text NOT NULL
);

CREATE TABLE users_cities (
    user_id integer PRIMARY KEY REFERENCES users(user_id),
    city_id integer NOT NULL REFERENCES cities(city_id)
);

WITH
    u AS (INSERT INTO users(name) VALUES('Max Mustermann') RETURNING user_id),
    c AS (INSERT INTO cities(name) VALUES('Musterstadt') RETURNING city_id)
INSERT INTO users_cities(user_id, city_id)
SELECT u.user_id, c.city_id FROM u, c;

WITH
    u AS (INSERT INTO users(name) VALUES('u2') RETURNING user_id),
    c AS (INSERT INTO cities(name) VALUES('c2') RETURNING city_id)
INSERT INTO users_cities(user_id, city_id)
SELECT u.user_id, c.city_id FROM u, c;
