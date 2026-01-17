ALTER USER weather_user CREATEDB;
CREATE USER redash_user WITH PASSWORD 'redash_password' CREATEDB;
CREATE DATABASE redash OWNER redash_user;
GRANT ALL PRIVILEGES ON DATABASE redash TO redash_user;