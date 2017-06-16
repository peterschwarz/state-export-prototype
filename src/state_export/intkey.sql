-- Docker Container
-- docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=mypassword -d postgres

-- Setup DB
-- via the psql in docker:
-- docker run -it --rm --link postgres:postgres postgres psql -h postgres -U postgres

CREATE DATABASE intkey;

CREATE USER intkey_app WITH PASSWORD 'intkey_proto';

GRANT ALL PRIVILEGES ON DATABASE intkey TO intkey_app;
