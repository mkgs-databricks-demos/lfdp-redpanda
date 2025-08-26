CREATE VIEW profiles_cdf AS
SELECT * FROM table_changes('profiles_bronze', 0);