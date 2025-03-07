-- schema.sql

-- Create the 'users' table
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a stored procedure 'doSomething'
DELIMITER //
CREATE FUNCTION doSomething(jsonParam TEXT)
RETURNS TEXT
DETERMINISTIC                -- или NO SQL, если функция не использует таблицы
BEGIN
    RETURN CONCAT('Processed: ', jsonParam);
END;
//
DELIMITER ;
