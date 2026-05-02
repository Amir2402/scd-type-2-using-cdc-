CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    street TEXT,
    city TEXT,
    country TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (first_name, last_name, email, street, city, country) VALUES
('Amir', 'Ben Ameur', 'amir.ben@example.com', '12 Rue Habib Bourguiba', 'Tunis', 'Tunisia'),
('Sarah', 'Connor', 's.connor@example.com', '45 Sunset Blvd', 'Los Angeles', 'USA'),
('John', 'Doe', 'john.doe@example.com', '78 Baker Street', 'London', 'UK'),
('Marie', 'Dupont', 'marie.dupont@example.com', '22 Avenue Victor Hugo', 'Paris', 'France'),
('Ali', 'Trabelsi', 'ali.trabelsi@example.com', '9 Rue de Marseille', 'Sfax', 'Tunisia'),
('Fatma', 'Jaziri', 'fatma.jaziri@example.com', '3 Rue de la Liberté', 'Sousse', 'Tunisia'),
('Michael', 'Smith', 'm.smith@example.com', '1600 Amphitheatre Pkwy', 'Mountain View', 'USA'),
('Lina', 'Khaled', 'lina.khaled@example.com', '14 Rue Palestine', 'Ariana', 'Tunisia'),
('Omar', 'Ben Salah', 'omar.bensalah@example.com', '7 Rue El Jazira', 'Bizerte', 'Tunisia'),
('Emma', 'Wilson', 'emma.wilson@example.com', '10 Downing Street', 'London', 'UK');