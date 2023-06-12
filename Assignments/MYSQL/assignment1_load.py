# This script generates 1000000 rows of data and writes it to a SQL file
# The data is generated using the Faker library and random module
# The data is inserted into the products table in the classicmodel database

import random
from faker import Faker

fake = Faker()

# Opening a file to write SQL statements to
with open('assignment1_load.sql', 'w') as f:
    f.write("USE classicmodel;\n")
    # Generating 1000000 rows of data
    for i in range(1000000):
        productCode = f"P{i+1:06}"
        productName = fake.company()
        productLine = fake.word()
        productScale = random.choice(['1:12', '1:18', '1:24', '1:32', '1:50', '1:72'])
        productVendor = fake.company()
        productDescription = fake.text(max_nb_chars=200)
        quantityInStock = random.randint(0, 10000)
        buyPrice = round(random.uniform(10, 1000), 2)
        MSRP = round(buyPrice * random.uniform(1.1, 1.8), 2)

        # Inserting the data into the SQL statement
        insert_statement = f"INSERT INTO `classicmodel`.`products` (`productCode`, `productName`, `productLine`, `productScale`, `productVendor`, `productDescription`, `quantityInStock`, `buyPrice`, `MSRP`) VALUES ('{productCode}', '{productName}', '{productLine}', '{productScale}', '{productVendor}', '{productDescription}', {quantityInStock}, {buyPrice}, {MSRP});\n"
        f.write(insert_statement)


   
    