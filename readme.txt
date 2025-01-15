Real Time DataWarehouse with extended MeshJoin

What is a Mesh Join?
In data warehousing, a **mesh join** is a technique used to combine data from multiple related tables or datasets by matching their keys. Unlike a simple join, a mesh join often involves integrating data from several tables that are interconnected in a complex manner, resembling a "mesh" of relationships.
The process typically involves:
- **Identifying relationships**: Determining how the tables are related, usually through foreign keys or common attributes.
- **Joining multiple tables**: Combining the data from these tables into a unified view or dataset by applying join conditions (e.g., INNER JOIN, OUTER JOIN).
- **Optimizing for performance**: Mesh joins can be resource-intensive due to the complexity of the relationships, so techniques like indexing or pre-aggregating data may be used.

What is the extension in my code?
In the plain mesh join a single hash table and  master data is used to match with the incoming stream of transactions. What I extended in this project is:
- **Use two seperate master data for example customer data and product data to be joined with the incoming transaction stream
- **Use two hash functions to store the incoming data which consisted of both the product and customer data aka the transactions on a single hash table
- **Made sure no redundancy in the final data
- **Parallel computing such that there are no dead-locks

Why use the extended version of mesh join?
Using multiple master datas is often a better approach in data warehousing, especially for large and complex systems, as it offers improved scalability, flexibility, and performance. By distributing data across domain-specific tables, the size of individual tables is reduced, which makes querying faster and more efficient. This structure also supports parallel processing, enabling simultaneous access to multiple tables, which is advantageous in distributed systems. Additionally, indexing can be tailored to the unique needs of each master table, further enhancing query speed and performance. While multiple master datas may introduce some complexity in managing relationships and joins, this trade-off is outweighed by the system’s ability to handle growing data volumes and provide faster responses for domain-focused queries. Properly managed, multiple master datas ensure a more robust, scalable, and performance-oriented data architecture.

How to run:
Pre-requisites:
1) Make sure to have the sql java connector in eclipse.
2) Make sure that mysql is installed.

How to run
1)First run the sql to generate the database and the tables.
2)Run the python code to generate the data and fill the sql tables.
3)Now open the Java file in eclipse and hit run.

That's how it works :)
