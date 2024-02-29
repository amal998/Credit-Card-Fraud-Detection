# Credit-Card-Fraud-Detection

As the sole architect and developer of this project, I have designed and implemented a comprehensive solution to address the challenges of fraud detection and customer information management.

Fraud Detection Solution:
Central to my project is a sophisticated fraud detection system built using advanced big data technologies. By analyzing transactional data in real time and applying a set of predefined rules, the system can efficiently identify potentially fraudulent activities. I have meticulously fine-tuned the rules to strike a balance between accurately detecting fraud and minimizing false positives. This approach ensures that genuine transactions are not wrongly flagged, thus preventing unnecessary inconvenience to users. Additionally, the system's proactive nature helps mitigate potential financial losses and maintains trust among users.

Customer Information Management:
In parallel with the fraud detection system, I have developed a platform for seamless customer information management. This platform continuously updates relevant customer data, providing real-time access to support teams for resolving complaints and queries promptly. Leveraging the power of big data technologies, I have optimized data retrieval and management processes, ensuring that support teams can access the latest information at their fingertips. This capability significantly enhances customer service delivery and overall user satisfaction.

In conclusion, through my project, I have demonstrated proficiency in designing and implementing solutions that effectively address security and customer service challenges in financial transactions. This endeavour not only showcases my technical skills but also underscores my commitment to creating impactful solutions that positively impact user experiences.


Objective: Create system to identify/detect Credit Card Fraud, Ingest data using Apache Sqoop , process the streaming data and make Real-time decisions Solution: Load data in No Sql , Ingest data using AWS RDS ,Create look up tables , Created streaming framework for real time data Key Achievement: Loaded Data and Created Look up tables , Created a streaming data processing framework that ingests real-time POS transaction data from Kafka, Validation system from Fraud detection

As part of the project, broadly, you are required to perform the following tasks:

Task 1: Load the transactions history data (card_transactions.csv) in a NoSQL database.

Task 2: Ingest the relevant data from AWS RDS to Hadoop.

Task 3: Create a look-up table with columns specified earlier in the problem statement.

Task 4: After creating the table, you need to load the relevant data in the lookup table.

Task 5: Create a streaming data processing framework that ingests real-time POS transaction data from Kafka. The transaction data is then validated based on the three rules’ parameters (stored in the NoSQL database) discussed previously.

Task 6: Update the transactions data along with the status (fraud/genuine) in the card_transactions table.

Task 7: Store the ‘postcode’ and ‘transaction_dt’ of the current transaction in the look-up table in the NoSQL database if the transaction was classified as genuine.

![image](https://github.com/amal998/Credit-Card-Fraud-Detection/assets/128129642/18c0ff1e-1a42-4868-9c30-7b10d2bbf5f8)








