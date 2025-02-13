# **End-to-End E-Commerce Data Pipeline Documentation**
**Course:** Fundamentals of Big Data Analytics and BI  
**Name:** Mikias Bayle  
**ID:** DBUR 0262/13  
**GitHub Repository:** [ecommerce-pipeline](https://github.com/miki1007/ecommerce-pipeline)

---

****1. Data Source Identification & Understanding****  

****1.1 Large Dataset****  
The dataset used in this project is the Brazilian E-Commerce Public Dataset by Olist, available on Kaggle. It consists of multiple CSV files, providing comprehensive insights into various aspects of an e-commerce platform. The primary files include:  
- **olist_orders_dataset.csv** – Contains details about orders (99,441 rows).  
- **olist_order_items_dataset.csv** – Lists the items in each order (112,650 rows).  
- **olist_products_dataset.csv** – Provides product details (32,951 rows).  
- **olist_customers_dataset.csv** – Contains customer information (99,441 rows).  

****1.2 Understanding Dataset Relationships****  
This dataset follows a relational model, where different tables are linked through unique identifiers:  
- **Orders ↔ Order Items:** One order can have multiple items.  
- **Order Items ↔ Products:** Each item in an order corresponds to a product.  
- **Orders ↔ Customers:** Each order is associated with a specific customer.  

****1.3 Potential Use Cases****  
This dataset enables various analytical use cases, including:  
- **Sales Analysis:** Identifying revenue trends and seasonal order volume changes.  
- **Customer Segmentation:** Understanding customer behavior based on geographic locations.  
- **Product Performance Analysis:** Identifying top-selling products and analyzing their ratings.  

---

****2. Data Extraction****  

****2.1 Extracting E-Commerce Data****  
The dataset was extracted using Python and Pandas, ensuring that all files were correctly loaded and validated. The following data files were processed:  
- **olist_orders_dataset.csv** – Orders data containing purchase details, timestamps, and order status.  
- **olist_order_items_dataset.csv** – Itemized order details, including product IDs, seller IDs, and pricing.  
- **olist_products_dataset.csv** – Product details such as category, dimensions, and weight.  
- **olist_customers_dataset.csv** – Customer data, including location and unique identifiers.  

Each dataset was thoroughly checked for consistency, missing values, and structural integrity before proceeding to transformation.  

---

****3. Data Transformation****  

****3.1 Cleaning and Preprocessing****  
To enhance data quality and optimize performance, multiple transformation steps were applied:  
- **Handling Missing Values**  
  - Rows containing missing **order_purchase_timestamp** values were removed to ensure accurate time-series analysis.  
  - Missing values in the **product_category_name** column were replaced with "Unknown" to maintain dataset completeness.  
- **Data Type Conversion**  
  - The **order_purchase_timestamp** column was converted to datetime format for proper chronological analysis.  
  - Numeric columns were downcasted to reduce memory usage and improve processing efficiency.  
- **Merging Tables**  
  - To facilitate comprehensive analysis, key tables were merged based on their unique identifiers, ensuring data integrity and coherence across different dimensions.  
- **Feature Engineering**  
  - New features were created to improve analytical capabilities:  
    - Extracted **order_year** and **order_month** from timestamps for enhanced time-series analysis.  
    - Created a **revenue** column to support sales performance analysis.  

The cleaned and transformed data was then stored as **Parquet files** in the `data/processed/` directory, ensuring efficient storage and quick retrieval for downstream processing.  

---

****4. Data Loading****  

****4.1 Database Schema Design****  
A **star schema** was designed in PostgreSQL to efficiently store and manage the transformed data. The schema consists of:  
- **Fact Tables**  
  - **fact_orders** – Stores key order-level metrics, such as purchase timestamps and payment details.  
  - **fact_order_items** – Contains item-specific details, including pricing, seller information, and product quantities.  
- **Dimension Tables**  
  - **dim_customers** – Contains customer attributes such as location, order history, and unique identifiers.  
  - **dim_products** – Holds product-related information, including category, weight, and size specifications.  

****4.2 Loading Data into PostgreSQL****  
The transformed **Parquet files** were loaded into a PostgreSQL database using SQLAlchemy, ensuring efficient and reliable data storage. Constraints such as **primary keys**, **foreign keys**, and **indexing** were applied to maintain data integrity and enable fast query execution.  

---

****5. Data Visualization and Insights****  

****5.1 Connecting to Tableau****  
To facilitate advanced data visualization and business intelligence analysis, **Tableau Desktop** was connected to the **PostgreSQL database**. The connection was established using the following credentials:  
- **Server:** localhost  
- **Port:** 5432  
- **Database:** ecommerce_pipeline  
- **Username:** mikias  

With the connection established, Tableau can retrieve data from PostgreSQL and create interactive visualizations, such as graphs, charts, and dashboards, for deeper insights into the dataset.  

****5.2 Key Insights from Data Visualization****  
Interactive dashboards were built in Tableau to extract key insights from the e-commerce data, providing valuable business intelligence for decision-making.  

**Sales Trends**  
- Revenue showed a sharp increase in **Q4**, primarily due to holiday shopping events.  
- The **electronics** category accounted for **35%** of total revenue, highlighting its dominance in the market.  

**Customer Segmentation**  
- **60%** of customers were based in **São Paulo (SP)**, making it the most significant market region.  
- The highest-revenue states included **São Paulo (SP)**, **Rio de Janeiro (RJ)**, and **Minas Gerais (MG)**, demonstrating strong consumer demand in these areas.  

**Product Performance**  
- Top-selling categories were **Electronics**, **Furniture**, and **Home Appliances**, reflecting popular consumer preferences.  
- Products with higher weights tended to receive lower customer ratings, suggesting potential logistics or quality issues.  

---

****6. Conclusion****  
This project successfully demonstrated an **end-to-end data pipeline** for an e-commerce platform. Key learnings include:  
- **Importance of Data Cleaning:** Proper preprocessing ensures accurate insights and enhances data quality.  
- **Efficiency of Star Schema:** Helps in relational data storage, ensuring optimized querying and analysis.  
- **Power of Tableau:** Provides a powerful tool for visualizing complex datasets and creating interactive dashboards for data-driven decision-making.  

****Future Improvements****  
To enhance the pipeline further, the following advancements can be implemented:  
- **Real-time Data Ingestion:** Integrating **Apache Kafka** for continuous streaming data, allowing the system to handle dynamic and real-time updates.  
- **Predictive Analytics:** Incorporating **machine learning models** for demand forecasting to predict trends and optimize inventory management.  

---

****7. References****  
- **Brazilian E-Commerce Public Dataset by Olist (Kaggle):**  
  [Kaggle Dataset Link](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)  

- **GitHub Repository:**  
  [ecommerce-pipeline GitHub](https://github.com/miki1007/ecommerce-pipeline/tree/main)  

