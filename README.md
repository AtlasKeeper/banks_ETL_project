# banks_ETL_project
## This project is a comprehensive demonstration of data engineering skills, focusing on the extraction, transformation, and loading (ETL) processes applied to financial data. This project was done as part of the IBM Professional Certification for Data Engineering on Courser

<h2>Project Overview</h2>

<p>The <strong>Bank ETL Project</strong> automates the process of compiling a list of the top 10 largest banks in the world by market capitalization. The project involves:</p>
<ol>
    <li><strong>Extraction:</strong> Retrieving the latest market capitalization data from a reliable online source through web scraping.</li>
    <li><strong>Transformation:</strong> Converting the market capitalization data into various currencies (USD, GBP, EUR, INR) using current exchange rates.</li>
    <li><strong>Loading:</strong> Storing the transformed data in both CSV format and a SQLite database for easy access and analysis.</li>
</ol>

<h2>Key Features</h2>

<ul>
    <li><strong>Automated Data Extraction:</strong> Utilizes web scraping techniques to gather up-to-date financial data.</li>
    <li><strong>Data Transformation:</strong> Converts and scales data into multiple currencies with high precision.</li>
    <li><strong>Data Storage:</strong> Efficiently saves data in both CSV files and SQLite databases, ensuring flexibility in data handling.</li>
    <li><strong>Logging:</strong> Implements robust logging to track the progress and status of ETL operations.</li>
</ul>

<h2>Technologies Used</h2>

<ul>
    <li><strong>Python:</strong> Core programming language used for scripting and automation.</li>
    <li><strong>Pandas:</strong> Essential library for data manipulation and analysis.</li>
    <li><strong>SQLite:</strong> Lightweight database for efficient data storage and retrieval.</li>
    <li><strong>Requests &amp; BeautifulSoup:</strong> Libraries for web scraping and data extraction.</li>
    <li><strong>NumPy:</strong> Utilized for numerical operations and data transformations.</li>
</ul>

<h2>How to Use</h2>

<p>To run this project, follow the steps below:</p>
<ol>
    <li><strong>Clone the Repository:</strong></li>
    <pre><code>git clone https://github.com/your_username/repository_name.git</code></pre>

    <li><strong>Navigate to the Project Directory:</strong></li>
    <pre><code>cd repository_name</code></pre>

    <li><strong>Install Dependencies:</strong></li>
    <p>Make sure you have Python and pip installed, then run:</p>
    <pre><code>pip install -r requirements.txt</code></pre>

    <li><strong>Run the Project:</strong></li>
    <p>Execute the main script to start the ETL process:</p>
    <pre><code>python banks_project.py</code></pre>
</ol>
