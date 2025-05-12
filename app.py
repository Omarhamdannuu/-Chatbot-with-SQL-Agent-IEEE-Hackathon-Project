import sqlite3
from langchain_community.llms import Ollama  # type: ignore # Import from langchain_community
from langchain_core.prompts import PromptTemplate  # type: ignore # Import from langchain_core
from langchain_core.runnables import chain  # type: ignore # Import chain
import requests  # type: ignore # For making HTTP requests
import json  # For working with JSON data
import schedule # type: ignore
import time
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def get_database_schema(database_path):
    """Retrieves the schema information from the SQLite database."""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    schema_description = ""
    for table in tables:
        schema_description += f"\nTable: {table}\n"
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        for column in columns:
            cid, name, type, notnull, dflt_value, pk = column
            schema_description += f"  Column: {name}, Type: {type}"
            if notnull:
                schema_description += ", NOT NULL"
            if pk:
                schema_description += ", PRIMARY KEY"
            schema_description += "\n"
    conn.close()
    return schema_description

def generate_sql_langchain(prompt, database_path, ollama_model="openchat"):
    """Generates SQL code using Langchain and Ollama with schema context."""
    schema_info = get_database_schema(database_path)
    template = f"""You are a helpful AI assistant that translates natural language queries into SQL code for a SQLite database.
    Here is the schema of the database:
    {{schema}}
    Only return the SQL code. Do not provide any explanations or surrounding text.
    Make sure to use table and column names exactly as they appear in the schema.

    User query: {{query}}
    SQL code:
    """
    prompt_template = PromptTemplate.from_template(template) # use from_template
    llm = Ollama(model=ollama_model)
    chain = prompt_template | llm # use the pipe operator
    sql_code = chain.invoke(input={"schema": schema_info, "query": prompt}).strip() # change from run to invoke and adjust the input
    return sql_code

def fetch_data(database_path, sql_query):
    """Connects to the SQLite database and fetches data."""
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()
        conn.close()
        return data
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None

def rephrase_answer_deepseek_api(prompt, data):
    """
    Rephrases the raw data from the database into a user-friendly answer using the DeepSeek API.

    Args:
        prompt (str): The original user query.
        data (list): The data fetched from the database.
        deepseek_api_key (str): Your DeepSeek API key.

    Returns:
        str: A user-friendly rephrased answer, or None on error.
    """
    deepseek_api_key = "sk-ab3bdd397ec34efabaa31c516d5acabf"
    if not data:
        return "No data found."

    data_str = "\n".join(str(row) for row in data)

    # Construct the prompt for DeepSeek API
    deepseek_prompt = f"""
    You are a helpful AI assistant.
    The user asked the following question: {prompt}
    The following data was retrieved from a database:
    {data_str}
    Please rephrase the data into a concise and user-friendly answer.
    """

    url = "https://api.deepseek.com/v1/chat/completions"  # Or the correct DeepSeek API endpoint
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {deepseek_api_key}"  # Include your API key
    }
    payload = {
        "model": "deepseek-chat",  # Or the appropriate DeepSeek model name
        "messages": [
            {"role": "user", "content": deepseek_prompt}
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an exception for bad status codes
        response_json = response.json()
        # Adjust the following line to extract the rephrased answer from the DeepSeek response
        rephrased_answer = response_json['choices'][0]['message']['content'].strip()  # Example, adjust as needed.
        return rephrased_answer
    except requests.exceptions.RequestException as e:
        print(f"Error communicating with DeepSeek API: {e}")
        return None
    except json.JSONDecodeError:
        print("Error decoding DeepSeek API response: Invalid JSON")
        return None
    except KeyError as e:
        print(f"Error extracting content from DeepSeek response.  Key not found: {e}")
        return None
    
# assume there is a database called data that is updated everytime a transaction is created 
# we will use the dataset you guys provided as an example 
FAILURE_THRESHOLD = 0.05
ANOMALY_THRESHOLD = 2.0  # Example threshold for anomaly detection (you'll need a proper anomaly detection method)
SMTP_SERVER = 'your_smtp_server.com'  # Replace with your SMTP server address
SMTP_PORT = 587  # Or your SMTP port
SMTP_USERNAME = 'your_email@example.com'  # Replace with your email address
SMTP_PASSWORD = 'your_email_password'  # Replace with your email password
EMAIL_FROM = 'system_alerts@example.com'  # Replace with the sender email address

def send_email(to_email, subject, body):
    """Sends an email."""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, to_email, msg.as_string())
        print(f"Email sent successfully to {to_email}: {subject}")
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}")

def check_failure_rates():
    """Checks failure rates against a threshold and alerts managers."""
    for location, data in data.items():
        if "failure_rate" in data and data["failure_rate"] > FAILURE_THRESHOLD:
            subject = f"FAILURE RATE ALERT: {location.capitalize()}"
            body = f"The failure rate at {location.capitalize()} is {data['failure_rate']:.2%}, which exceeds the threshold of {FAILURE_THRESHOLD:.2%}."
            send_email(data["manager_email"], subject, body)

def detect_anomalies():
    """Detects anomalies (this is a placeholder and needs a proper implementation)."""
    # In a real system, you would implement a robust anomaly detection algorithm here.
    # This could involve statistical methods, machine learning models, etc.
    # For this example, we'll just flag locations with unusually high values in their financial reports.
    average_sales = sum(data[loc]["daily_financial_report"]["sales"] for loc in data) / len(data) if data else 0
    for location, data in data.items():
        if "daily_financial_report" in data and data["daily_financial_report"]["sales"] > average_sales * ANOMALY_THRESHOLD:
            subject = f"ANOMALY DETECTED: Unusual Sales at {location.capitalize()}"
            body = f"Anomalously high sales detected at {location.capitalize()}: {data['daily_financial_report']['sales']} (Average: {average_sales:.2f})."
            send_email(data["manager_email"], subject, body)
        # Add more anomaly detection for other metrics as needed

def send_daily_financial_reports():
    """Sends daily financial reports to each location's manager."""
    now = datetime.now()
    subject_prefix = f"Daily Financial Report ({now.strftime('%Y-%m-%d')})"
    for location, data in data.items():
        if "daily_financial_report" in data and "manager_email" in data:
            report = json.dumps(data["daily_financial_report"], indent=4)
            subject = f"{subject_prefix}: {location.capitalize()}"
            body = f"Please find the daily financial report for {location.capitalize()} attached:\n\n{report}"
            send_email(data["manager_email"], subject, body)
        else:
            print(f"Warning: Could not send daily report for {location}. Missing financial data or manager email.")

def send_monthly_tax_collection_report():
    """Sends a monthly tax collection report (this is a placeholder)."""
    now = datetime.now()
    if now.day == 1:  # Send on the first day of the month
        total_tax_collection = sum(data[loc].get("monthly_tax_collection", 0) for loc in data)
        subject = f"Monthly Tax Collection Report ({now.strftime('%Y-%m')})"
        body = f"Total tax collected across all locations for {now.strftime('%Y-%m')}: ${total_tax_collection:.2f}"
        # Determine who should receive this report (e.g., a central finance manager)
        central_manager_email = "finance_manager@example.com"  # Replace with the actual email
        send_email(central_manager_email, subject, body)
        print("Monthly tax collection report sent.")
    else:
        print("Not the first day of the month, skipping monthly tax report.")

# Schedule the tasks
schedule.every().hour.do(check_failure_rates)
schedule.every().day.at("09:00").do(send_daily_financial_reports)
schedule.every().day.at("10:00").do(detect_anomalies)
schedule.every().month.at("08:00").do(send_monthly_tax_collection_report) # Will only trigger on the 1st of the month

if __name__ == "__main__":
    database_file = "jordan_transactions.db"
    ollama_model_name = "openchat"

    while True:
        user_prompt = input("Enter your query (or type 'exit' to quit): ")
        if user_prompt.lower() == 'exit':
            break

        sql_query = generate_sql_langchain(user_prompt, database_file, ollama_model_name)

        if sql_query:
            data = fetch_data(database_file, sql_query)
            rephrased_answer = rephrase_answer_deepseek_api(user_prompt, data)
            print(rephrased_answer)
        

