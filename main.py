import os
import dotenv
import psycopg2
from psycopg2 import sql
import pandas as pd
from groq import Groq
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from pyspark.sql import SparkSession

app = FastAPI()

class QuestionRequest(BaseModel):
    question: str

def get_database_connection():
    dotenv.load_dotenv()
    try:
        conn = psycopg2.connect(
            database=os.environ.get("DATABASE_NAME"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
            host=os.environ.get("DATABASE_HOST"),
            port=os.environ.get("DATABASE_PORT")
        )
        print("Connected to the PostgreSQL database!")
        return conn
    except Exception as error:
        print("Error while connecting to PostgreSQL", error)
        return None

def get_table_metadata_postgresql(conn, table_name):
    try:
        cursor = conn.cursor()

        cursor.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s;
        """), [table_name])
        columns = cursor.fetchall()

        table_metadata = {}
        for column_name, data_type in columns:
            column_name = column_name.lower().replace(" ", "_").replace("/", "_")

            unique_values = []
            if data_type == 'text':
                cursor.execute(sql.SQL("""
                    SELECT DISTINCT {column}
                    FROM {table}
                    LIMIT 10;
                """).format(
                    column=sql.Identifier(column_name),
                    table=sql.Identifier(table_name)
                ))
                unique_values = cursor.fetchall()
                unique_values = [val[0] for val in unique_values]

            table_metadata[column_name] = {
                'data_type': data_type,
                'unique_values': unique_values
            }

        cursor.close()
        return table_metadata

    except Exception as error:
        print("Error while fetching table metadata", error)
        return None

def format_metadata(metadata):
    formatted_metadata = ""
    for col, info in metadata.items():
        formatted_metadata += f"{col}: {info['data_type']}"
        if info['data_type'] == 'text':
            formatted_metadata += f" (Unique Values: {', '.join(info['unique_values'])})"
        formatted_metadata += "\n"
    return formatted_metadata

def get_llama_assistance(prompt, formatted_metadata, table_name):
    main_purpose = f"""
    As an SQL Query Expert, your primary role is to understand the given data, answer the questions based on the provided input and generate accurate SQL queries ONLY. 
    Remember, you only have to answer the Query for the given input, don't give any explanation, just the query. 
    Here are the column names with respect to their information: 
    {formatted_metadata}
    The table name is {table_name}
    Here is/are the Questions:"""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
    completion = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[
            {
                "role": "user",
                "content": f"{main_purpose} {prompt}"
            },
            {
                "role": "assistant",
                "content": ""
            }
        ],
        temperature=1.4,
        max_tokens=8192,
        top_p=1,
        stream=True,
        stop=None,
    )

    response_text = ""
    for chunk in completion:
        response_text += chunk.choices[0].delta.content or ""
    
    cleaned_response = response_text.replace("```", "").strip()
    return cleaned_response

def execute_query_postgresql(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=colnames)
        cursor.close()
        return df
    except Exception as error:
        print("Error while executing the query", error)
        return None

@app.post("/ask_question_postgresql")
def ask_question_postgresql(request: QuestionRequest):
    try:
        connection = get_database_connection()
        if connection:
            table_name = os.environ.get("TABLE_NAME_POST")
            metadata = get_table_metadata_postgresql(connection, table_name)
            formatted_metadata = format_metadata(metadata)
            
            input_question = request.question
            print(f"Question asked: {input_question}")
            query = get_llama_assistance(input_question, formatted_metadata, table_name)
            print("Generated SQL Query:")
            print(query)
            
            result_df = execute_query_postgresql(connection=connection, query=query)
            if result_df is not None:
                connection.close()
                return {
                    "question": input_question,
                    "query": query,
                    "results": result_df.to_dict(orient="records")
                }
            else:
                connection.close()
                raise HTTPException(status_code=404, detail="No Results Found")
        else:
            raise HTTPException(status_code=500, detail="Database Connection Error")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(error)}")
    

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)