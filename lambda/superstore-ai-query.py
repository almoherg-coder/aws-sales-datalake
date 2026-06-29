import json
import boto3
import time

athena = boto3.client('athena', region_name='eu-west-1')
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-1')

DATABASE = 'superstore_db'
S3_OUTPUT = 's3://sales-datalake-georgios/athena-results/'

#MODEL_ID = 'mistral.mistral-7b-instruct-v0:2'
#MODEL_ID = 'amazon.titan-text-express-v1'
MODEL_ID = 'eu.amazon.nova-lite-v1:0'

SCHEMA = """
delta_sales(order_date, customer_id, product_id, sales, quantity)
delta_products(product_id, product_name, category)
delta_customers(customer_id, customer_name)

joins:
delta_sales.customer_id = delta_customers.customer_id
delta_sales.product_id = delta_products.product_id
"""

# ─────────────────────────────────────────────
# AI SQL GENERATOR (Bedrock)
# ─────────────────────────────────────────────
def ai_generate_sql(question):
    prompt = f"""
You are an AWS Athena SQL expert.
Return ONLY SQL.

Schema:
{SCHEMA}

Question:
{question}
"""
    body = json.dumps({
    "messages": [
        {"role": "user", "content": [{"text": prompt}]}
     ]
    })

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=body
    )

  
    result = json.loads(response['body'].read())
    sql = result['output']['message']['content'][0]['text'].strip()
    # Remove markdown code blocks that Nova adds
    sql = sql.replace('```sql', '').replace('```', '').strip()
    # Extract only from SELECT onwards
    if 'SELECT' in sql.upper():
      sql = sql[sql.upper().index('SELECT'):]

    return sql.split(";")[0].strip()


# ─────────────────────────────────────────────
# HYBRID CONTROLLER (MAIN LOGIC)
# ─────────────────────────────────────────────
def generate_sql(question):
    try:
        sql = ai_generate_sql(question)
        print("AI SQL used:", sql)
        return sql
    except Exception as e:
        print("AI failed:", str(e))
        return None   # ← explicit None, caller will handle it


# ─────────────────────────────────────────────
# ATHENA EXECUTION
# ─────────────────────────────────────────────
def run_athena_query(sql):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )

    query_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        status = result['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if status != 'SUCCEEDED':
        return None

    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results['ResultSet']['Rows']

    if len(rows) < 2:
        return None

    headers = [c['VarCharValue'] for c in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        data.append({
            headers[i]: row['Data'][i].get('VarCharValue', '')
            for i in range(len(headers))
        })

    return data


# ─────────────────────────────────────────────
# LAMBDA HANDLER
# ─────────────────────────────────────────────
def lambda_handler(event, context):
    question = event.get('question', '')

    if not question:
        return {
            'statusCode': 400,
            'body': 'Please provide a question'
        }

    # 1. Generate SQL
    sql = generate_sql(question)

    # 2. Guard: if AI failed (throttled), return friendly error
    if not sql:
        return {
            'statusCode': 503,
            'body': json.dumps({
                'question': question,
                'error': 'AI model unavailable. Please try again later.'
            })
        }

    # 3. Run Athena
    data = run_athena_query(sql)

    if not data:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'question': question,
                'sql': sql,
                'error': 'Athena query failed or returned no results'
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'question': question,
            'sql': sql,
            'data': data,
            'mode': 'AI'
        })
    }
