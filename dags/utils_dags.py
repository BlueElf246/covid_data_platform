from datetime import datetime, timedelta
from airflow.operators.python import get_current_context
def get_execution_date():
    context = get_current_context()
    today = context["ds"]
    # today = '2020-01-15'
    yesterday = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return today, yesterday