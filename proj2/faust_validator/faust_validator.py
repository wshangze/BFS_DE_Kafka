import faust
import json
from datetime import datetime

class Employee(faust.Record, serializer='json'): 
    action_id: int
    emp_id: int
    emp_FN: str
    emp_LN: str
    emp_dob: str
    emp_city: str
    action: str

app = faust.App(
    'employee_validator', 
    broker='kafka://kafka:9092', 
    value_serializer='json', 
    consumer_auto_offset_reset='earliest'
)
raw_topic = app.topic('bf_employee_cdc', value_type=Employee)
valid_topic = app.topic('bf_employee_cdc_valid', value_type=Employee)
dlq_topic = app.topic('bf_employee_cdc_dlq', value_type=str)

def is_valid(emp: Employee): 
    try: 
        if not emp.emp_FN or not emp.emp_LN or not emp.emp_city: 
            return False, "Missing values"
        if emp.emp_id <= 0: 
            return False, "Invalid emp_id"
        if emp.action not in {"INSERT", "UPDATE", "DELETE"}: 
            return False, f"Invalid action {emp.action}"
        datetime.strptime(emp.emp_dob, "%Y-%m-%d")
        return True, ""
    except Exception as e: 
        return False, f"Exception during validation {str(e)}"

@app.agent(raw_topic)
async def validate_employee(stream): 
    async for emp in stream: 
        valid, error_msg = is_valid(emp)
        if valid: 
            await valid_topic.send(value=emp)
        else: 
            err_payload = emp.to_representation()
            err_payload["error"] = error_msg
            await dlq_topic.send(value=json.dumps(err_payload))