import json


from redis_create_sesion import redis_client

def report(slot_id:str,
           status:str,
           error_text:str=None,
           conteiner_name:str=None)->None:

    """Отчет менеджеру о статусе работы"""
    payload = {"slot_id":slot_id,
               "status":status,
               "initiated_by": "worker"
               }
    if error_text:
        payload["error_text"] = error_text
    if conteiner_name:
        payload["conteiner_name"] = conteiner_name

    redis_client.lpush("WORKER_REPORTS", json.dumps(payload))