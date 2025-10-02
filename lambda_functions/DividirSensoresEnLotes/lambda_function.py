import json

def lambda_handler(event, context):
    """
    Recibe una lista larga y la divide en una lista de listas más pequeñas (lotes).
    """
    # La lista completa viene de la Lambda A
    all_ids = event
    
    # Tamaño del lote. 500 sensores * 7 eventos/sensor = 3500 eventos. ¡Seguro!
    batch_size = 500 
    
    # Lógica de división en lotes
    batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]
    
    print(f"Dividida la lista de {len(all_ids)} IDs en {len(batches)} lotes de tamaño ~{batch_size}.")
    
    # Devolvemos un diccionario que el Map State puede usar
    # El formato es una lista de objetos, donde cada objeto contiene un lote

    # return {
    #     "batches": [{"sensors": batch} for batch in batches]
    # }

    return [batch for batch in batches]