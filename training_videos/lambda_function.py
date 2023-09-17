import lambda_DEV
import lambda_PROD

# ESSA FUNCAO SERVE APENAS DE ROTEADOR PARA AS FUNCOES DE DEV E PROD
# NAO EDITAR
def lambda_handler(event, context):
    data = None
    stage = event['stageVariables']['lambda']
    if stage == "PROD":
        data = lambda_PROD.main(event, context)
        
    elif stage == "DEV":
        data = lambda_DEV.main(event, context)

    return data