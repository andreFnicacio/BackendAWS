import lambda_DEV

# ESSA FUNCAO SERVE APENAS DE ROTEADOR PARA AS FUNCOES DE DEV E PROD
# NAO EDITAR
def lambda_handler(event, context):
    data = None
    stage = event['stageVariables']['lambda']
    data = lambda_DEV.main(event, context)
        

    return data