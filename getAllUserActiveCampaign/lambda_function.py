# %%
import lambda_DEV
import lambda_PROD

def lambda_handler(event, context):
    print(event)
    stage = event['stageVariables']['lambda']
    data = None
        
    if stage == "PROD":
        data = lambda_PROD.main(event, context)
    elif stage == "DEV":
        data = lambda_DEV.main(event, context)

    return data