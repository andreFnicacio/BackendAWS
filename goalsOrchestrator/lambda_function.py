# %%
import lambda_DEV
import lambda_PROD

def lambda_handler(event, context):
    print(event)
    # stage = event['messageAttributes']['aws']
    stage = "DEV"
    data = None
        
    if stage == "PROD":
        data = lambda_PROD.main(event, context)
    elif stage == "DEV":
        data = lambda_DEV.main(event, context)

    return data

lambda_handler({'Records': [{'body': '{"sofier":"davi.aquila@mysofie.com", "task": "CPC"}'}]}, None)
