import os
import pandas as pd
import io
import tempfile
import boto3
from provider import *

'''
[1] função Listing = recebe stageVariables(pode ser DEV ou PROD) e queryStringParameters(filtros do front)
    [1.1] transforma valores recebidos em querry para o mongodb 
    [1.2] com o resultado da querry do mongo executa a função report alelo e retorna valores como "data" 
    
[2] report_alelo = ler e tratas itens vindos do Mongodb 
    [2.1] retorna uma planilha para download

'''


def listing(company: str = None, **kwargs) -> tuple:
    host = os.environ['MONGODB_HOST']
    port = os.environ['MONGODB_PORT']
    table = os.environ[f'{kwargs["stage"]}_analytics']
    provider = None
    if kwargs["stage"] == "PROD":
        provider = Prod_Provider()
    else:
        provider = Dev_Provider()
    mongodb = provider.get_mongodb()
    # mongodb = MongoClient(host=host, port=int(port))
    collection = mongodb[company.lower()][table]
    print("esses são os argumentos")
    print(kwargs)
    
    if company =="ALELO":
        params = {
            'result': 'FINISH'
        }
    
        if company:
            params['company'] = company.lower()
    
        if 'address.state' in kwargs:
            if kwargs['address.state'] != 'TODOS':
                params['state'] = kwargs['address.state']
    
        if 'address.city' in kwargs:
            if kwargs['address.city'] != 'null':
                params['city'] = kwargs['address.city']
    
        if 'task_info.cnpj' in kwargs:
            params['cnpj'] = kwargs['task_info.cnpj']
    
        if 'interval.start' in kwargs and 'interval.end' in kwargs:
            params['start'] = {
                '$gte': kwargs['interval.start'],
                '$lte': kwargs['interval.end']
            }
    
    elif company == 'APSEN':
        collection = mongodb['apsen']['dsh_analytics']
        params = {
            'status': 'SUCCESS',
        }
        
        if 'address.state' in kwargs:
            if kwargs['address.state'] != 'TODOS':
                params['CUP_UF'] = kwargs['address.state']
    
        if 'address.city' in kwargs:
            if kwargs['address.city'] != 'null':
                params['CUP_CIDADE'] = kwargs['address.city']
    
        if 'task_info.cnpj' in kwargs:
            params['CNPJ'] = kwargs['task_info.cnpj']
            
        if 'task' in kwargs:
            if kwargs['task'] == 'RFA-2':
                params['task'] = kwargs['task']
            else:
                params['task'] = 'RFA'
    
        if 'interval.start' in kwargs and 'interval.end' in kwargs:
            params['data da tarefa'] = {
                '$gte': kwargs['interval.start'],
                '$lte': kwargs['interval.end']
            }
            
    elif company == 'VR':
        collection = mongodb['VR']['warehouse']
        print(company)
        params = {
            'status': 'SUCCESS',
        }
    
        if 'address.state' in kwargs:
            if kwargs['address.state'] != 'TODOS':
                params['stat'] = kwargs['address.state']
    
        if 'address.city' in kwargs:
            if kwargs['address.city'] != 'null':
                params['city'] = kwargs['address.city']
    
        if 'task_info.cnpj' in kwargs:
            params['CNPJ'] = kwargs['task_info.cnpj']
    
        if 'interval.start' in kwargs and 'interval.end' in kwargs:
            params['data da tarefa'] = {
                '$gte': kwargs['interval.start'],
                '$lte': kwargs['interval.end']
            }
            
            
    tasks = list()
    print(params)
    cursor = collection.find(
        filter=params
    )

    cursor.sort('start', -1)
    
    if company.lower() == 'alelo':
        tasks = [i for i in cursor if not i['result'] == 'CANCEL']
    else:
        tasks = [i for i in cursor if i['status'] == 'SUCCESS']
    
    print("esses são os resultados")
    print(tasks)
    data = ''

    if len(tasks) > 0 and company.lower() == 'alelo':
        data = report_alelo(tasks)
    
    elif len(tasks) > 0 and company.lower() == 'apsen':
        data = report_apsen(tasks)
        
    elif len(tasks) > 0 and company.lower() == 'vr':
        data = report_VR(tasks)
        
    return 200, 'text/csv', data


def report_alelo(itens):
    df = pd.DataFrame(itens)
    excluir = ["ENDERECO", "CIDADE", "RAZAO_SOCIAL"]
    df = df.astype(str)
    for i in df.columns:
        if i in excluir:
            df.drop(i, inplace=True, axis=1)
    
    if 'RESPONSAVEL' in df.columns:
        df.drop('RESPONSAVEL', inplace=True, axis=1)

    df.rename(columns={
        'sofie_name': 'RAZAO_SOCIAL',
        'NU_SO_EC': 'EC',
        'when_audit': 'AUDITORIA',
        'formatted_address': 'ENDERECO',
        'city': "CIDADE",
        'state': "ESTADO",
        'LUGAR:CONFIRMACAO_VISUAL': "CONFIRMACAO",
        'PROPRIETARIO_01': "RESPONSAVEL",
        'ESTABELECIMENTO:EXPEDIENTE:DIAS_0': "DIA_FUNCIONAMENTO",
        'ESTABELECIMENTO:EXPEDIENTE:HORARIO': 'HORA_FUNCIONAMENTO',
        'ESTABELECIMENTO:ANTECIPACAO:CONHECE': "ANTECIPACAO",
        'ESTABELECIMENTO:FOTO:FACHADA_0': "FOTO_1",
        'ESTABELECIMENTO:FOTO:INTERIOR_0': "FOTO_2",
        'LUGAR:ERRADO:ALIMENTACAO': "ERRADO_ALIMENTACAO",
        'LUGAR:ERRADO:EVIDENCIA_0': "FOTO_3",
        'lat': 'LATITUDE',
        'lng': 'LONGITUDE',
        'ESTABELECIMENTO:FOTO:CARDAPIO_0': 'FOTO_4'
    }, inplace=True)
    
    if not 'APP_0' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_0': 'APP_0'}, inplace=True)
    
    if not 'APP_1' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_1': 'APP_1'}, inplace=True)
    
    if not 'APP_2' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_2': 'APP_2'}, inplace=True)
    
    if not 'APP_3' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_3': 'APP_3'}, inplace=True)
    
    if not 'APP_4' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_4': 'APP_4'}, inplace=True)
    
    if not 'APP_5' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_5': 'APP_5'}, inplace=True)
    
    if not 'APP_6' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA:APP_6': 'APP_6'}, inplace=True)
    
    if not 'EMAIL_EC' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:CONTATOS_email': 'EMAIL_EC'}, inplace=True)
    
    if not 'TEL_EC_1' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:CONTATOS_phone': 'TEL_EC_1'}, inplace=True)
    
    if not 'CONHECE_PAINEL' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:PAINEL': 'CONHECE_PAINEL'}, inplace=True)
    
    if not 'SABER_PAINEL' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:PAINEL:ACEITA': 'SABER_PAINEL'}, inplace=True)
    
    if not 'FUNCIONARIOS' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:FUNCIONARIOS': 'FUNCIONARIOS'}, inplace=True)
    
    if not 'ATIVIDADE' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:ATIVIDADE': 'ATIVIDADE'}, inplace=True)
    
    if not 'ADESIVO' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:ACEITA:ADESIVO': 'ADESIVO'}, inplace=True)
    
    if not 'COMPRA_TICKET' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:COMPRA_TICKET': 'COMPRA_TICKET'}, inplace=True)
    
    if not 'DELIVERY' in df.columns:
        df.rename(
            columns={'ESTABELECIMENTO:PD:ENTREGA': 'DELIVERY'}, inplace=True)
            
    # Retrocompatibilidade
    df = df.astype(str)
    df.replace('nan', '', inplace=True)
    
    if 'EXISTE_1' in df.columns and 'CONFIRMACAO' in df.columns:
        df['CONFIRMACAO'] = df['CONFIRMACAO'] + ' ' + df['EXISTE_1']
        if 'EXISTE_2' in df.columns:
            df['CONFIRMACAO'] = df['CONFIRMACAO'] + ' ' + df['EXISTE_2']
    elif 'EXISTE_1' in df.columns:
        df['CONFIRMACAO'] = df['EXISTE_1']
        if 'EXISTE_2' in df.columns:
            df['CONFIRMACAO'] = df['CONFIRMACAO'] + ' ' + df['EXISTE_2']
            
    if 'ERRADO_ALIMENTACAO_1' in df.columns and 'ERRADO_ALIMENTACAO' in df.columns:
        df['ERRADO_ALIMENTACAO'] = df['ERRADO_ALIMENTACAO'] + \
            ' ' + df['ERRADO_ALIMENTACAO_1']
        if 'ERRADO_ALIMENTACAO_2' in df.columns:
            df['ERRADO_ALIMENTACAO'] = df['ERRADO_ALIMENTACAO'] + \
                ' ' + df['ERRADO_ALIMENTACAO_2']
    elif 'ERRADO_ALIMENTACAO_1' in df.columns:
        df['ERRADO_ALIMENTACAO'] = df['ERRADO_ALIMENTACAO_1']
        if 'ERRADO_ALIMENTACAO_2' in df.columns:
            df['ERRADO_ALIMENTACAO'] = df['ERRADO_ALIMENTACAO'] + \
                ' ' + df['ERRADO_ALIMENTACAO_2']
            
    if 'ANTECIPACAO_0' in df.columns and 'ANTECIPACAO' in df.columns:
        df['ANTECIPACAO'] = df['ANTECIPACAO'] + ' ' + df['ANTECIPACAO_0']
    elif 'ANTECIPACAO_0' in df.columns:
        df['ANTECIPACAO'] = df['ANTECIPACAO_0']

    if 'TEL_EC_1_phone' in df.columns and 'TEL_EC_1' in df.columns:
        df['TEL_EC_1'] = df['TEL_EC_1'] + ' ' + df['TEL_EC_1_phone']
    elif 'TEL_EC_1_phone' in df.columns:
        df['TEL_EC_1'] = df['TEL_EC_1_phone']
    
    if 'EMAIL_EC_email' in df.columns and 'EMAIL_EC' in df.columns:
        df['EMAIL_EC'] = df['EMAIL_EC'] + ' ' + df['EMAIL_EC_email']
    elif 'EMAIL_EC_email' in df.columns:
        df['EMAIL_EC'] = df['EMAIL_EC_email']
    
    if 'FOTO_1_0' in df.columns and 'FOTO_1' in df.columns:
        df['FOTO_1'] = df['FOTO_1'] + ' ' + df['FOTO_1_0']
    elif 'FOTO_1_0' in df.columns:
        df['FOTO_1'] = df['FOTO_1_0']
    
    if 'FOTO_2_0' in df.columns and 'FOTO_2' in df.columns:
        df['FOTO_2'] = df['FOTO_2'] + ' ' + df['FOTO_2_0']
    elif 'FOTO_2_0' in df.columns:
        df['FOTO_2'] = df['FOTO_2_0']
    
    if 'FOTO_3_0' in df.columns and 'FOTO_3' in df.columns:
        df['FOTO_3'] = df['FOTO_3'] + ' ' + df['FOTO_3_0']
    elif 'FOTO_3_0' in df.columns:
        df['FOTO_3'] = df['FOTO_3_0']
    
    if 'FOTO_4_0' in df.columns and 'FOTO_4' in df.columns:
        df['FOTO_4'] = df['FOTO_4'] + ' ' + df['FOTO_4_0']
    elif 'FOTO_4_0' in df.columns:
        df['FOTO_4'] = df['FOTO_4_0']
        
    df.rename(columns={
        "TEL_EC_2_phone": "TEL_EC_2",
        "CEL_EC_1_phone": "CEL_EC_1",
        "CEL_EC_2_phone": "CEL_EC_2",
        "TEL_RESP_1_phone": "TEL_RESP_1",
        "TEL_RESP_2_phone": "TEL_RESP_2",
        "CEL_RESP_1_phone": "CEL_RESP_1",
        "CEL_RESP_2_phone": "CEL_RESP_2",
        "EMAIL_RESP_email": "EMAIL_RESP"
    }, inplace=True)
    
    df = df.astype(str)
    df.replace('nan', '', inplace=True)

    # Separando data e Hora da Auditoria
    df.AUDITORIA = pd.to_datetime(df.AUDITORIA) - pd.DateOffset(hours=3)
    df['AUDITORIA_DATA'] = df['AUDITORIA'].dt.date
    df['AUDITORIA_HORA'] = df['AUDITORIA'].dt.time

    # juntando dias e horarios
    df['CREDENCIADO_DATA'] = ' '
    if 'CRED_DIA' in df.columns and 'CRED_HORARIO' in df.columns:
        df['CREDENCIADO_DATA'] = df['CRED_DIA'] + ' ' + df['CRED_HORARIO']

    # verificando apps de delivery
    df['APP'] = ' '
    if 'APP_0' in df.columns:
        df['APP'] = df['APP_0']
        if 'APP_1' in df.columns:
            df['APP'] = df['APP'] + ' ' + df['APP_1']
            if 'APP_2' in df.columns:
                df['APP'] = df['APP'] + ' ' + df['APP_2']
                if 'APP_3' in df.columns:
                    df['APP'] = df['APP'] + ' ' + df['APP_3']
                    if 'APP_4' in df.columns:
                        df['APP'] = df['APP'] + ' ' + df['APP_4']
                        if 'APP_5' in df.columns:
                            df['APP'] = df['APP'] + ' ' + df['APP_5']
                            if 'APP6' in df.columns:
                                df['APP'] = df['APP'] + ' ' + df['APP_6']

    

    # verificando melhores dias
    if not 'MELHORES_DATAS' in df: df['MELHORES_DATAS'] = ' '
    if 'MELHORES_DIAS_0' in df.columns:
        df['MELHORES_DATAS'] = df['MELHORES_DIAS_0']
        if 'MELHORES_DIAS_1' in df.columns:
            df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                ' ' + df['MELHORES_DIAS_1']
            if 'MELHORES_DIAS_2' in df.columns:
                df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                    ' ' + df['MELHORES_DIAS_2']
                if 'MELHORES_DIAS_3' in df.columns:
                    df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                        ' ' + df['MELHORES_DIAS_3']
                    if 'MELHORES_DIAS_4' in df.columns:
                        df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                            ' ' + df['MELHORES_DIAS_4']
                        if 'MELHORES_DIAS_5' in df.columns:
                            df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                                ' ' + df['MELHORES_DIAS_5']
                            if 'MELHORES_DIAS_6' in df.columns:
                                df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                                    ' ' + df['MELHORES_DIAS_6']

    # verificando melhores horários
    if 'MELHORES_HORARIOS_0' in df.columns:
        df['MELHORES_DATAS'] = df['MELHORES_HORARIOS_0']
        if 'MELHORES_HORARIOS_1' in df.columns:
            df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                ' ' + df['MELHORES_HORARIOS_1']
            if 'MELHORES_HORARIOS_2' in df.columns:
                df['MELHORES_DATAS'] = df['MELHORES_DATAS'] + \
                    ' ' + df['MELHORES_HORARIOS_2']

    # verificando dia de funcionamento
    if not 'DIA_FUNCIONAMENTO' in df:
        df['DIA_FUNCIONAMENTO'] = ' '
    if 'DIA_FUNCIONAMENTO_0' in df.columns:
        df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO_0']
        if 'DIA_FUNCIONAMENTO_1' in df.columns:
            df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                ' ' + df['DIA_FUNCIONAMENTO_1']
            if 'DIA_FUNCIONAMENTO_2' in df.columns:
                df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                    ' ' + df['DIA_FUNCIONAMENTO_2']
                if 'DIA_FUNCIONAMENTO_3' in df.columns:
                    df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                        ' ' + df['DIA_FUNCIONAMENTO_3']
                    if 'DIA_FUNCIONAMENTO_4' in df.columns:
                        df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                            ' ' + df['DIA_FUNCIONAMENTO_4']
                        if 'DIA_FUNCIONAMENTO_5' in df.columns:
                            df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                                ' ' + df['DIA_FUNCIONAMENTO_5']
                            if 'DIA_FUNCIONAMENTO_6' in df.columns:
                                df['DIA_FUNCIONAMENTO'] = df['DIA_FUNCIONAMENTO'] + \
                                    ' ' + df['DIA_FUNCIONAMENTO_6']

    # verificando hora de funcionamento
    if not 'HORA_FUNCIONAMENTO' in df:
        df['HORA_FUNCIONAMENTO'] = ' '
    if 'HORA_FUNCIONAMENTO_0' in df.columns:
        df['HORA_FUNCIONAMENTO'] = df['HORA_FUNCIONAMENTO_0']
        if 'HORA_FUNCIONAMENTO_1' in df.columns:
            df['HORA_FUNCIONAMENTO'] = df['HORA_FUNCIONAMENTO'] + \
                ' ' + df['HORA_FUNCIONAMENTO_1']
            if 'HORA_FUNCIONAMENTO_2' in df.columns:
                df['HORA_FUNCIONAMENTO'] = df['HORA_FUNCIONAMENTO'] + \
                    ' ' + df['HORA_FUNCIONAMENTO_2']
    report = [
        'AUDITORIA_DATA', 'AUDITORIA_HORA', 'EC', 'RAZAO_SOCIAL', 'CNPJ', 'CONFIRMACAO',
        'RESPONSAVEL', 'LEAD', 'TEL_EC_1', 'TEL_EC_2', 'TEL_RESP_1', 'TEL_RESP_2', 'CEL_EC_1',
        'CEL_EC_2', 'CEL_RESP_1', 'CEL_RESP_2', 'EMAIL_EC', 'EMAIL_RESP', 'MELHORES_DATAS',
        'ANTECIPACAO', 'CONHECE_PAINEL', 'SABER_PAINEL', 'FUNCIONARIOS', 'ATIVIDADE',
        'DIA_FUNCIONAMENTO', 'HORA_FUNCIONAMENTO', 'ADESIVO', 'ERRADO_ALIMENTACAO', 'COMPRA_TICKET',
        'CREDENCIADO', 'MOTIVO', 'CRED_NOME', 'CRED_CNPJ', 'CREDENCIADO_DATA', 'CRED_RESP', 'CRED_TEL_1',
        'CRED_TEL_2', 'CRED_CEL_1', 'CRED_CEL_2', 'CRED_MAIL', 'DELIVERY', 'APP', 'FOTO_1', 'FOTO_2',
        'FOTO_3', 'FOTO_4', 'CIDADE', 'ESTADO', 'ENDERECO', 'LATITUDE', 'LONGITUDE'
    ]

    for i in df.columns:
        if i not in report:
            df.drop(i, inplace=True, axis=1)
    col = list()
    for i in report:
        if i not in df.columns:
            df[i] = 'nan'
        col.append(i)

    df = df[col]
    df.replace('nan', '', inplace=True)
    #df.to_excel(f's3://reportsofie/file_name.xls')
    
    return df.to_csv(index=False, sep=';', encoding='UTF-8-SIG')
    # return df.to_excel(index=False)
    # output = io.BytesIO()

    # # Use the BytesIO object as the filehandle.
    # writer = pd.ExcelWriter(output, engine='xlsxwriter')
    
    # # Write the data frame to the BytesIO object.
    # df.to_excel(writer, sheet_name='Sheet1')
    
    # writer.save()
    # xlsx_data = output.getvalue()
    # return xlsx_data

def report_apsen(itens):
    if itens[0]['task'] =='RFA-2':
        df = pd.DataFrame(itens)
        df.astype(str)
        df.replace('nan', '', inplace=True)
        for i in range(len(df)):
            if df.loc[i, "Produto não medicamento, recomendado para ansiedade"] == "PROBIANS":
                df.loc[i, "Tem Probians na loja ?"] = "Sim"
        df = df[[
            '_id',
            
            'SUBCANAL',
            'BANDEIRA',
            'GRUPO',
            'CNPJ',
            'CUP_BAIRRO',
            'CUP_CEP',
            'CUP_CIDADE',
            'CUP_ENDERECO',
            'CUP_NUMERO',
            'CUP_UF',
            'DESCRICAO_CUP',
            'visita',
            'onda',
            'data da tarefa',
            'data da aprovação',
            'Local encontrado',
            'O que encontrou no local',
            'Tem Inilok 40mg c/ 60 comprimidos ?',
            'Preço do Inilok 60 cpr',
            'Tem algum outro medicamento genérico para o Inilok ?',
            'O genérico é exatamente a mesma coisa do Inilok ?',
            'Qual é a diferença entre o Inilok e o genérico ?',
            'Produto não medicamento, recomendado para ansiedade',
            'Funcionário conhece o Probians, pode explicar o que é e para o que serve ?',
            'Tem Probians na loja ?',
            'Tem Probians na loja?',
            'Foto prateleira Probians_0',
            'Foto prateleira Probians_1',
            'Tem Extima lata ?',
            'Está atrás do balcão ou na prateleira ao seu alcance ?',
            'Qual o preço do Extima lata ?',
            'Quais os sabores que você encontrou na prateleira ?_0',
            'Quais os sabores que você encontrou na prateleira ?_1',
            'Quais os sabores que você encontrou na prateleira ?_2',
            'Foto Extima Lata na prateleira_0',
            'Foto Extima Lata na prateleira_1',
            'Você foi informado sobre o programa Sou Mais Vida ?',
            'Encontrou alguma informação sobre o Sou Mais Vida na embalagem ?']]
    else:    
        df = pd.DataFrame(itens)
        df.astype(str)
        df.replace('nan', '', inplace=True)
        df = df[[
            '_id',
            'Apsen ',
            'SUBCANAL',
            'BANDEIRA',
            'GRUPO',
            'CNPJ',
            'CUP_BAIRRO',
            'CUP_CEP',
            'CUP_CIDADE',
            'CUP_ENDERECO',
            'CUP_NUMERO',
            'CUP_UF',
            'DESCRICAO_CUP',
            'visita',
            'onda',
            'data da tarefa',
            'data da aprovação',
            'Local encontrado',
            'O que encontrou no local',
            'Encontrou Labirin 24mg 30cpr',
            'Preço unitário Labirin',
            'Desconto na compra de mais de uma unidade Labirin',
            'Ofereceu outro medicamento além do Labirin?',
            'Medicamento oferecido na troca do Labirin',
            'Primeiro colágeno indicado',
            'Foto MOTILEX HA, MOTILEX CAPS ou prateleira_0',
            'Foto MOTILEX HA, MOTILEX CAPS ou prateleira_1',
            'Primeira enzima para intolerância a lactose indicada',
            'Encontrou Lactosil 10,000 FCC 30tbl',
            'Foto Lactosil 10,000 FCC 30tbl ou prateleira_0',
            'Foto Lactosil 10,000 FCC 30tbl ou prateleira_1',
            'Posicionamento das caixas na prateleira Lactosil',
            'Localizou o preço do Lactosil na prateleira',
            'Havia promoção sinalizada na etiqueta Lactosil',
            'Preço unitário Lactosil']]
    
    return df.to_csv(index=False, sep=';', encoding='UTF-8-SIG')

def separar_numeros(df):
    DDD = list()
    number = list()
    for item in df:
        if item != "Não informado":
            telefone = str(item)
            DDD.append(telefone[0:2])
            number.append(telefone[2:])
        else:
            DDD.append(" ")
            number.append("Não informado")
    df.update(number)
    return {"DDD":DDD, "Telefone":number}
    
def report_VR(itens):
    final = list()
    for item in itens:
        item_tabela = {
            "Data Envio": item.get('Data Envio', " "),
            "Data_limite": item.get('Data Limite', " "),
            "Data da tarefa": item.get('data da tarefa', " "),
            "Data de aprovação": item.get('data da aprovação', " "),
            "_id": item.get('_id', " "),
            "execution_id": item.get('execution_id', " "),
            "states": "FINISHED",
            "status": item.get('status', " "),
            "aprovada": item.get('aprovada', " "),
            "tipo de visita": item.get('Tipo de Visita', " "),
            "Tipo de formulário": item.get('Tipo de formulário'),
            "Formulários": item.get('Formulários', " "),
            "CNPJ": item.get('CNPJ', " "),
            "Nome Fantasia": item.get('Nome Fantasia', " "),
            "Tipo de Logradouro": item.get('Tipo de Logradouro', " "),
            "Logradouro":  item.get('Logradouro', " "),
            "número":  item.get('Numero', " "),
            "complemento":  item.get('complement', " "),
            "bairro": item.get('Bairro', " "),
            'cidade': item.get('Cidade', " "),
            'Estado': item.get('UF', " "),
            "CEP":  item.get('CEP', item.get('postal_code')), 
            "O estabelecimento foi localizado ?": item.get('O estabelicimento foi localizado ?', item.get('O estabelicimento foi localizado?')),
            "Motivo do Insucesso": item.get('Motivo do insucesso'),
            "Motivo do insucesso ou sucesso": item.get('Motivo do insucesso ou sucesso', " "),
            "O CNPJ é o mesmo?" : item.get('O CNPJ é o mesmo?', " "),
            "Número_Outro CNPJ" : item.get('Número outro CNPJ_cnpj', " "),
            "Nome Contato" : item.get('Nome Contato', " "),
            "Telefone 01": item.get('Telefone 01_phone', " "),
            "Telefone 02": item.get('Telefone 02_phone', " "),
            "E-mail do Contato":item.get('Email do Contato_email', " "),
            "Prefere receber contato por E-mail ou Whatsapp?": item.get('Prefere receber contato por E-mail?', item.get('Prefere receber contato por E-mail ou Whatsapp?')),
            "Área de Atendimento": item.get('Área de Atendimento', " "),
            "Horário de funcionamento inicial": item.get('Horário de funcionamento inicial', " "),
            "Horário de funcionamento final": item.get('Horário de funcionamento final', " "),
            "Dias de Funcionamento":item.get('Dias de Funcionamento', " "),
            "Qtde de Mesas": item.get('Qtde de Mesas', " "),
            "Qtde lugares no Balcão": item.get('Lugares no Balcão', " "),
            "Qtde de Cadeiras": item.get('Qtde de Cadeiras', " "),
            "Qtde de Atendentes": item.get('Qtde de Atendentes', " "),
            "Qtde de Refeições Dia": item.get('Qtde de Refeições Dia', " "),
            "Qtde Caixas Loja":item.get('Qtde Caixas Loja', " "),
            "Ciente de incluir frutas no cardápio": item.get('Ciente de incluir frutas no cardápio', " "),
            "Cardápio informa Caloria": item.get('informa calorias', item.get('Informa calorias no cardápio')),
            "Delivery": item.get('Delivery:', " "),
            "Telefone Delivery": item.get('Telefone Delivery_phone', " "),
            "Atende através de Site, Aplicativo Próprio e/ou outros APP?":item.get('APP crediciado ou entrega própria_0', " "),
            "Ciente de cardápio saudável":item.get('Ciente de incluir frutas no cardápio', " "),
            "Estabelecimento tem adesivo?": item.get('Estabelecimento tem adesivo?', " "),
            "Deseja receber adesivos?":item.get('Deseja receber adesivos?', " "),
            "Foto fachada local encontrado_0": item.get('Foto fachada local encontrado_0', " "),
            "Foto fachada local encontrado_1": item.get('Foto fachada local encontrado_1', " "),
            "Foto da fachada_0": item.get('Foto da fachada_0', " "),
            "Foto da fachada_1": item.get('Foto da fachada_1', " "),
            "Foto da infraestrutura_0": item.get('Foto da infraestrutura_1', " "),
            "Foto da infraestrutura_1": item.get('Foto da infraestrutura_2', " "),
            'Foto do cardápio_0': item.get('Foto Cardápio_0', " "),
            'Foto do cardápio_1': item.get('Foto Cardápio_1', " "),
            "Foto do adesivo VR_1": item.get('Foto da sinalização_1', " "),
            "Foto do adesivo VR_2": item.get('Foto da sinalização_2', " ")
    
        }

        final.append(item_tabela)
    
    
    df = pd.DataFrame(final)
    df.astype(str)
    df.replace('nan', '', inplace=True)
    df['Motivo do Insucesso'] = df['Motivo do Insucesso'].fillna(df['Motivo do insucesso ou sucesso'])
    df['Data de aprovação'] = df['Data de aprovação'].sort_values()
    print(df['Data de aprovação'])
    
    new_format = list()
    for item in df['Data da tarefa']:
        try:
            horario = (item.split("T"))[1]
            hoje = f'{item[8:10]}-{item[5:8]}{item[0:4]} {horario}'
            new_format.append(hoje)
        except:
            new_format.append(' ')
    df['Data da tarefa'].update(new_format)
    
    new_format_2 = list()
    for item in df['Data de aprovação']:
        try:
            horario = (item.split("T"))[1]
            hoje = f'{item[8:10]}-{item[5:8]}{item[0:4]} {horario}'
            new_format_2.append(hoje)
        except:
            new_format_2.append(' ')
    df['Data de aprovação'].update(new_format_2)
    
    telefone_1 = separar_numeros(df['Telefone 01'])
    df.insert(28, "DDD_01", telefone_1['DDD'])
    telefone_2 = separar_numeros(df['Telefone 02'])
    df.insert(30, "DDD_02", telefone_2['DDD'])
    
    
    new_CNPJ = list()
    for item in df['CNPJ']:
        first = item[0:8]
        last = item[8:]
        new_CNPJ.append(first + "/" + last)
    df['CNPJ'].update(new_CNPJ)
    
    new_outro_CNPJ = list()
    for item in df['Número_Outro CNPJ']:
        valor = str(item)
        if len(valor) > 10:
            first = valor[0:8]
            last = valor[8:]
            new_outro_CNPJ.append(first + "/" + last)
        else:
            new_outro_CNPJ.append(" ")
    
    df['Número_Outro CNPJ'].update(new_outro_CNPJ)
    
    df.drop(columns=['Motivo do insucesso ou sucesso'], inplace=True)
    df['E-mail do Contato'] = df['E-mail do Contato'].replace("Não informado", "")
    df['Telefone 02'] = df['Telefone 02'].replace("Não informado", "")
    
    
    #df.to_csv("/tmp/arquivo.csv", index=False, sep=';', encoding='UTF-8')
    df.to_excel("/tmp/arquivo.xlsx", index=False, encoding='UTF-8')
    
    path = "/tmp/arquivo.xlsx"
    name = "report2.xlsx"
    s3name = "sofie-reports"
    
    print('https://sofie-reports.s3.sa-east-1.amazonaws.com/sofie-reports/report.xlsx')
    
    s3 = boto3.client('s3')
    with open(path, 'rb') as f:
        s3.upload_fileobj(f, s3name, name)
    
        
   
    
    
    return df.to_csv(index=False, sep=';', encoding='UTF-8')
    
    

    