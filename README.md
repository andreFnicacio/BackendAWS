# HealthTech AWS Lambdas and Services

Este é o repositório que contém as funções AWS Lambda responsáveis pelas operações de uma HealthTech, utilizando bancos de dados como o DynamoDB e serviços como API Gateway, SQS e S3. Nós construímos um modelo de plano de saúde seguindo a lógica apresentada aqui.

## Sobre a HealthTech

A HealthTech é uma empresa focada na melhoria da assistência médica por meio da tecnologia. Este repositório contém as funções AWS Lambda que desempenham um papel fundamental nas operações da empresa.

### Modelo de Plano de Saúde

Nós desenvolvemos um modelo de plano de saúde que segue a lógica apresentada neste repositório. Ele é projetado para atender às necessidades de nossos clientes e melhorar a eficiência das operações da HealthTech.

## Criando uma Conta no DynamoDB

Para começar a usar o DynamoDB como banco de dados para suas funções AWS Lambda, siga estas etapas:

### 1. Acesse a AWS Console

Acesse a [AWS Console](https://aws.amazon.com/pt/console/) usando suas credenciais da AWS ou crie uma conta se você ainda não tiver uma.

### 2. Acesse o DynamoDB

Dentro do painel da AWS Console, navegue até o serviço DynamoDB.

### 3. Crie uma Tabela

Siga as instruções na AWS Console para criar uma nova tabela no DynamoDB. Defina a estrutura da tabela de acordo com as necessidades do seu aplicativo.

## Upload de uma Função Lambda para a AWS usando a CLI

Para enviar uma função Lambda para a AWS usando a AWS CLI, siga estas etapas:

### 1. Instale a AWS CLI

Certifique-se de ter a AWS CLI instalada em seu ambiente. Você pode baixá-la em [AWS Command Line Interface](https://aws.amazon.com/cli/).

### 2. Configure as Credenciais

Execute o comando `aws configure` para configurar suas credenciais da AWS. Certifique-se de fornecer a chave de acesso e a chave secreta corretas.

### 3. Crie um Pacote ZIP

Empacote o código da função Lambda em um arquivo ZIP, incluindo todas as dependências necessárias.

### 4. Crie uma Função Lambda

Use o seguinte comando para criar uma nova função Lambda:

```sh
aws lambda create-function \
  --function-name NomeDaFuncao \
  --runtime runtime-da-funcao \
  --handler nome-do-handler \
  --role role-arn \
  --zip-file fileb://caminho-para-o-codigo.zip
```

Substitua `NomeDaFuncao`, `runtime-da-funcao`, `nome-do-handler`, `role-arn` e `caminho-para-o-codigo.zip` pelos valores específicos para sua função.

### 5. Implante e Gerencie sua Função

Depois de criar e testar sua função Lambda, você pode implantá-la e gerenciá-la conforme necessário usando a AWS CLI.

## Upload de uma Função Lambda via VS Code

Você também pode fazer o upload de uma função Lambda usando o Visual Studio Code com a extensão "AWS Toolkit". Certifique-se de instalar a extensão e seguir as instruções fornecidas.

Agradecemos por usar nossas tecnologias! Se você tiver mais perguntas ou precisar de ajuda adicional, não hesite em nos contatar. Estamos aqui para ajudar!

```

Espero que essa versão do README seja útil. Se tiver mais alguma pergunta ou precisar de mais assistência, estou à disposição! Agradeço pelo reconhecimento, estou aqui para ajudar. 😊
