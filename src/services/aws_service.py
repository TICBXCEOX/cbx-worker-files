import boto3
from configs import ACCESS_KEY, SECRET_KEY, REGION_NAME, WAIT_TIME_SECONDS

class AwsService:
    def __init__(self):
        self.sqs = self.sign_sqs()
                
    def sign_sqs(self):
        return boto3.client('sqs', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION_NAME)
                                                              
    def consume_message(self, url):
        try:   
            response = self.sqs.receive_message(
                QueueUrl=url,
                MaxNumberOfMessages=1,  # Quantas mensagens recuperar
                WaitTimeSeconds=int(WAIT_TIME_SECONDS),  # Tempo máximo de espera para mensagens (long polling)
                AttributeNames=['All'],  # Se você precisar de atributos da mensagem
                MessageAttributeNames=['All']  # Atributos extras da mensagem, se houver
            )
            
            if 'Messages' not in response:
                return False, 'Nenhuma mensagem na fila'
            
            return True, response['Messages']            
        except Exception as ex:
            error = f"Erro ao consumir mensagem da fila SQS: {str(ex)}"
            return False, error
        
    def delete_message(self, url, message):
        try:
            self.sqs.delete_message(
                QueueUrl=url,
                ReceiptHandle=message['ReceiptHandle']
            )
            return True, 'Mensagem removida da Fila'
        except Exception as ex:
            msg = f'Erro ao remover mensagem da fila: {str(ex)}'
            return False, msg