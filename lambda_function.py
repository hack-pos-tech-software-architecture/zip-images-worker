import boto3
import json
import os
import tempfile
import zipfile
import uuid

# Configuração do S3
s3_client = boto3.client("s3", region_name="us-east-1")
# BUCKET_VIDEO_PROCESSOR_S3 = os.environ.get("BUCKET_VIDEO_PROCESSOR_S3")

# Configuração do SNS para notificação
# sns_client = boto3.client("sns")
# SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:seu-id:seu-topico"

def lambda_handler(event, context):
    for record in event["Records"]:
        message = json.loads(record["body"])
        frames = message["frames"]
        bucket = message["bucket"]

        print(f"frames ---> {frames}")
        print(f"bucket ---> {bucket}")
        
        temp_dir = tempfile.mkdtemp()
        zip_filename = f"frames-{uuid.uuid4()}.zip"
        zip_path = os.path.join(temp_dir, zip_filename)
        
        try:
            print("Baixando as imagens do S3")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for frame in frames:
                    frame_temp_path = os.path.join(temp_dir, frame)
                    s3_client.download_file(bucket, frame, frame_temp_path)
                    zipf.write(frame_temp_path, arcname=frame)
            
            print("Fazendo upload do arquivo ZIP para o S3")
            zip_s3_key = f"zips/{zip_filename}"
            s3_client.upload_file(zip_path, bucket, zip_s3_key)
            
            # Envia notificação via SNS
            # zip_url = f"https://{bucket}.s3.amazonaws.com/{zip_s3_key}"
            # sns_client.publish(
            #     TopicArn=SNS_TOPIC_ARN,
            #     Message=json.dumps({"message": "Arquivo ZIP pronto", "zip_url": zip_url}),
            #     Subject="Download do Arquivo ZIP"
            # )
            
            return {"message": "Compactação concluída", "zip_file": zip_s3_key, "zip_url": "teste"}
        
        except Exception as e:
            print(f"Erro na compactação: {str(e)}")
            raise e
