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
        frames  = message["frames"]
        bucket  = message["bucket"]
        file_id = message["file_id"]

        print(f"frames ---> {frames}")
        print(f"bucket ---> {bucket}")
        
        temp_dir = tempfile.mkdtemp()
        zip_filename = f"frames-{file_id}.zip"
        zip_path = os.path.join(temp_dir, zip_filename)
        
        try:
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for frame in frames:
                    pathFrame = f"frames/{file_id}/{frame}"
                    print(f"path ---> {pathFrame}")
                    frame_temp_path = os.path.join(temp_dir, pathFrame)
                    s3_client.download_file(bucket, pathFrame, frame_temp_path)
                    zipf.write(frame_temp_path, arcname=frame)

            print("Imagens do S3 baixadas")
            
            zip_s3_key = f"zips/{zip_filename}"
            s3_client.upload_file(zip_path, bucket, zip_s3_key)

            print("Relizado upload do arquivo ZIP para o S3")
            
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
