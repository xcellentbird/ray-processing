import os

from dotenv import load_dotenv


load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

IN_KUBERNETES = True if os.getenv('KUBERNETES_SERVICE_HOST') else False