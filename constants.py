import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the keys
kraken_public_key = os.getenv('KRAKEN_PUBLIC')
kraken_private_key = os.getenv('KRAKEN_PRIVATE')

dollar_threshold = 5000000

