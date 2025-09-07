import os

AMAZON_CREDENTIALS = {
    'email': os.getenv('AMAZON_EMAIL', ''),
    'password': os.getenv('AMAZON_PASSWORD', ''),
    'totp_secret': os.getenv('AMAZON_TOTP_SECRET', ''),
}
