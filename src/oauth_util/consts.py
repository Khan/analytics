try:
    # Try to see if there is a local developer copy of the OAuth params.
    import dev_consts
    SERVER_URL = dev_consts.SERVER_URL
    CONSUMER_KEY = dev_consts.CONSUMER_KEY
    CONSUMER_SECRET = dev_consts.CONSUMER_SECRET
except ImportError:
    # These are default OAuth constants tied to a test app in production.
    SERVER_URL = "http://www.khanacademy.org"
    CONSUMER_KEY = '4rFmvSXgK7aDxfLY'
    CONSUMER_SECRET = 'TacTQ3NmKTyeFuM7'
