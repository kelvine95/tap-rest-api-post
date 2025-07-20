from singer_sdk.authenticators import APIAuthenticatorBase

class HeaderAPIKeyAuthenticator(APIAuthenticatorBase):
    def __init__(self, stream, key: str, value: str):
        super().__init__(stream)
        self.key = key
        self.value = value
        
    def apply(self, request):
        request.headers[self.key] = self.value
        request.headers["Content-Type"] = "application/json"
        return request
    