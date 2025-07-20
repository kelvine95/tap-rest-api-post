from singer_sdk.authenticators import APIAuthenticatorBase

class HeaderAPIKeyAuthenticator(APIAuthenticatorBase):
    """Authenticator that sends API key in headers."""
    
    def __init__(self, stream, key: str, value: str):
        super().__init__(stream)
        self.key = key
        self.value = value
        
    def apply(self, request):
        """Apply the authentication to the request."""
        request.headers[self.key] = self.value
        # Ensure content-type is set for POST requests
        if request.method == "POST":
            if "Content-Type" not in request.headers:
                request.headers["Content-Type"] = "application/json"
        return request
        