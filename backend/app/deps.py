from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer_scheme = HTTPBearer(auto_error=False)


def optional_bearer_auth(
    credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
) -> HTTPAuthorizationCredentials | None:
    return credentials
