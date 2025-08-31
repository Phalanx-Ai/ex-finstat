import logging
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class Configuration(BaseModel):
    api_key: str = Field(..., description="API Key provided with your finstat account")
    private_key: str = Field(..., alias="#private_key", description="Private Key provided with your finstat account")
    request_type: str = Field(..., description="Request type: detail or extended")
    ico_field: str = Field(..., description="Name of field in the input file which contains the ICO number")
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.debug("Component will run in Debug mode")
