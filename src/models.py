from pydantic import BaseModel, ConfigDict

class RawData(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, str_strip_whitespace=True)

    name: str
    category: str
    value: int

    def to_mongo(self) -> dict:
        data = self.model_dump(by_alias=True)
        return data