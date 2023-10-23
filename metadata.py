from typing import List
from pydantic import BaseModel

from atlan import Atlan

class MetadataChangeEvent(BaseModel):
    """
    A model representing a metadata change event.
    """

    entity_type: str
    entity_id: str
    change_type: str
    data: dict

class MetadataConnector:
    """
    A base class for developing connectors to ingest and consume metadata from upstream data sources and downstream consumers.
    """

    def __init__(self, atlan: Atlan):
        self.atlan = atlan

    def ingest(self, events: List[MetadataChangeEvent]):
        """
        Ingest a list of metadata change events into Atlan.

        Args:
            events: A list of metadata change events.
        """

        for event in events:
            self.atlan.write_metadata(event.entity_type, event.entity_id, event.change_type, event.data)

    def consume(self):
        """
        Consume metadata change events from the upstream data source.
        """

        raise NotImplementedError()
