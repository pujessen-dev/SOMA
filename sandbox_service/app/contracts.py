"""
API contracts for sandbox service.
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class ExecuteBatchRequest(BaseModel):
    """Request to execute a batch of compression tasks."""
    
    batch_id: str = Field(..., description="Unique identifier for this batch")
    challenge_code: str = Field(..., description="Python code to execute")
    challenge_texts: List[str] = Field(..., description="Texts to compress")
    compression_ratios: List[Optional[float]] = Field(
        ..., description="Target compression ratios"
    )
    ttl_seconds: Optional[int] = Field(
        default=120, description="Execution timeout in seconds"
    )


class ExecuteBatchResponse(BaseModel):
    """Response from batch execution."""
    
    success: bool = Field(..., description="Whether execution succeeded")
    batch_id: str = Field(..., description="Batch identifier")
    error: Optional[str] = Field(default=None, description="Error message if failed")
