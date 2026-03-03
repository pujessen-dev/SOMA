from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import List
import uuid

import httpx

from app.services.blob.compressed_text_storage import CompressedTextStorage


logger = logging.getLogger(__name__)


class RemoteSandboxManager:
    """Remote sandbox manager that delegates execution to a separate sandbox service."""

    def __init__(
        self,
        *,
        sandbox_service_url: str,
        compressed_text_storage: CompressedTextStorage,
        default_ttl: timedelta,
        exec_timeout_seconds: float | None = None,
        max_sandboxes: int = 10,
        request_timeout: float = 120.0,
    ):
        """Initialize remote sandbox manager.
        
        Args:
            sandbox_service_url: Base URL of the sandbox service
            compressed_text_storage: Storage for compressed texts
            default_ttl: Default time-to-live for sandbox operations
            exec_timeout_seconds: Execution timeout for sandbox operations
            max_sandboxes: Maximum concurrent sandbox operations (for semaphore)
            request_timeout: HTTP request timeout in seconds
        """
        self.max_sandboxes = max_sandboxes
        self.default_ttl = default_ttl
        self._sandbox_service_url = sandbox_service_url.rstrip("/")
        self._compressed_text_storage = compressed_text_storage
        self._exec_timeout_seconds = (
            float(exec_timeout_seconds)
            if exec_timeout_seconds is not None and exec_timeout_seconds > 0
            else None
        )
        self._request_timeout = request_timeout
        self._semaphore = asyncio.Semaphore(max_sandboxes)

    async def run_batch(
        self,
        *,
        batch_id: str,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        ttl: timedelta | None = None,
        acquire_timeout: float = 10.0,
    ) -> list[str]:
        """Execute a batch of challenges on remote sandbox service.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch
            challenge_code: Python code to compress texts
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            ttl: Time-to-live for the operation
            acquire_timeout: Timeout for acquiring semaphore slot
            
        Returns:
            List of compressed texts
            
        Raises:
            RuntimeError: If platform is at capacity or execution fails
        """
        try:
            async with asyncio.timeout(acquire_timeout):
                await self._semaphore.acquire()
        except asyncio.TimeoutError:
            logger.warning(
                "[RemoteSandbox] Platform at capacity - no sandbox slots available within %ss",
                acquire_timeout
            )
            raise RuntimeError(
                f"Platform is at capacity. Maximum {self.max_sandboxes} sandboxes are currently running. "
                "Please try again later."
            )
        
        try:
            return await self._execute_remote_batch(
                batch_id,
                challenge_code,
                challenge_texts,
                compression_ratios,
                ttl,
            )
        except Exception as exc:
            logger.error(
                "[RemoteSandbox] Batch execution failed: %s", exc, exc_info=True
            )
            return [""] * len(challenge_texts)
        finally:
            self._semaphore.release()

    async def _execute_remote_batch(
        self,
        batch_id: str,
        challenge_code: str,
        challenge_texts: list[str],
        compression_ratios: list[float | None],
        ttl: timedelta | None = None,
    ) -> list[str]:
        """Execute batch on remote sandbox service and retrieve results from S3.
        
        Args:
            batch_id: Unique batch identifier from ChallengeBatch
            challenge_code: Python code to compress texts
            challenge_texts: List of texts to compress
            compression_ratios: Target compression ratios
            ttl: Time-to-live for the operation
            
        Returns:
            List of compressed texts
        """
        
        ttl_seconds = None
        if ttl is not None:
            ttl_seconds = int(ttl.total_seconds())
        elif self.default_ttl is not None:
            ttl_seconds = int(self.default_ttl.total_seconds())
        
        if self._exec_timeout_seconds is not None and self._exec_timeout_seconds > 0:
            if ttl_seconds is None:
                ttl_seconds = int(self._exec_timeout_seconds)
            else:
                ttl_seconds = min(ttl_seconds, int(self._exec_timeout_seconds))
        
        # Prepare request payload
        payload = {
            "batch_id": batch_id,
            "challenge_code": challenge_code,
            "challenge_texts": challenge_texts,
            "compression_ratios": compression_ratios,
            "ttl_seconds": ttl_seconds,
        }
        
        logger.info(
            "[RemoteSandbox] Sending batch to sandbox service: batch_id=%s, texts=%d",
            batch_id,
            len(challenge_texts),
        )
        
        # Send request to sandbox service
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self._sandbox_service_url}/execute_batch",
                    json=payload,
                    timeout=self._request_timeout,
                )
                response.raise_for_status()
                result = response.json()
                
                if not result.get("success"):
                    error_msg = result.get("error", "Unknown error")
                    logger.error(
                        "[RemoteSandbox] Sandbox service returned error: %s",
                        error_msg,
                    )
                    return [""] * len(challenge_texts)
                
                logger.info(
                    "[RemoteSandbox] Sandbox execution successful: batch_id=%s",
                    batch_id,
                )
                
            except httpx.TimeoutException as exc:
                logger.error(
                    "[RemoteSandbox] Request to sandbox service timed out: %s",
                    exc,
                )
                return [""] * len(challenge_texts)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[RemoteSandbox] Sandbox service returned error status %d: %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                return [""] * len(challenge_texts)
            except Exception as exc:
                logger.error(
                    "[RemoteSandbox] Failed to communicate with sandbox service: %s",
                    exc,
                    exc_info=True,
                )
                return [""] * len(challenge_texts)
        
        # Retrieve compressed texts from S3
        try:
            compressed_texts = await self._compressed_text_storage.get_batch(batch_id)
            logger.info(
                "[RemoteSandbox] Retrieved %d compressed texts from storage",
                len(compressed_texts),
            )
            
            # Normalize the result to match expected length
            if len(compressed_texts) < len(challenge_texts):
                compressed_texts.extend([""] * (len(challenge_texts) - len(compressed_texts)))
            elif len(compressed_texts) > len(challenge_texts):
                compressed_texts = compressed_texts[:len(challenge_texts)]
            
            return compressed_texts
            
        except Exception as exc:
            logger.error(
                "[RemoteSandbox] Failed to retrieve compressed texts from storage: %s",
                exc,
                exc_info=True,
            )
            return [""] * len(challenge_texts)
