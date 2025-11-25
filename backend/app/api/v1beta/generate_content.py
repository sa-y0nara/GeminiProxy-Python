from typing import Annotated
import uuid

from app.core import manager
from app.core.log_utils import Logger
from app.schemas import GenerateContentPayload, GenerateContentResponse
from fastapi import Path, Request
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter

router = APIRouter(tags=["Generating content"])


async def _execute_generation(
    *,
    model: str,
    payload: GenerateContentPayload,
    request: Request,
    is_stream: bool,
):
    request_id = str(uuid.uuid4())
    action = "流式生成" if is_stream else "生成内容"
    Logger.api_request(request_id, f"{action} | {model}")

    request_payload = {
        "model": model,
        "payload": payload.model_dump(by_alias=True, exclude_none=True),
    }

    result = await manager.handle_api_request(
        command_type="streamGenerateContent" if is_stream else "generateContent",
        payload=request_payload,
        request=request,
        is_streaming=is_stream,
    )

    if not is_stream:
        Logger.api_response(request_id, "生成完成")
        return result

    async def generator():
        async for chunk in result:
            yield chunk
        Logger.api_response(request_id, "流式生成完成")

    return StreamingResponse(generator())


@router.post(
    "/models/{model}:generateContent",
    response_model=GenerateContentResponse,
    response_model_exclude_none=True,
    name="models.generateContent",
)
async def generate_content(
    model: Annotated[str, Path(description="Required. The name of the Model to use for generating the completion.")],
    payload: GenerateContentPayload,
    request: Request,
):
    """
    Generates a model response given an input GenerateContentRequest. Refer to the text generation guide for detailed usage information. Input capabilities differ between models, including tuned models. Refer to the model guide and tuning guide for details.
    """
    return await _execute_generation(model=model, payload=payload, request=request, is_stream=False)


@router.post(
    "/models/{model}:streamGenerateContent",
    response_class=StreamingResponse,
    name="models.streamGenerateContent",
)
async def stream_generate_content(
    model: Annotated[str, Path(description="Required. The name of the Model to use for generating the completion.")],
    payload: GenerateContentPayload,
    request: Request,
):
    """
    Generates a streamed response from the model given an input GenerateContentRequest.
    """
    return await _execute_generation(model=model, payload=payload, request=request, is_stream=True)
