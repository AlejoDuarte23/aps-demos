from pydantic import BaseModel, Field
from typing import Union, Literal
from textwrap import dedent
import instructor
from openai import OpenAI
from dotenv import load_dotenv

# Import the structural tools
from app.tools import PlotOpenSeesModelTool, RunOpenSeesModelTool, DisplayLoadsTool

load_dotenv()
client = instructor.from_openai(OpenAI())


class StructuralTools(BaseModel):
    response: str = Field(
        ...,
        description="Be conversational, friendly and format the response always nicely",
    )
    tool: Union[PlotOpenSeesModelTool, RunOpenSeesModelTool, DisplayLoadsTool] = Field(
        ...,
        description="Select the appropriate structural tool based on the user request",
    )
    why: str = Field(..., description="Explain why you selected this specific tool")


def structural_agent(conversation_history: list[dict]) -> StructuralTools:
    messages = []
    system_message = {
        "role": "system",
        "content": dedent(
            """
            You are a structural engineering assistant with access to OpenSees for structural analysis.
            Select the most appropriate tool to respond to the user's request:

            1. PlotOpenSeesModelTool: Use when the user wants to visualize the structural model
               geometry and sections without running analysis.

            2. RunOpenSeesModelTool: Use when the user wants to analyze the model, calculate reactions,
               displacements, or any other type of structural analysis.
               
            3. DisplayLoadsTool: Use when the user specifically wants to see or visualize the loads
               applied to the structural model.

            Be clear and friendly in your response, and explain why you selected the specific tool.
            """
        ),
    }
    messages.append(system_message)
    messages.extend(conversation_history)

    return client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_model=StructuralTools,
    )
